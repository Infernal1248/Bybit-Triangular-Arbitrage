const { log, error } = console;
const got = require("got");
const events = require("events");
const Websocket = require("ws");
const { sort } = require("fast-sort");
const { promisify } = require("util");
const delay = promisify(setTimeout);

const {
  FLOW_THRESHOLD,
  MIN_TICKS_TO_SHOW,
  FLOW_NOCHANGE_CLOSE_MS,
  FLOW_NOCHANGE_REQUIRE_ALL,
  L1_HISTORY_LEN,
  L1_MIN_DISTINCT_STATES,
  L1_REQUIRE_MOVEMENT_TO_START,
  L1_REQUIRE_MOVEMENT_TO_KEEP,
  ORDERBOOK_DEPTH,
} = require("./config");

const { sendTelegram, formatFlowStartMsg, formatFlowEndMsg } = require("./telegram");
const { writeFlowEvent, logFlow } = require("./logger");
const { fetchSpotFeeRates, getFee } = require("./fees");
const { getBalance, getBalances } = require("./balance");

// ==== ГЛОБАЛ ====
let pairs = [];
let symValJ = {};     // { [symbol]: { bidPrice, askPrice, bidQty, askQty, bids: [], asks: [] } }
let validSymbols = [];
let ws = "";
let subs = [];

let wsConnectedOnce = false;

let tickCounters = Object.create(null);

// { active, posTicks, negTicks, startTs, lastValue, notifiedStart, notifiedEndAt, watchSyms, lastChangeAtStart }
let flowState = new Map();

let broadcastTimer = null;

// свежесть
let symMeta = {};     // { [symbol]: { tickDec } }
let lastArrival = {}; // { [symbol]: ts }
let lastChange  = {}; // { [symbol]: ts }
let lastBA      = {}; // { [symbol]: { bid, ask } }

// История L1 по символу: массив последних снапшотов
// l1Hist[symbol] = [{bp, ap, bq, aq, ts}, ...]
let l1Hist = Object.create(null);

const eventEmitter = new events();
const pairId = (d) => `${d.lv1}|${d.lv2}|${d.lv3}`;
// ==== инициализация ====
const getPairs = async () => {
  try {
    const resp = await got("https://api.bybit.com/v5/market/instruments-info?category=spot", {
      responseType: "json",
      timeout: { request: 15000 },
    });
    const list = resp.body.result.list;

    validSymbols = list.map((d) => d.symbol);

    // метаданные и свежесть
    symMeta = {};
    lastArrival = {};
    lastChange = {};
    lastBA = {};
    l1Hist = Object.create(null);

    list.forEach((d) => {
      const tk = String(d.priceFilter?.tickSize ?? "0.0001");
      const tickDec = tk.includes(".") ? tk.split(".")[1].length : 0;
      symMeta[d.symbol] = { tickDec };
      lastArrival[d.symbol] = 0;
      lastChange[d.symbol]  = 0;
      lastBA[d.symbol]      = { bid: undefined, ask: undefined };
    });

    validSymbols.forEach((symbol) => {
      symValJ[symbol] = { bidPrice: 0, askPrice: 0, bidQty: 0, askQty: 0, bids: [], asks: [] };
      l1Hist[symbol] = [];
    });

    await fetchSpotFeeRates();

    const assets = [...new Set(list.map((d) => [d.baseCoin, d.quoteCoin]).flat())];

    // строим треугольники, первая валюта — USDT
    const BASE = "USDT";
    pairs = [];
    assets.forEach((d2) => {
      if (d2 === BASE) return;
      assets.forEach((d3) => {
        if (d3 === BASE || d3 === d2) return;

        let lv1 = [], lv2 = [], lv3 = [];
        let l1 = "", l2 = "", l3 = "";

        if (symValJ[BASE + d2]) { lv1.push(BASE + d2); l1 = "num"; }
        if (symValJ[d2 + BASE]) { lv1.push(d2 + BASE); l1 = "den"; }

        if (symValJ[d2 + d3]) { lv2.push(d2 + d3); l2 = "num"; }
        if (symValJ[d3 + d2]) { lv2.push(d3 + d2); l2 = "den"; }

        if (symValJ[d3 + BASE]) { lv3.push(d3 + BASE); l3 = "num"; }
        if (symValJ[BASE + d3]) { lv3.push(BASE + d3); l3 = "den"; }

        if (lv1.length && lv2.length && lv3.length) {
          pairs.push({
            l1, l2, l3,
            d1: BASE,
            d2,
            d3,
            lv1: lv1[0],
            lv2: lv2[0],
            lv3: lv3[0],
            value: -100,
            tpath: "",
          });
        }
      });
    });

    // подписки только на нужные символы
    const used = new Set();
    pairs.forEach(d => { used.add(d.lv1); used.add(d.lv2); used.add(d.lv3); });

    const totalOnExchange = validSymbols.length;
    validSymbols = [...used];

    const newSymValJ = {};
    const newL1Hist = {};
    for (const s of validSymbols) {
      newSymValJ[s] = symValJ[s] || { bidPrice: 0, askPrice: 0, bidQty: 0, askQty: 0, bids: [], asks: [] };
      newL1Hist[s] = l1Hist[s] || [];
    }
    symValJ = newSymValJ;
    l1Hist = newL1Hist;

    log(`[init] Путей найдено: ${pairs.length} (USDT-first). Символов всего на бирже: ${totalOnExchange}. К подписке: ${validSymbols.length}`);
  } catch (err) {
    error('[init] Не удалось получить символы:', err.message);
  }
};

// ==== вспомогательное для потокового вещания в сокет ====
function snapshotActivePayload() {
  const active = pairs.filter((d) => {
    const st = flowState.get(pairId(d));
    return st?.active === true;
  });
  return sort(active).desc((u) => u.value);
}
function ensureBroadcastLoop() {
  const hasActive = [...flowState.values()].some((st) => st.active);
  if (hasActive && !broadcastTimer) {
    broadcastTimer = setInterval(() => {
      const payload = snapshotActivePayload();
      if (payload.length > 0) {
        eventEmitter.emit("ARBITRAGE", payload);
      } else {
        clearInterval(broadcastTimer);
        broadcastTimer = null;
      }
    }, 1);
  } else if (!hasActive && broadcastTimer) {
    clearInterval(broadcastTimer);
    broadcastTimer = null;
  }
}

// ==== утилита: проверка, что нужная сторона L1 у символа имеет qty > 0 ====
function qtySideOk(symbol, side /* 'bid' | 'ask' */) {
  const p = symValJ[symbol];
  if (!p) return false;
  if (side === "bid") return Number(p.bidQty) > 0;
  return Number(p.askQty) > 0;
}

// ==== история L1 и проверки движения ====
function pushL1(symbol, bp, bq, ap, aq, ts) {
  if (!l1Hist[symbol]) l1Hist[symbol] = [];
  const arr = l1Hist[symbol];
  arr.push({ bp, bq, ap, aq, ts });
  if (arr.length > L1_HISTORY_LEN) arr.splice(0, arr.length - L1_HISTORY_LEN);
}

function l1DistinctStates(symbol) {
  const arr = l1Hist[symbol] || [];
  if (!arr.length) return 0;
  // Считаем уникальные комбинации цены/объёма обеих сторон
  const set = new Set();
  for (const it of arr) {
    set.add(`${it.bp}|${it.ap}|${it.bq}|${it.aq}`);
  }
  return set.size;
}

function hasRecentL1Movement(symbol) {
  // Истинно, если в последних N снапшотах есть как минимум L1_MIN_DISTINCT_STATES разных комбинаций
  return l1DistinctStates(symbol) >= L1_MIN_DISTINCT_STATES;
}

// ==== обработка сообщений WS ====
const processData = (pl) => {
  try {
    pl = JSON.parse(pl);
    const topicPrefix = `orderbook.${ORDERBOOK_DEPTH}.`;
    if (!pl.topic || !pl.topic.startsWith(topicPrefix)) return;

    const symbol = pl.topic.slice(topicPrefix.length);
    const data = pl.data;
    if (
        !data ||
        !Array.isArray(data.b) || data.b.length < ORDERBOOK_DEPTH ||
        !Array.isArray(data.a) || data.a.length < ORDERBOOK_DEPTH
    ) return;

    const bids = data.b.slice(0, ORDERBOOK_DEPTH).map((lvl) => [parseFloat(lvl[0]), parseFloat(lvl[1])]);
    const asks = data.a.slice(0, ORDERBOOK_DEPTH).map((lvl) => [parseFloat(lvl[0]), parseFloat(lvl[1])]);
    if (
        bids.some(([p, q]) => !Number.isFinite(p) || !Number.isFinite(q)) ||
        asks.some(([p, q]) => !Number.isFinite(p) || !Number.isFinite(q))
    ) return;

    const [bidPrice, bidQty] = bids[0];
    const [askPrice, askQty] = asks[0];

    const now = Date.now();
    lastArrival[symbol] = now;

    const dec = symMeta[symbol]?.tickDec ?? 8;
    const rb = Number(bidPrice.toFixed(dec));
    const ra = Number(askPrice.toFixed(dec));
    const prev = lastBA[symbol] || {};
    const changed = (prev.bid !== rb) || (prev.ask !== ra);
    if (changed) {
      lastChange[symbol] = now;
      lastBA[symbol] = { bid: rb, ask: ra };
    }

    symValJ[symbol].bidPrice = rb;
    symValJ[symbol].askPrice = ra;
    symValJ[symbol].bidQty = bidQty;
    symValJ[symbol].askQty = askQty;
    symValJ[symbol].bids = bids;
    symValJ[symbol].asks = asks;

    // заполняем историю L1
    pushL1(symbol, rb, bidQty, ra, askQty, now);

    // пересчитываем только пути с этим символом
    pairs
        .filter((d) => (d.lv1 + d.lv2 + d.lv3).includes(symbol))
        .forEach((d) => {
          const p1 = symValJ[d.lv1];
          const p2 = symValJ[d.lv2];
          const p3 = symValJ[d.lv3];
          if (!p1?.bidPrice || !p2?.bidPrice || !p3?.bidPrice || !p1?.askPrice || !p2?.askPrice || !p3?.askPrice) return;

          let lv_calc, lv_str;

          // шаг 1
          const fee1 = getFee(d.lv1);
          if (d.l1 === "num") {
            lv_calc = p1.bidPrice;
            lv_str = `${d.d1}->${d.lv1}['bidP':'${p1.bidPrice}'] (fee:${fee1}) -> ${d.d2}<br/>`;
          } else {
            lv_calc = 1 / p1.askPrice;
            lv_str = `${d.d1}->${d.lv1}['askP':'${p1.askPrice}'] (fee:${fee1}) -> ${d.d2}<br/>`;
          }
          lv_calc *= (1 - fee1);

          // шаг 2
          const fee2 = getFee(d.lv2);
          if (d.l2 === "num") {
            lv_calc *= p2.bidPrice;
            lv_str += `${d.d2}->${d.lv2}['bidP':'${p2.bidPrice}'] (fee:${fee2}) -> ${d.d3}<br/>`;
          } else {
            lv_calc *= 1 / p2.askPrice;
            lv_str += `${d.d2}->${d.lv2}['askP':'${p2.askPrice}'] (fee:${fee2}) -> ${d.d3}<br/>`;
          }
          lv_calc *= (1 - fee2);

          // шаг 3
          const fee3 = getFee(d.lv3);
          if (d.l3 === "num") {
            lv_calc *= p3.bidPrice;
            lv_str += `${d.d3}->${d.lv3}['bidP':'${p3.bidPrice}'] (fee:${fee3}) -> ${d.d1}`;
          } else {
            lv_calc *= 1 / p3.askPrice;
            lv_str += `${d.d3}->${d.lv3}['askP':'${p3.askPrice}'] (fee:${fee3}) -> ${d.d1}`;
          }
          lv_calc *= (1 - fee3);

          d.tpath = lv_str;
          d.value = parseFloat(((lv_calc - 1) * 100).toFixed(3));

          // ===== ПРОСТОЙ ЧЕК ОБЪЁМА: нужная сторона qty > 0 на каждой ноге
          const needSides = [
            d.l1 === "num" ? ["bid", d.lv1] : ["ask", d.lv1],
            d.l2 === "num" ? ["bid", d.lv2] : ["ask", d.lv2],
            d.l3 === "num" ? ["bid", d.lv3] : ["ask", d.lv3],
          ];
          const zeros = needSides
              .filter(([side, sym]) => !qtySideOk(sym, side))
              .map(([_, sym]) => sym);

          const id = pairId(d);
          const prevSt = flowState.get(id) || {
            active: false, posTicks: 0, negTicks: 0,
            startTs: 0, lastValue: -Infinity,
            notifiedStart: false, notifiedEndAt: 0,
            watchSyms: null, lastChangeAtStart: null
          };
          const st = { ...prevSt };

          // Конструируем список наблюдаемых (без USDT)
          const observed = [d.lv1, d.lv2, d.lv3].filter(s => !s.includes("USDT"));

          // анти-залипание: и по таймеру, и по истории последних L1
          if (st.active) {
            let frozenList = [];
            if (observed.length) {
              const nowTs = now;
              const byTimer = observed.filter(s => (nowTs - (lastChange[s] || 0)) >= FLOW_NOCHANGE_CLOSE_MS);
              const byHist  = observed.filter(s => !hasRecentL1Movement(s)); // нет движения в последних N
              const comboSet = new Set([...byTimer, ...byHist]);
              frozenList = [...comboSet];
            }

            const shouldClose = FLOW_NOCHANGE_REQUIRE_ALL
              ? (frozenList.length === observed.length && observed.length > 0)
              : (frozenList.length > 0);

            if (L1_REQUIRE_MOVEMENT_TO_KEEP && shouldClose) {
              st.active = false;
              const endTs = now;
              const durationSec = (endTs - (st.startTs || endTs)) / 1000;
              const reason = `nochange:${frozenList.join(",")}`;
              if (!st.notifiedEndAt) {
                const endMsg = formatFlowEndMsg(d, durationSec, reason);
                sendTelegram(endMsg, "HTML", { force: true }).catch(() => {});
                logFlow("end", d, { durationSec, reason });
                eventEmitter.emit("FLOW_END", { id, value: d.value, path: d.tpath, durationSec, ts: endTs, reason });
                st.notifiedEndAt = endTs;
                writeFlowEvent({
                  ts: endTs,
                  kind: "end",
                  valuePct: d.value,
                  pathText: d.tpath.replace(/<br\/>/g, "\n"),
                  durationSec,
                  reason,
                  pairId: pairId(d), lv1: d.lv1, lv2: d.lv2, lv3: d.lv3
                });
              }
              st.posTicks = 0; st.negTicks = 0; st.notifiedStart = false;
              flowState.set(id, st);
              return;
            }
          }

          // если где-то qty == 0 — не стартуем/закрываем
          if (zeros.length > 0) {
            const reason = `qty0:${zeros.join(",")}`;
            if (st.active) {
              st.active = false;
              const endTs = now;
              const durationSec = (endTs - (st.startTs || endTs)) / 1000;
              if (!st.notifiedEndAt) {
                const endMsg = formatFlowEndMsg(d, durationSec, reason);
                sendTelegram(endMsg, "HTML", { force: true }).catch(() => {});
                logFlow("end", d, { durationSec, reason });
                eventEmitter.emit("FLOW_END", { id, value: d.value, path: d.tpath, durationSec, ts: endTs, reason });
                st.notifiedEndAt = endTs;
                writeFlowEvent({
                  ts: endTs,
                  kind: "end",
                  valuePct: d.value,
                  pathText: d.tpath.replace(/<br\/>/g, "\n"),
                  durationSec,
                  reason,
                  pairId: pairId(d), lv1: d.lv1, lv2: d.lv2, lv3: d.lv3
                });
              }
              st.posTicks = 0; st.negTicks = 0; st.notifiedStart = false;
            } else {
              st.posTicks = 0;
              st.negTicks += 1;
            }
            st.lastValue = d.value;
            flowState.set(id, st);
            return;
          }

          // обычная логика потоков
          const legacyKey = pairId(d);
          if (d.value > 0) tickCounters[legacyKey] = (tickCounters[legacyKey] || 0) + 1;
          else tickCounters[legacyKey] = 0;

          if (d.value > FLOW_THRESHOLD) {
            st.posTicks += 1;
            st.negTicks = 0;

            // Перед стартом: проверяем, что на НЕ-USDT ногах есть движение по истории L1
            if (!st.active && st.posTicks >= MIN_TICKS_TO_SHOW) {
              const histFrozen = observed.filter(s => !hasRecentL1Movement(s));
              if (L1_REQUIRE_MOVEMENT_TO_START && histFrozen.length > 0) {
                // не стартуем, пока стаканы плоские
                st.notifiedStart = false;
                st.lastValue = d.value;
                flowState.set(id, st);
                return;
              }

              st.active = true;
              const startTs = now;
              st.startTs = startTs;
              st.notifiedEndAt = 0;
              st.notifiedStart = true;

              // наблюдаем только те ноги, где НЕТ USDT
              st.watchSyms = observed;
              st.lastChangeAtStart = {
                [d.lv1]: lastChange[d.lv1] || 0,
                [d.lv2]: lastChange[d.lv2] || 0,
                [d.lv3]: lastChange[d.lv3] || 0,
              };

              const startMsg = formatFlowStartMsg(d);
              sendTelegram(startMsg, "HTML", { force: true }).catch(() => {});
              logFlow("start", d);
              eventEmitter.emit("FLOW_START", { id, value: d.value, path: d.tpath, ts: st.startTs });

              writeFlowEvent({
                ts: startTs,
                kind: "start",
                valuePct: d.value,
                pathText: d.tpath.replace(/<br\/>/g, "\n"),
                pairId: pairId(d), lv1: d.lv1, lv2: d.lv2, lv3: d.lv3
              });
            }
          } else if (d.value < FLOW_THRESHOLD) {
            st.negTicks += 1;
            st.posTicks = 0;

            if (st.active && st.negTicks >= MIN_TICKS_TO_SHOW) {
              st.active = false;
              const endTs = now;
              const durationSec = (endTs - (st.startTs || endTs)) / 1000;

              if (!st.notifiedEndAt) {
                const endMsg = formatFlowEndMsg(d, durationSec);
                sendTelegram(endMsg, "HTML", { force: true }).catch(() => {});
                logFlow("end", d, { durationSec });
                eventEmitter.emit("FLOW_END", { id, value: d.value, path: d.tpath, durationSec, ts: endTs });
                st.notifiedEndAt = endTs;

                writeFlowEvent({
                  ts: endTs,
                  kind: "end",
                  valuePct: d.value,
                  pathText: d.tpath.replace(/<br\/>/g, "\n"),
                  durationSec,
                  pairId: pairId(d), lv1: d.lv1, lv2: d.lv2, lv3: d.lv3
                });
              }

              st.posTicks = 0; st.negTicks = 0; st.notifiedStart = false;
            }
          } else {
            st.posTicks = 0;
            st.negTicks = 0;
          }

          st.lastValue = d.value;
          flowState.set(id, st);
        });

    ensureBroadcastLoop();

  } catch (err) {
    error(err);
  }
};

// ==== WS ====
const wsconnect = () => {
  ws = new Websocket("wss://stream.bybit.com/v5/public/spot");
  subs = validSymbols.map((symbol) => `orderbook.${ORDERBOOK_DEPTH}.${symbol}`);

  ws.on("open", async () => {
    log(`[ws] Открыто. Подписываюсь на orderbook.${ORDERBOOK_DEPTH} для всех символов…`);

    const chunkSize = 10;
    for (let i = 0; i < subs.length; i += chunkSize) {
      const args = subs.slice(i, i + chunkSize);
      await delay(1000);
      ws.send(JSON.stringify({ op: "subscribe", args }));
      log(`[ws] осталось подписать ~${Math.max(subs.length - (i + chunkSize), 0)}`);
    }

    log("[ws] Подписки установлены. Открой http://127.0.0.1:3000/");

    if (!wsConnectedOnce) {
      wsConnectedOnce = true;
      sendTelegram("🚀 Бот запущен и подключен к Bybit Spot").catch(() => {});
    }
  });

  ws.on("close", (code, reason) => {
    log(`[ws] Закрыто. code=${code} reason=${reason?.toString?.() || ""}`);
    sendTelegram("⚠️ Соединение с Bybit Spot закрыто").catch(() => {});
  });

  ws.on("error", (e) => log("[ws ошибка]", e?.message || e));
  ws.on("message", processData);

  setInterval(() => {
    if (ws.readyState === Websocket.OPEN) ws.ping();
  }, 20 * 1000);
};

// ==== Грейсфул-шатдаун ====
function shutdownHandler(sig) {
  return () => {
    log(`[proc] Получен ${sig}, завершаю работу…`);
    sendTelegram("🛑 Бот остановлен").catch(() => {});
    try { if (ws && ws.readyState === Websocket.OPEN) ws.close(1000, "shutdown"); } catch (_) {}
    if (broadcastTimer) { clearInterval(broadcastTimer); broadcastTimer = null; }
    setTimeout(() => process.exit(0), 300);
  };
}
process.on("SIGINT", shutdownHandler("SIGINT"));
process.on("SIGTERM", shutdownHandler("SIGTERM"));

module.exports = { getPairs, wsconnect, eventEmitter, getBalance, getBalances };
