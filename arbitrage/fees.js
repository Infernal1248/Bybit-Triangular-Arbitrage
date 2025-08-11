const { log, error } = console;
const got = require('got');
const { BYBIT_KEY, BYBIT_SECRET, RECV_WINDOW, FEE_MODE, DEFAULT_SPOT_FEE } = require('./config');
const { signQuery } = require('./auth');

const feeRates = {};

async function fetchSpotFeeRates() {
  if (!BYBIT_KEY || !BYBIT_SECRET) {
    log('[fee] BYBIT_KEY/SECRET не заданы — применяю DEFAULT_SPOT_FEE ко всем парам');
    return;
  }
  const endpoint = 'https://api.bybit.com/v5/account/fee-rate';
  const query = 'category=spot';
  const { timestamp, sign } = signQuery(query);
  try {
    const res = await got(`${endpoint}?${query}`, {
      method: 'GET',
      headers: {
        'X-BAPI-API-KEY': BYBIT_KEY,
        'X-BAPI-SIGN': sign,
        'X-BAPI-TIMESTAMP': timestamp,
        'X-BAPI-RECV-WINDOW': RECV_WINDOW.toString(),
      },
      responseType: 'json',
      timeout: { request: 10000 },
    });
    const list = res.body?.result?.list || [];
    list.forEach((it) => {
      const s = it.symbol;
      const maker = Number(it.makerFeeRate ?? DEFAULT_SPOT_FEE);
      const taker = Number(it.takerFeeRate ?? DEFAULT_SPOT_FEE);
      feeRates[s] = { maker, taker };
    });
    log(`[fee] Комиссии загружены для ${Object.keys(feeRates).length} символов (режим: ${FEE_MODE}).`);
  } catch (e) {
    error('[fee] Ошибка получения комиссий, использую DEFAULT_SPOT_FЕЕ:', e.message);
  }
}

function getFee(symbol) {
  const fr = feeRates[symbol];
  if (!fr) return DEFAULT_SPOT_FEE;
  return FEE_MODE === 'maker' ? fr.maker : fr.taker;
}

module.exports = { fetchSpotFeeRates, getFee };
