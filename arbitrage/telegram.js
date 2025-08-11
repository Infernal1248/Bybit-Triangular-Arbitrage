const { error } = console;
const got = require('got');
const { TG_TOKEN, TG_CHAT_ID, TG_MIN_INTERVAL } = require('./config');

let lastTgSentAt = 0;

async function sendTelegram(text, parseMode = 'HTML', opts = {}) {
  if (!TG_TOKEN || !TG_CHAT_ID) return;
  const now = Date.now();
  if (!opts.force && (now - lastTgSentAt < TG_MIN_INTERVAL)) return;
  try {
    await got.post(`https://api.telegram.org/bot${TG_TOKEN}/sendMessage`, {
      json: { chat_id: TG_CHAT_ID, text, parse_mode: parseMode, disable_web_page_preview: true },
      timeout: { request: 10000 },
    });
    if (!opts.force) lastTgSentAt = now;
  } catch (e) {
    error('[tg] sendMessage error:', e.message);
  }
}

function formatFlowStartMsg(d) {
  const path = d.tpath.replace(/<br\/>/g, '\n');
  return `🟢 <b>Поток начался</b>\n<code>${d.value.toFixed(3)}%</code>\n${path}`;
}

function formatFlowEndMsg(d, durationSec, reason = '') {
  const path = d.tpath.replace(/<br\/>/g, '\n');
  const r = reason ? `\nПричина: <code>${reason}</code>` : '';
  return `🛑 <b>Поток закончился</b>\nДлился: <code>${durationSec.toFixed(2)}s</code>\nПоследний профит: <code>${d.value.toFixed(3)}%</code>${r}\n${path}`;
}

module.exports = { sendTelegram, formatFlowStartMsg, formatFlowEndMsg };
