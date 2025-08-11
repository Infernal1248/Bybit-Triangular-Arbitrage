const crypto = require('crypto');
const { BYBIT_KEY, BYBIT_SECRET, RECV_WINDOW } = require('./config');

function signQuery(query) {
  const timestamp = Date.now().toString();
  const payload = `${timestamp}${BYBIT_KEY}${RECV_WINDOW}${query}`;
  const sign = crypto.createHmac('sha256', BYBIT_SECRET).update(payload).digest('hex');
  return { timestamp, sign };
}

module.exports = { signQuery };
