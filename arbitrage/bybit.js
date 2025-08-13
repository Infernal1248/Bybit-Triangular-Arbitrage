const got = require('got');
const { BYBIT_KEY, RECV_WINDOW } = require('./config');
const { signQuery } = require('./auth');

async function bybitRequest(path, { method = 'GET', query = '', body } = {}) {
  const url = `https://api.bybit.com${path}${query ? `?${query}` : ''}`;
  const { timestamp, sign } = signQuery(query);
  const options = {
    method,
    headers: {
      'X-BAPI-API-KEY': BYBIT_KEY,
      'X-BAPI-SIGN': sign,
      'X-BAPI-TIMESTAMP': timestamp,
      'X-BAPI-RECV-WINDOW': RECV_WINDOW.toString(),
    },
    responseType: 'json',
    timeout: { request: 10000 },
  };
  if (method !== 'GET' && body) {
    options.json = body;
  }
  const res = await got(url, options);
  return res.body;
}

module.exports = { bybitRequest };
