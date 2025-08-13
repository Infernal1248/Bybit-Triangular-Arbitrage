const { log, error } = console;
const { BYBIT_KEY, BYBIT_SECRET } = require('./config');
const { bybitRequest } = require('./bybit');

async function getBalance(coin = 'USDT') {
  if (!BYBIT_KEY || !BYBIT_SECRET) {
    log('[bal] BYBIT_KEY/SECRET не заданы — считаю баланс равным 0');
    return 0;
  }
  try {
    const data = await bybitRequest('/v5/account/wallet-balance', { query: `accountType=SPOT&coin=${coin}` });
    const list = data?.result?.list || [];
    const coins = list[0]?.coin || [];
    const item = coins.find((c) => c.coin === coin);
    const bal = Number(item?.walletBalance || 0);
    log(`[bal] Текущий баланс ${coin}: ${bal}`);
    return bal;
  } catch (e) {
    error(`[bal] Не удалось получить баланс ${coin}:`, e.message);
    return 0;
  }
}

async function getBalances() {
  if (!BYBIT_KEY || !BYBIT_SECRET) {
    log('[bal] BYBIT_KEY/SECRET не заданы — считаю, что балансы отсутствуют');
    return {};
  }
  try {
    const data = await bybitRequest('/v5/account/wallet-balance', { query: 'accountType=SPOT' });
    const list = data?.result?.list || [];
    const coins = list[0]?.coin || [];
    const balances = {};
    coins.forEach((c) => {
      const bal = Number(c.walletBalance || 0);
      if (bal !== 0) {
        balances[c.coin] = bal;
      }
    });
    log('[bal] Текущие ненулевые балансы:', balances);
    return balances;
  } catch (e) {
    error('[bal] Не удалось получить балансы:', e.message);
    return {};
  }
}

module.exports = { getBalance, getBalances };
