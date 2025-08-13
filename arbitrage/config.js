const BYBIT_KEY = process.env.BYBIT_KEY || "";
const BYBIT_SECRET = process.env.BYBIT_SECRET || "";
const RECV_WINDOW = 5000;
const FEE_MODE = (process.env.FEE_MODE || "taker").toLowerCase(); // 'taker' | 'maker'
const DEFAULT_SPOT_FEE = 0.001; // 0.1% fallback

const TG_TOKEN = process.env.TELEGRAM_BOT_TOKEN || "";
const TG_CHAT_ID = process.env.TELEGRAM_CHAT_ID || "";
const TG_MIN_INTERVAL = Number(process.env.TELEGRAM_MIN_INTERVAL_MS || 3000);

const FLOW_THRESHOLD = Number(process.env.FLOW_THRESHOLD ?? 0.05);   // %
const MIN_TICKS_TO_SHOW = Number(process.env.MIN_TICKS_TO_SHOW ?? 3);

const FLOW_NOCHANGE_CLOSE_MS = Number(process.env.FLOW_NOCHANGE_CLOSE_MS ?? 30000);
const FLOW_NOCHANGE_REQUIRE_ALL = String(process.env.FLOW_NOCHANGE_REQUIRE_ALL ?? "false").toLowerCase() === "true";

const L1_HISTORY_LEN = Number(process.env.L1_HISTORY_LEN ?? 7); // default 7
const L1_MIN_DISTINCT_STATES = Number(process.env.L1_MIN_DISTINCT_STATES ?? 2);
const L1_REQUIRE_MOVEMENT_TO_START = String(process.env.L1_REQUIRE_MOVEMENT_TO_START ?? "true").toLowerCase() === "true";
const L1_REQUIRE_MOVEMENT_TO_KEEP = String(process.env.L1_REQUIRE_MOVEMENT_TO_KEEP ?? "true").toLowerCase() === "true";

const ARB_LOG_DIR = process.env.ARB_LOG_DIR || "logs";
const ARB_LOG_FORMAT = (process.env.ARB_LOG_FORMAT || "json").toLowerCase(); // 'txt' | 'json'

const ORDERBOOK_DEPTH = Number(process.env.ORDERBOOK_DEPTH || 1);

module.exports = {
  BYBIT_KEY,
  BYBIT_SECRET,
  RECV_WINDOW,
  FEE_MODE,
  DEFAULT_SPOT_FEE,
  TG_TOKEN,
  TG_CHAT_ID,
  TG_MIN_INTERVAL,
  FLOW_THRESHOLD,
  MIN_TICKS_TO_SHOW,
  FLOW_NOCHANGE_CLOSE_MS,
  FLOW_NOCHANGE_REQUIRE_ALL,
  L1_HISTORY_LEN,
  L1_MIN_DISTINCT_STATES,
  L1_REQUIRE_MOVEMENT_TO_START,
  L1_REQUIRE_MOVEMENT_TO_KEEP,
  ARB_LOG_DIR,
  ARB_LOG_FORMAT,
  ORDERBOOK_DEPTH,
};
