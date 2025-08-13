const { log, error } = console;
const fs = require('fs');
const path = require('path');
const { ARB_LOG_DIR, ARB_LOG_FORMAT } = require('./config');

function fmtLocal(ts) {
  const d = new Date(ts);
  return new Intl.DateTimeFormat(undefined, {
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
    hour12: false,
  }).format(d).replace(',', '');
}

function logFileFor(ts) {
  const d = new Date(ts);
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, '0');
  const da = String(d.getDate()).padStart(2, '0');
  const ext = ARB_LOG_FORMAT === 'txt' ? '.txt' : '.jsonl';
  return path.join(ARB_LOG_DIR, `${y}-${m}-${da}${ext}`);
}

function writeFlowEvent(evt) {
  const file = logFileFor(evt.ts);
  try {
    if (ARB_LOG_FORMAT === 'txt') {
      const lines = [
        fmtLocal(evt.ts),
        evt.pathText || '',
        `Profit: ${Number(evt.valuePct).toFixed(3)}%`,
      ];
      if (evt.kind === 'end') {
        const extra = [];
        if (Number.isFinite(evt.durationSec)) extra.push(`Duration: ${evt.durationSec.toFixed(2)}s`);
        if (evt.reason) extra.push(`Reason: ${evt.reason}`);
        if (extra.length) lines.push(extra.join(' | '));
      }
      lines.push('');
      fs.appendFile(file, lines.join('\n'), () => {});
    } else {
      const rec = {
        ts: new Date(evt.ts).toISOString(),
        kind: evt.kind,
        valuePct: evt.valuePct,
        pathText: evt.pathText,
        durationSec: evt.durationSec ?? null,
        reason: evt.reason ?? null,
        pairId: evt.pairId,
        lv1: evt.lv1, lv2: evt.lv2, lv3: evt.lv3,
      };
      fs.appendFile(file, JSON.stringify(rec) + '\n', () => {});
    }
  } catch (e) {
    error('[log] ошибка writeFlowEvent:', e.message);
  }
}

function ensureLogDir() {
  try {
    if (!fs.existsSync(ARB_LOG_DIR)) fs.mkdirSync(ARB_LOG_DIR, { recursive: true });
  } catch (e) {
    error('[log] ошибка mkdir:', e.message);
  }
}

function logFilePath(d = new Date()) {
  const day = d.toISOString().slice(0, 10);
  const ext = ARB_LOG_FORMAT === 'txt' ? 'txt' : 'json';
  return path.join(ARB_LOG_DIR, `flows-${day}.${ext}`);
}

function formatTxtEntry(d, kind, extra) {
  const ts = new Date().toISOString();
  const body = d.tpath.replace(/<br\/>/g, '\n');
  const profit = `{${d.value.toFixed(3)}%}`;
  const reason = extra?.reason ? `\n{reason:${extra.reason}}` : '';
  const dur = typeof extra?.durationSec === 'number' ? `\n{duration:${extra.durationSec.toFixed(2)}s}` : '';
  return `{${ts}}\n${body}\n${profit}${dur}${reason}\n\n`;
}

function logFlow(kind, d, extra = {}) {
  try {
    ensureLogDir();
    const file = logFilePath();
    if (ARB_LOG_FORMAT === 'txt') {
      fs.appendFileSync(file, formatTxtEntry(d, kind, extra), 'utf8');
    } else {
      const row = {
        ts: new Date().toISOString(),
        kind,
        valuePct: Number(d.value.toFixed(3)),
        pathHtml: d.tpath,
        pathText: d.tpath.replace(/<br\/>/g, '\n'),
        durationSec: extra.durationSec ?? null,
        reason: extra.reason ?? null,
        pairId: d.pairId || undefined,
        lv1: d.lv1, lv2: d.lv2, lv3: d.lv3,
      };
      fs.appendFileSync(file, JSON.stringify(row) + '\n', 'utf8');
    }
  } catch (e) {
    error('[log] ошибка записи:', e.message);
  }
}

module.exports = { writeFlowEvent, logFlow };
