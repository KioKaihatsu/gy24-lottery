// GY24 QRエントリー＆抽選サーバー（依存パッケージなし / Node標準モジュールのみ）
// 役割:
//   /        … 参加者がスマホで開くエントリーフォーム（QRのリンク先）
//   /host    … 主催者の画面（QR表示・登録状況・ルーレット抽選）
//   /api/*   … 登録/取得/抽選/リセット + SSE(リアルタイム配信)
const http = require('http');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 3200;
const ADMIN_KEY = process.env.ADMIN_KEY || ''; // 設定すると /host の抽選・リセットに鍵が必要
const PUBLIC = path.join(__dirname, 'public');
const DATA_FILE = path.join(__dirname, 'data', 'entries.json');

// ---------- データ ----------
let entries = load();
function load() {
  try { return JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')); } catch (e) { return []; }
}
function save() {
  try {
    fs.mkdirSync(path.dirname(DATA_FILE), { recursive: true });
    fs.writeFileSync(DATA_FILE, JSON.stringify(entries, null, 2));
  } catch (e) { console.error('save failed', e); }
}
function uid() { return 'e' + Date.now().toString(36) + Math.floor(Math.random() * 1e6).toString(36); }

// ---------- SSE（リアルタイム配信） ----------
const clients = new Set();
function broadcast(type, payload) {
  const msg = `data: ${JSON.stringify({ type, ...payload })}\n\n`;
  for (const res of clients) { try { res.write(msg); } catch (e) {} }
}
function publicState() {
  return { count: entries.length, won: entries.filter(e => e.won).length };
}

// ---------- ヘルパ ----------
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8', '.json': 'application/json; charset=utf-8',
  '.png': 'image/png', '.svg': 'image/svg+xml', '.ico': 'image/x-icon' };
function sendFile(res, file) {
  fs.readFile(file, (err, data) => {
    if (err) { res.writeHead(404); return res.end('not found'); }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(file).toLowerCase()] || 'application/octet-stream' });
    res.end(data);
  });
}
function json(res, code, obj) {
  res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(obj));
}
function readBody(req) {
  return new Promise((resolve) => {
    let b = ''; req.on('data', c => { b += c; if (b.length > 1e5) req.destroy(); });
    req.on('end', () => { try { resolve(JSON.parse(b || '{}')); } catch (e) { resolve({}); } });
  });
}
function adminOk(req, url) {
  if (!ADMIN_KEY) return true;
  const key = req.headers['x-admin-key'] || url.searchParams.get('key');
  return key === ADMIN_KEY;
}

// ---------- サーバ ----------
// 不正なリクエストやハンドラ内の例外でプロセスが落ちないようにする
process.on('unhandledRejection', e => console.error('unhandledRejection:', e && e.message));
process.on('uncaughtException', e => console.error('uncaughtException:', e && e.message));

const server = http.createServer((req, res) => {
  handle(req, res).catch(err => {
    console.error('handler error:', err && err.message);
    try { if (!res.headersSent) { res.writeHead(500); res.end('error'); } else res.end(); } catch (e) {}
  });
});

async function handle(req, res) {
  // ヘルスチェック用（ファイルI/Oを伴わない軽量応答）
  if (req.url === '/healthz') { res.writeHead(200, { 'Content-Type': 'text/plain' }); return res.end('ok'); }

  let url;
  try { url = new URL(req.url, `http://${req.headers.host || 'localhost'}`); }
  catch (e) { res.writeHead(400); return res.end('bad request'); }
  const p = url.pathname;

  // --- API ---
  if (p === '/api/entries') {
    return json(res, 200, { entries, ...publicState() });
  }

  if (p === '/api/stream') { // SSE
    res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache',
      Connection: 'keep-alive', 'X-Accel-Buffering': 'no' });
    res.write('retry: 3000\n\n');
    res.write(`data: ${JSON.stringify({ type: 'init', entries, ...publicState() })}\n\n`);
    clients.add(res);
    const ping = setInterval(() => { try { res.write(': ping\n\n'); } catch (e) {} }, 25000);
    req.on('close', () => { clearInterval(ping); clients.delete(res); });
    return;
  }

  if (p === '/api/entry' && req.method === 'POST') {
    const b = await readBody(req);
    const name = String(b.name || '').trim().slice(0, 40);
    const fav = String(b.fav || '').trim().slice(0, 60);
    if (!name) return json(res, 400, { ok: false, error: 'name required' });
    const e = { id: uid(), name, fav, won: false, ts: Date.now() };
    entries.push(e); save();
    broadcast('add', { entry: e, ...publicState() });
    return json(res, 200, { ok: true, count: entries.length });
  }

  if (p === '/api/draw' && req.method === 'POST') {
    if (!adminOk(req, url)) return json(res, 403, { ok: false, error: 'forbidden' });
    const b = await readBody(req);
    const pool = b.excludeWon === false ? entries : entries.filter(e => !e.won);
    if (!pool.length) return json(res, 200, { ok: false, error: 'empty' });
    const winner = pool[Math.floor(Math.random() * pool.length)];
    winner.won = true; save();
    broadcast('draw', { winner, ...publicState() });
    return json(res, 200, { ok: true, winner });
  }

  if (p === '/api/reset' && req.method === 'POST') {
    if (!adminOk(req, url)) return json(res, 403, { ok: false, error: 'forbidden' });
    entries = []; save();
    broadcast('reset', { ...publicState() });
    return json(res, 200, { ok: true });
  }

  // --- ページ / 静的ファイル ---
  if (p === '/' || p === '/entry') return sendFile(res, path.join(PUBLIC, 'entry.html'));
  if (p === '/host') return sendFile(res, path.join(PUBLIC, 'host.html'));

  const safe = path.normalize(p).replace(/^(\.\.[/\\])+/, '');
  const file = path.join(PUBLIC, safe);
  if (file.startsWith(PUBLIC) && fs.existsSync(file) && fs.statSync(file).isFile()) return sendFile(res, file);

  res.writeHead(404); res.end('not found');
}

server.listen(PORT, '0.0.0.0', () => {
  console.log(`GY24 抽選アプリ起動: http://localhost:${PORT}`);
  console.log(`  参加者用(QRリンク先): http://localhost:${PORT}/`);
  console.log(`  主催者用(QR表示/抽選): http://localhost:${PORT}/host`);
  if (ADMIN_KEY) console.log('  ADMIN_KEY 有効: /host?key=... で操作');
});
