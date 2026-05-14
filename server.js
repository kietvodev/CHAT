const http = require('http');
const fs   = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');
const Database = require('better-sqlite3');

const PORT    = 3000;
const DATA    = path.join(__dirname, 'data');
const UPLOADS = path.join(__dirname, 'uploads');

if (!fs.existsSync(DATA))    fs.mkdirSync(DATA,    { recursive: true });
if (!fs.existsSync(UPLOADS)) fs.mkdirSync(UPLOADS, { recursive: true });

// ─── SQLITE ───────────────────────────────────────────────────────────────────
const db = new Database(path.join(DATA, 'chat.db'));
db.pragma('journal_mode = WAL');
db.pragma('synchronous = NORMAL');
db.pragma('foreign_keys = ON');

db.exec(`
  CREATE TABLE IF NOT EXISTS users (
    name         TEXT PRIMARY KEY,
    lang         TEXT NOT NULL DEFAULT 'en',
    phone        TEXT NOT NULL DEFAULT '',
    email        TEXT NOT NULL DEFAULT '',
    avatar_color TEXT NOT NULL DEFAULT '#6366f1',
    avatar_image TEXT NOT NULL DEFAULT '',
    last_seen    INTEGER NOT NULL DEFAULT 0
  );

  CREATE TABLE IF NOT EXISTS messages (
    id                 TEXT    PRIMARY KEY,
    from_user          TEXT    NOT NULL,
    to_user            TEXT,
    from_lang          TEXT    NOT NULL DEFAULT 'en',
    to_lang            TEXT    NOT NULL DEFAULT 'en',
    text               TEXT    NOT NULL DEFAULT '',
    translated_text    TEXT    NOT NULL DEFAULT '',
    url                TEXT    NOT NULL DEFAULT '',
    caption            TEXT    NOT NULL DEFAULT '',
    translated_caption TEXT    NOT NULL DEFAULT '',
    msg_type           TEXT    NOT NULL DEFAULT 'chat',
    ts                 INTEGER NOT NULL,
    reactions          TEXT    NOT NULL DEFAULT '{}',
    deleted            INTEGER NOT NULL DEFAULT 0
  );

  CREATE INDEX IF NOT EXISTS idx_msg_conv ON messages(from_user, to_user, ts);
  CREATE INDEX IF NOT EXISTS idx_msg_ts   ON messages(ts);

  CREATE TABLE IF NOT EXISTS config (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
  );
`);

// ─── CONFIG ───────────────────────────────────────────────────────────────────
const DEF_CONFIG = {
    adminPass : 'admin123',
    ltUrl     : 'https://api.languagetool.org'
};

const stmtGetCfg = db.prepare('SELECT value FROM config WHERE key = ?');
const stmtSetCfg = db.prepare(
    'INSERT INTO config(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value'
);

function getCfgVal(k)    { const r = stmtGetCfg.get(k); return r ? r.value : null; }
function setCfgVal(k, v) { stmtSetCfg.run(k, v); }

// Seed defaults
for (const [k, v] of Object.entries(DEF_CONFIG)) {
    if (getCfgVal(k) === null) setCfgVal(k, typeof v === 'string' ? v : JSON.stringify(v));
}

let cfg = {
    adminPass : getCfgVal('adminPass'),
    ltUrl     : getCfgVal('ltUrl') || 'https://api.languagetool.org'
};

function saveCfg() {
    setCfgVal('adminPass', cfg.adminPass);
    setCfgVal('ltUrl',     cfg.ltUrl);
}

// ─── PREPARED STATEMENTS ──────────────────────────────────────────────────────
const stmtUpsertUser = db.prepare(`
    INSERT INTO users(name, lang, phone, email, avatar_color, avatar_image, last_seen)
    VALUES(@name, @lang, @phone, @email, @avatarColor, @avatarImage, @lastSeen)
    ON CONFLICT(name) DO UPDATE SET
        lang=excluded.lang, phone=excluded.phone, email=excluded.email,
        avatar_color=excluded.avatar_color, avatar_image=excluded.avatar_image,
        last_seen=excluded.last_seen
`);

const stmtInsertMsg = db.prepare(`
    INSERT INTO messages(id, from_user, to_user, from_lang, to_lang,
        text, translated_text, url, caption, translated_caption, msg_type, ts)
    VALUES(@id, @fromUser, @toUser, @fromLang, @toLang,
        @text, @translatedText, @url, @caption, @translatedCaption, @msgType, @ts)
`);

const stmtGetHistory    = db.prepare(`SELECT * FROM messages WHERE deleted=0 AND (from_user=? OR to_user=?) ORDER BY ts DESC LIMIT 200`);
const stmtGetMsg        = db.prepare('SELECT id, from_user, reactions FROM messages WHERE id=?');
const stmtDeleteMsg     = db.prepare('UPDATE messages SET deleted=1 WHERE id=? AND from_user=?');
const stmtDeleteUserMsg = db.prepare('UPDATE messages SET deleted=1 WHERE from_user=?');
const stmtGetUserMsgIds = db.prepare('SELECT id FROM messages WHERE from_user=? AND deleted=0');
const stmtUpdateReact   = db.prepare('UPDATE messages SET reactions=? WHERE id=?');
const stmtSetLastSeen   = db.prepare('UPDATE users SET last_seen=? WHERE name=?');

// ─── IN-MEMORY STATE ──────────────────────────────────────────────────────────
const clients  = new Map();  // ws → {id, name, lang, inCall}
const allUsers = new Map();  // name → {lang, online, lastSeen, phone, email, avatarColor, avatarImage}

// Hydrate users from DB on startup
for (const row of db.prepare('SELECT * FROM users').all()) {
    allUsers.set(row.name, {
        lang: row.lang, online: false, lastSeen: row.last_seen,
        phone: row.phone, email: row.email,
        avatarColor: row.avatar_color, avatarImage: row.avatar_image
    });
}

const LANGS = {
    vi:'Vietnamese', ja:'Japanese',  en:'English',    zh:'Chinese',
    ko:'Korean',     fr:'French',     de:'German',     es:'Spanish',
    th:'Thai',       ar:'Arabic',     ru:'Russian',    pt:'Portuguese',
    it:'Italian',    hi:'Hindi',      id:'Indonesian'
};

function uid()         { return crypto.randomBytes(8).toString('hex'); }
function send(ws, o)   { if (ws.readyState === 1) ws.send(JSON.stringify(o)); }
function sendAll(o,ex) { for (const [w] of clients) if (w !== ex) send(w, o); }
function sendEvery(o)  { for (const [w] of clients) send(w, o); }

function userList() {
    const list = [];
    for (const [n, u] of allUsers) {
        list.push({ name:n, lang:u.lang, online:u.online, lastSeen:u.lastSeen,
            phone:u.phone||'', email:u.email||'',
            avatarColor:u.avatarColor||'', avatarImage:u.avatarImage||'' });
    }
    return list;
}

function saveUser(name) {
    const u = allUsers.get(name);
    if (!u) return;
    stmtUpsertUser.run({ name, lang:u.lang, phone:u.phone||'',
        email:u.email||'', avatarColor:u.avatarColor||'',
        avatarImage:u.avatarImage||'', lastSeen:u.lastSeen||Date.now() });
}

function mapHistRow(row, forUser) {
    const isOwn = row.from_user === forUser;
    let reactions = {};
    try { reactions = JSON.parse(row.reactions || '{}'); } catch {}
    if (row.msg_type === 'image') {
        const cap     = !isOwn && row.translated_caption ? row.translated_caption : row.caption;
        const origCap = !isOwn && row.translated_caption && row.to_lang !== row.from_lang ? row.caption : null;
        return { type:'image', id:row.id, from:row.from_user, fromLang:row.from_lang,
            url:row.url, caption:cap, originalCaption:origCap,
            isOwn, status:'delivered', ts:row.ts, reactions, to:row.to_user, isHistory:true };
    }
    const text = !isOwn && row.translated_text ? row.translated_text : row.text;
    const orig = !isOwn && row.translated_text && row.to_lang !== row.from_lang ? row.text : null;
    return { type:'chat', id:row.id, from:row.from_user, fromLang:row.from_lang,
        text, original:orig, isOwn, status:'delivered', ts:row.ts,
        model:null, reactions, to:row.to_user, isHistory:true };
}

// ─── DEEPL TRANSLATION ────────────────────────────────────────────────────────
const DEEPL_ENDPOINT   = 'https://oneshot-free.www.deepl.com/v1/storefront/translate';
const DEEPL_CFG_FILE   = path.join(DATA, 'deepl.json');
const DEEPL_LIFETIME   = 4 * 3600 * 1000;  // cookie giả định sống 4h
const DEEPL_REFRESH_AT = 5 * 60  * 1000;  // làm mới khi còn < 5 phút

// Các ngôn ngữ DeepL hỗ trợ (map từ code nội bộ)
const DEEPL_LANG = {
    en:'en', vi:'vi', ja:'ja', zh:'zh', ko:'ko',
    fr:'fr', de:'de', es:'es', th:'th', ar:'ar',
    ru:'ru', pt:'pt', it:'it', hi:'hi', id:'id'
};

function loadDeeplCfg() {
    try {
        if (fs.existsSync(DEEPL_CFG_FILE))
            return JSON.parse(fs.readFileSync(DEEPL_CFG_FILE, 'utf8'));
        // Bootstrap lần đầu từ config của API CHAT
        const sibling = path.join(__dirname, '..', 'API CHAT', 'config.json');
        if (fs.existsSync(sibling))
            return JSON.parse(fs.readFileSync(sibling, 'utf8'));
    } catch {}
    return {};
}

function saveDeeplCfg(cookie) {
    fs.writeFileSync(DEEPL_CFG_FILE,
        JSON.stringify({ cookie, saved_at: Math.floor(Date.now() / 1000) }, null, 2));
}

function parseDeeplCookies(str) {
    const out = {};
    for (const p of str.split(';')) {
        const eq = p.indexOf('=');
        if (eq > 0) out[p.slice(0, eq).trim()] = p.slice(eq + 1).trim();
    }
    return out;
}

function splitText(text, max = 1500) {
    if (text.length <= max) return [text];
    const chunks = [];
    while (text.length) {
        if (text.length <= max) { chunks.push(text); break; }
        const seg = text.slice(0, max);
        let cut = Math.max(seg.lastIndexOf('\n'), seg.lastIndexOf('. '),
                           seg.lastIndexOf('! '), seg.lastIndexOf('? '));
        if (cut <= 0) cut = seg.lastIndexOf(' ');
        if (cut <= 0) cut = max; else cut += 1;
        chunks.push(text.slice(0, cut).trimEnd());
        text = text.slice(cut).trimStart();
    }
    return chunks.filter(c => c);
}

function logTime() { return new Date().toLocaleTimeString('vi-VN', { hour12: false }); }

async function translate(text, from, to) {
    if (from === to) return { text, model: null };
    if (!DEEPL_LANG[from] || !DEEPL_LANG[to]) {
        console.warn(`[DeepL ${logTime()}] Ngôn ngữ không hỗ trợ: ${from} → ${to}`);
        return { text, model: null };
    }
    const dc = loadDeeplCfg();
    if (!dc.cookie) {
        console.warn(`[DeepL ${logTime()}] ❌ Chưa có cookie — bỏ qua dịch`);
        return { text, model: null };
    }
    const preview = text.length > 40 ? text.slice(0, 40) + '…' : text;
    const t0 = Date.now();
    const instanceId = parseDeeplCookies(dc.cookie)['dapUid'] || '';
    const chunks  = splitText(text);
    const results = new Array(chunks.length);
    try {
        await Promise.all(chunks.map(async (chunk, i) => {
            const r = await fetch(DEEPL_ENDPOINT, {
                method: 'POST',
                headers: {
                    'accept': '*/*', 'accept-language': 'en,vi;q=0.9',
                    'content-type': 'application/json',
                    'origin': 'https://www.deepl.com',
                    'referer': 'https://www.deepl.com/',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36',
                    'cookie': dc.cookie
                },
                body: JSON.stringify({
                    text: [chunk],
                    source_lang: DEEPL_LANG[from],
                    target_lang: DEEPL_LANG[to],
                    language_model: 'next-gen', usage_type: 'Translate',
                    app_information: { instance_id: instanceId,
                        app_build: 'Chrome', os: 'Windows',
                        app_version: 'any', os_version: 'any' }
                })
            });
            if (!r.ok) {
                console.error(`[DeepL ${logTime()}] ❌ HTTP ${r.status} (${from}→${to}) — cookie có thể hết hạn`);
                throw new Error(`DeepL HTTP ${r.status}`);
            }
            const d = await r.json();
            const t = d.translations?.[0]?.text;
            if (!t) throw new Error('Không có kết quả dịch');
            results[i] = t;
        }));
        const ms = Date.now() - t0;
        console.log(`[DeepL ${logTime()}] ✓ ${from}→${to} | ${ms}ms | "${preview}"`);
        return { text: results.join('\n'), model: 'deepl' };
    } catch (e) {
        console.error(`[DeepL ${logTime()}] ❌ Lỗi dịch (${from}→${to}): ${e.message}`);
        return { text, model: null };
    }
}

async function refreshDeeplCookie() {
    let pw;
    try { pw = require('playwright'); } catch {
        console.warn(`[DeepL ${logTime()}] ⚠ Chưa cài playwright. Chạy: npm install playwright && npx playwright install chromium`);
        return false;
    }
    console.log(`[DeepL ${logTime()}] 🔄 Đang lấy cookie mới qua Playwright...`);
    const t0 = Date.now();
    let capturedCookie = null;
    try {
        const browser = await pw.chromium.launch({ headless: true });
        const ctx = await browser.newContext({
            userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36',
            locale: 'en-US'
        });
        const page = await ctx.newPage();
        page.on('request', req => {
            if (req.url().includes('storefront/translate') && !capturedCookie) {
                const h = req.headers();
                if (h['cookie']) capturedCookie = h['cookie'];
            }
        });
        await page.goto('https://www.deepl.com/en/translator', { waitUntil: 'domcontentloaded', timeout: 30000 });
        await page.waitForTimeout(3000);
        try {
            const src = page.locator("[data-testid='translator-source-input'], textarea[name='source']").first();
            await src.click({ timeout: 5000 });
            await src.fill('Hello world');
            await page.waitForTimeout(4000);
        } catch {}
        if (!capturedCookie) {
            const cookies = await ctx.cookies('https://www.deepl.com');
            if (cookies.length) capturedCookie = cookies.map(c => `${c.name}=${c.value}`).join('; ');
        }
        await browser.close();
        if (capturedCookie) {
            saveDeeplCfg(capturedCookie);
            console.log(`[DeepL ${logTime()}] ✅ Cookie làm mới thành công (${((Date.now()-t0)/1000).toFixed(1)}s)`);
            return true;
        }
        console.warn(`[DeepL ${logTime()}] ❌ Không lấy được cookie từ trình duyệt`);
    } catch (e) { console.error(`[DeepL ${logTime()}] ❌ Playwright lỗi: ${e.message}`); }
    return false;
}

function deeplCookieStatus() {
    const dc = loadDeeplCfg();
    if (!dc.cookie)    return '❌ Chưa có cookie';
    if (!dc.saved_at)  return '⚠ Cookie chưa có thời gian lưu';
    const elapsed   = Date.now() - dc.saved_at * 1000;
    const remaining = DEEPL_LIFETIME - elapsed;
    if (remaining <= 0) return '❌ Cookie đã hết hạn';
    const h = Math.floor(remaining / 3600000);
    const m = Math.floor((remaining % 3600000) / 60000);
    return `✅ Cookie còn hạn ~${h}h${m}m`;
}

function scheduleDeeplRefresh() {
    const dc = loadDeeplCfg();
    let delayMs = 0;
    if (dc.saved_at) {
        const remaining = DEEPL_LIFETIME - (Date.now() - dc.saved_at * 1000);
        delayMs = Math.max(0, remaining - DEEPL_REFRESH_AT);
    }
    const h = Math.floor(delayMs / 3600000);
    const m = Math.floor((delayMs % 3600000) / 60000);
    console.log(`[DeepL ${logTime()}] ${deeplCookieStatus()} — làm mới sau ${h}h${m}m`);
    setTimeout(async () => { await refreshDeeplCookie(); scheduleDeeplRefresh(); }, delayMs || 5000);
}

scheduleDeeplRefresh();

const ltCache = new Map(); // cache kết quả spell check

// ─── HTTP SERVER ──────────────────────────────────────────────────────────────
const MIME = {
    '.html':'text/html', '.css':'text/css', '.js':'application/javascript',
    '.png':'image/png', '.jpg':'image/jpeg', '.jpeg':'image/jpeg',
    '.gif':'image/gif', '.webp':'image/webp', '.svg':'image/svg+xml'
};
const ALLOWED_IMG = new Set(['.png','.jpg','.jpeg','.gif','.webp']);
const MAX_UPLOAD  = 10 * 1024 * 1024;

const srv = http.createServer((req, res) => {
    res.setHeader('Access-Control-Allow-Origin',  '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
    if (req.method === 'OPTIONS') { res.writeHead(200); res.end(); return; }

    if (req.method === 'POST' && req.url === '/correct') {
        let body = '';
        req.on('data', c => { body += c; });
        req.on('end', async () => {
            res.setHeader('Content-Type', 'application/json');
            try {
                const { text, lang } = JSON.parse(body);
                if (!text || text.trim().length < 3)
                    return res.end(JSON.stringify({ corrected: text, changed: false }));

                const key = `${lang}:${text.trim()}`;
                if (ltCache.has(key)) return res.end(JSON.stringify(ltCache.get(key)));

                const LT_LANG = {
                    vi:'vi', en:'en-US', fr:'fr', de:'de-DE',
                    es:'es', pt:'pt-BR', ru:'ru', it:'it',
                    zh:'zh-CN', ja:'ja', ko:'ko', ar:'ar',
                    id:'id', hi:'hi', th:'th'
                };
                const ltLang = LT_LANG[lang] || 'auto';
                const ltUrl  = (cfg.ltUrl || 'https://api.languagetool.org') + '/v2/check';
                const params = new URLSearchParams({ text: text.trim(), language: ltLang });

                const r = await fetch(ltUrl, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
                    body: params.toString()
                });
                if (!r.ok) throw new Error(`LanguageTool HTTP ${r.status}`);
                const d = await r.json();

                let corrected = text.trim();
                const matches = (d.matches || [])
                    .filter(m => m.replacements?.length > 0)
                    .sort((a, b) => b.offset - a.offset);
                for (const m of matches) {
                    const best = m.replacements[0].value;
                    corrected = corrected.slice(0, m.offset) + best + corrected.slice(m.offset + m.length);
                }

                const changed = corrected !== text.trim();
                if (changed) console.log(`[Correct ${logTime()}] ${lang} | "${text.trim().slice(0,40)}" → "${corrected.slice(0,40)}"`);
                const result = { corrected, changed };
                if (ltCache.size > 500) ltCache.clear(); // tránh tràn bộ nhớ
                ltCache.set(key, result);
                res.end(JSON.stringify(result));
            } catch (e) {
                res.writeHead(500);
                res.end(JSON.stringify({ error: e.message }));
            }
        });
        return;
    }

    if (req.method === 'POST' && req.url === '/upload') {
        let size = 0; const chunks = [];
        req.on('data', chunk => {
            size += chunk.length;
            if (size > MAX_UPLOAD) { req.destroy(); res.writeHead(413); res.end(JSON.stringify({ error:'File too large' })); return; }
            chunks.push(chunk);
        });
        req.on('end', () => {
            const body = Buffer.concat(chunks);
            const ct = req.headers['content-type'] || '';
            const bMatch = ct.match(/boundary=(.+)/);
            if (!bMatch) { res.writeHead(400); res.end(JSON.stringify({ error:'Invalid upload' })); return; }
            const parts = parseMultipart(body, bMatch[1]);
            const filePart = parts.find(p => p.filename);
            if (!filePart) { res.writeHead(400); res.end(JSON.stringify({ error:'No file' })); return; }
            const ext = path.extname(filePart.filename).toLowerCase();
            if (!ALLOWED_IMG.has(ext)) { res.writeHead(400); res.end(JSON.stringify({ error:'Only images allowed' })); return; }
            const fname = uid() + ext;
            fs.writeFileSync(path.join(UPLOADS, fname), filePart.data);
            res.writeHead(200, { 'Content-Type':'application/json' });
            res.end(JSON.stringify({ url:'/uploads/'+fname, filename:filePart.filename }));
        });
        return;
    }

    if (req.url.startsWith('/uploads/')) {
        const fname = path.basename(req.url);
        const fp    = path.join(UPLOADS, fname);
        const ext   = path.extname(fname).toLowerCase();
        if (!ALLOWED_IMG.has(ext)) { res.writeHead(403); res.end(); return; }
        fs.readFile(fp, (e, d) => {
            if (e) { res.writeHead(404); res.end('Not found'); return; }
            res.writeHead(200, { 'Content-Type':MIME[ext]||'application/octet-stream', 'Cache-Control':'public, max-age=86400' });
            res.end(d);
        });
        return;
    }

    const urlPath = req.url.split('?')[0];
    let fp;
    if (urlPath === '/' || urlPath === '/realtime.html') {
        fp = path.join(__dirname, 'realtime.html');
    } else {
        const safe = path.normalize(urlPath).replace(/^(\.\.[\/\\])+/, '');
        if (safe.includes('data') || safe.endsWith('server.js')) { res.writeHead(403); res.end(); return; }
        fp = path.join(__dirname, safe);
    }
    const ext = path.extname(fp);
    fs.readFile(fp, (e, d) => {
        if (e) { res.writeHead(404); res.end('Not found'); return; }
        const noCache = ['.html','.css','.js'].includes(ext);
        const headers = { 'Content-Type':(MIME[ext]||'text/plain')+'; charset=utf-8' };
        if (noCache) headers['Cache-Control'] = 'no-cache, no-store, must-revalidate';
        res.writeHead(200, headers);
        res.end(d);
    });
});

function parseMultipart(buf, boundary) {
    const parts = [], sep = Buffer.from('--' + boundary);
    let start = 0;
    while (true) {
        const idx = buf.indexOf(sep, start);
        if (idx === -1) break;
        if (start > 0) {
            const chunk = buf.slice(start, idx - 2);
            const headerEnd = chunk.indexOf('\r\n\r\n');
            if (headerEnd !== -1) {
                const header  = chunk.slice(0, headerEnd).toString();
                const data    = chunk.slice(headerEnd + 4);
                const fnMatch = header.match(/filename="([^"]+)"/);
                parts.push({ header, data, filename: fnMatch ? fnMatch[1] : null });
            }
        }
        start = idx + sep.length + 2;
    }
    return parts;
}

// ─── WEBSOCKET ────────────────────────────────────────────────────────────────
const wss = new WebSocketServer({ server: srv });

// Heartbeat: detect dead connections in ~12s
const HBEAT_INTERVAL = 4000;
const HBEAT_TIMEOUT  = 12000;

setInterval(() => {
    const now = Date.now();
    for (const [ws] of clients) {
        if (now - (ws._lastPong || now) > HBEAT_TIMEOUT) {
            ws.terminate();
        } else {
            ws.ping();
        }
    }
}, HBEAT_INTERVAL);

wss.on('connection', ws => {
    ws._lastPong = Date.now();
    ws.on('pong', () => { ws._lastPong = Date.now(); });

    ws.on('message', async raw => {
        let m; try { m = JSON.parse(raw); } catch { return; }

        if (m.type === 'ping') { ws._lastPong = Date.now(); send(ws, { type:'pong' }); return; }

        // ── JOIN ──────────────────────────────────────────────────────────────
        if (m.type === 'join') {
            const name = String(m.name||'').trim().slice(0, 30);
            if (!name) return send(ws, { type:'error', text:'Name required' });

            for (const [oldWs, c] of clients) {
                if (c.name === name && oldWs !== ws) {
                    send(oldWs, { type:'error', text:'Logged in from another tab' });
                    oldWs.terminate();
                    clients.delete(oldWs);
                }
            }

            const lang = LANGS[m.lang] ? m.lang : 'en';
            clients.set(ws, { id:uid(), name, lang });

            const uData = {
                lang, online:true, lastSeen:Date.now(),
                phone:       String(m.phone||'').slice(0, 20),
                email:       String(m.email||'').slice(0, 100),
                avatarColor: String(m.avatarColor||'').slice(0, 20),
                avatarImage: String(m.avatarImage||'').slice(0, 80000)
            };
            allUsers.set(name, uData);
            saveUser(name);

            send(ws, { type:'joined', name, lang });

            const rows = stmtGetHistory.all(name, name).reverse();
            send(ws, { type:'history', messages: rows.map(r => mapHistRow(r, name)) });

            sendEvery({ type:'users', users:userList() });
            return;
        }

        const me = clients.get(ws);
        if (!me) return;

        // ── CHAT ──────────────────────────────────────────────────────────────
        if (m.type === 'chat') {
            const text = String(m.text||'').trim().slice(0, 4000);
            if (!text) return;
            const id = uid(), ts = Date.now();
            const to = m.to ? String(m.to).trim() : null;

            send(ws, { type:'chat', id, from:me.name, fromLang:me.lang, text, original:null, isOwn:true, status:'sent', ts, model:null, to });

            if (to) {
                let recipientLang = null, recipientWs = null;
                for (const [cws, c] of clients) { if (c.name === to) { recipientLang=c.lang; recipientWs=cws; break; } }
                if (!recipientLang) { const u=allUsers.get(to); recipientLang=u?u.lang:'en'; }

                let translatedText = text, model = null;
                if (recipientLang !== me.lang) { const r=await translate(text,me.lang,recipientLang); translatedText=r.text; model=r.model; }

                if (recipientWs) {
                    send(recipientWs, { type:'chat', id, from:me.name, fromLang:me.lang,
                        text:translatedText, original:recipientLang!==me.lang?text:null,
                        isOwn:false, status:'delivered', ts, model, to });
                    send(ws, { type:'status', id, status:'delivered' });
                }

                stmtInsertMsg.run({ id, fromUser:me.name, toUser:to, fromLang:me.lang, toLang:recipientLang,
                    text, translatedText, url:'', caption:'', translatedCaption:'', msgType:'chat', ts });
            } else {
                const targetLangs=new Set(), cache={[me.lang]:text}; let usedModel=null;
                for (const [,c] of clients) if (c.name!==me.name) targetLangs.add(c.lang);
                for (const l of targetLangs) {
                    if (!cache[l]) { const r=await translate(text,me.lang,l); cache[l]=r.text; if(r.model) usedModel=r.model; }
                }
                let delivered = false;
                for (const [cws,c] of clients) {
                    if (cws===ws) continue;
                    send(cws, { type:'chat', id, from:me.name, fromLang:me.lang, text:cache[c.lang]||text,
                        original:c.lang!==me.lang?text:null, isOwn:false, status:'delivered', ts, model:usedModel, to:null });
                    delivered = true;
                }
                if (delivered) send(ws, { type:'status', id, status:'delivered' });
                stmtInsertMsg.run({ id, fromUser:me.name, toUser:null, fromLang:me.lang, toLang:'',
                    text, translatedText:'', url:'', caption:'', translatedCaption:'', msgType:'chat', ts });
            }
            return;
        }

        // ── IMAGE ─────────────────────────────────────────────────────────────
        if (m.type === 'image') {
            const url     = String(m.url||'').trim();
            const caption = String(m.caption||'').trim().slice(0, 500);
            if (!url || !url.startsWith('/uploads/')) return;
            const id=uid(), ts=Date.now();
            const to = m.to ? String(m.to).trim() : null;

            send(ws, { type:'image', id, from:me.name, fromLang:me.lang, url, caption, isOwn:true, status:'sent', ts, to });

            if (to) {
                let recipientLang=null, recipientWs=null;
                for (const [cws,c] of clients) { if (c.name===to) { recipientLang=c.lang; recipientWs=cws; break; } }
                if (!recipientLang) { const u=allUsers.get(to); recipientLang=u?u.lang:'en'; }

                let translatedCaption=caption, capModel=null;
                if (caption && recipientLang!==me.lang) { const r=await translate(caption,me.lang,recipientLang); translatedCaption=r.text; capModel=r.model; }

                if (recipientWs) {
                    send(recipientWs, { type:'image', id, from:me.name, fromLang:me.lang, url,
                        caption:translatedCaption, originalCaption:recipientLang!==me.lang?caption:null,
                        isOwn:false, status:'delivered', ts, to });
                    send(ws, { type:'status', id, status:'delivered' });
                }

                stmtInsertMsg.run({ id, fromUser:me.name, toUser:to, fromLang:me.lang, toLang:recipientLang,
                    text:'', translatedText:'', url, caption, translatedCaption, msgType:'image', ts });
            } else {
                let delivered=false;
                for (const [cws,c] of clients) {
                    if (cws===ws) continue;
                    let trCap=caption;
                    if (caption && c.lang!==me.lang) { const r=await translate(caption,me.lang,c.lang); trCap=r.text; }
                    send(cws, { type:'image', id, from:me.name, fromLang:me.lang, url,
                        caption:trCap, originalCaption:c.lang!==me.lang?caption:null,
                        isOwn:false, status:'delivered', ts, to:null });
                    delivered=true;
                }
                if (delivered) send(ws, { type:'status', id, status:'delivered' });
                stmtInsertMsg.run({ id, fromUser:me.name, toUser:null, fromLang:me.lang, toLang:'',
                    text:'', translatedText:'', url, caption, translatedCaption:caption, msgType:'image', ts });
            }
            return;
        }

        // ── TYPING ────────────────────────────────────────────────────────────
        if (m.type === 'typing') {
            if (m.to) {
                for (const [cws,c] of clients) { if (c.name===m.to) { send(cws,{type:'typing',from:me.name,active:!!m.active,to:me.name}); break; } }
            } else {
                sendAll({ type:'typing', from:me.name, active:!!m.active, to:null }, ws);
            }
            return;
        }

        // ── DELETE ────────────────────────────────────────────────────────────
        if (m.type === 'delete') {
            const id = String(m.id||'');
            const info = stmtDeleteMsg.run(id, me.name);
            if (info.changes > 0) sendEvery({ type:'deleted', id });
            return;
        }

        if (m.type === 'delete-user-msgs') {
            const target = String(m.user||'').trim();
            if (!target) return;
            if (target === me.name) {
                const ids = stmtGetUserMsgIds.all(target).map(r => r.id);
                stmtDeleteUserMsg.run(target);
                ids.forEach(id => sendEvery({ type:'deleted', id }));
            } else {
                send(ws, { type:'deleted-user', user:target });
            }
            return;
        }

        // ── REACT ─────────────────────────────────────────────────────────────
        if (m.type === 'react') {
            const id    = String(m.id||'');
            const emoji = String(m.emoji||'').slice(0, 8);
            const to    = m.to ? String(m.to).trim() : null;
            if (!id || !emoji) return;

            const row = stmtGetMsg.get(id);
            if (!row) return;
            let reactions = {};
            try { reactions = JSON.parse(row.reactions||'{}'); } catch {}
            if (!reactions[emoji]) reactions[emoji] = [];
            const arr = reactions[emoji], idx = arr.indexOf(me.name);
            idx !== -1 ? arr.splice(idx, 1) : arr.push(me.name);
            if (arr.length === 0) delete reactions[emoji];
            stmtUpdateReact.run(JSON.stringify(reactions), id);

            const isAdded = !!(reactions[emoji] && reactions[emoji].includes(me.name));
            const payload = { type:'react', id, emoji, from:me.name, action:isAdded?'add':'remove' };
            send(ws, payload);
            if (to) { for (const [cws,c] of clients) { if (c.name===to) { send(cws,payload); break; } } }
            else    { sendAll(payload, ws); }
            return;
        }

        // ── READ ──────────────────────────────────────────────────────────────
        if (m.type === 'read') { sendAll({ type:'status', id:m.id, status:'read', by:me.name }, ws); return; }

        // ── VIDEO CALL SIGNALING ──────────────────────────────────────────────
        if (m.type === 'call-request') {
            const target = String(m.to||'').trim();
            for (const [cws,c] of clients) {
                if (c.name===target) {
                    c.inCall ? send(ws,{type:'call-busy',from:target})
                             : send(cws,{type:'call-request',from:me.name,fromLang:me.lang});
                    break;
                }
            }
            return;
        }
        if (m.type === 'call-accept') {
            me.inCall = true;
            for (const [cws,c] of clients) {
                if (c.name===m.to) { c.inCall=true; send(cws,{type:'call-accept',from:me.name,fromLang:me.lang}); break; }
            }
            sendEvery({ type:'user-call-status', name:me.name, inCall:true });
            sendEvery({ type:'user-call-status', name:m.to,    inCall:true });
            return;
        }
        if (m.type === 'call-reject') {
            for (const [cws,c] of clients) { if (c.name===m.to) { send(cws,{type:'call-reject',from:me.name}); break; } }
            return;
        }
        if (m.type === 'call-end') {
            me.inCall = false;
            for (const [cws,c] of clients) {
                if (c.name===m.to) {
                    c.inCall=false; send(cws,{type:'call-end',from:me.name});
                    sendEvery({type:'user-call-status',name:c.name,inCall:false}); break;
                }
            }
            sendEvery({ type:'user-call-status', name:me.name, inCall:false });
            return;
        }
        if (m.type === 'webrtc-offer')  { for (const [cws,c] of clients) { if (c.name===m.to) { send(cws,{type:'webrtc-offer', from:me.name,offer:m.offer});     break; } } return; }
        if (m.type === 'webrtc-answer') { for (const [cws,c] of clients) { if (c.name===m.to) { send(cws,{type:'webrtc-answer',from:me.name,answer:m.answer});   break; } } return; }
        if (m.type === 'webrtc-ice')    { for (const [cws,c] of clients) { if (c.name===m.to) { send(cws,{type:'webrtc-ice',  from:me.name,candidate:m.candidate});break; } } return; }

        if (m.type === 'call-translate') {
            const text = String(m.text||'').trim(); if (!text) return;
            const targetName = String(m.to||'').trim();
            for (const [cws,c] of clients) {
                if (c.name===targetName) {
                    const r = await translate(text, me.lang, c.lang);
                    send(cws,{type:'call-subtitle',from:me.name,original:text,translated:r.text,fromLang:me.lang}); break;
                }
            }
            return;
        }
        if (m.type === 'call-subtitle-sync') {
            const targetName = String(m.to||'').trim();
            for (const [cws,c] of clients) { if (c.name===targetName) { send(cws,{type:'call-subtitle-sync',from:me.name,enabled:!!m.enabled}); break; } }
            return;
        }

        // ── PROFILE UPDATE ────────────────────────────────────────────────────
        if (m.type === 'profile-update') {
            const u = allUsers.get(me.name);
            if (u) {
                if (m.lang && LANGS[m.lang]) { u.lang=m.lang; me.lang=m.lang; }
                u.phone       = String(m.phone||'').slice(0, 20);
                u.email       = String(m.email||'').slice(0, 100);
                u.avatarColor = String(m.avatarColor||'').slice(0, 20);
                u.avatarImage = String(m.avatarImage||'').slice(0, 80000);
                u.lastSeen    = Date.now();
                saveUser(me.name);
            }
            sendEvery({ type:'users', users:userList() });
            return;
        }

        // ── ADMIN ─────────────────────────────────────────────────────────────
        if (m.type === 'admin-login') {
            const ok = m.pass === cfg.adminPass;
            const dc = ok ? loadDeeplCfg() : null;
            send(ws, { type:'admin-auth', ok, cfg: ok ? {
                adminPass: cfg.adminPass,
                deeplCookieSavedAt: dc?.saved_at || null
            } : null });
            return;
        }
        if (m.type === 'admin-save') {
            if (m.pass !== cfg.adminPass) return send(ws,{type:'admin-result',ok:false});
            if (m.cfg.adminPass) cfg.adminPass = m.cfg.adminPass;
            saveCfg();
            send(ws, { type:'admin-result', ok:true });
            return;
        }
        if (m.type === 'admin-set-cookie') {
            if (m.pass !== cfg.adminPass) return send(ws,{type:'error',text:'Unauthorized'});
            const cookie = String(m.cookie||'').trim();
            if (!cookie) return send(ws,{type:'error',text:'Cookie rỗng'});
            saveDeeplCfg(cookie);
            send(ws, { type:'admin-cookie-updated', ok:true });
            return;
        }
        if (m.type === 'admin-refresh-cookie') {
            if (m.pass !== cfg.adminPass) return send(ws,{type:'error',text:'Unauthorized'});
            refreshDeeplCookie().then(ok => send(ws, { type:'admin-cookie-updated', ok }));
            return;
        }
    });

    ws.on('close', () => {
        const c = clients.get(ws);
        if (c) {
            const wasInCall = c.inCall;
            const callName  = c.name;
            c.inCall = false;
            clients.delete(ws);
            const u = allUsers.get(c.name);
            if (u) {
                u.online   = false;
                u.lastSeen = Date.now();
                stmtSetLastSeen.run(u.lastSeen, c.name);
            }
            sendEvery({ type:'system', text:c.name+' left', ts:Date.now() });
            sendEvery({ type:'users', users:userList() });
            if (wasInCall) {
                sendEvery({ type:'user-call-status', name:callName, inCall:false });
                for (const [cws, cc] of clients) {
                    if (cc.inCall) {
                        cc.inCall = false;
                        send(cws, { type:'call-offline', from:callName });
                        sendEvery({ type:'user-call-status', name:cc.name, inCall:false });
                        break;
                    }
                }
            }
        }
    });
});

srv.listen(PORT, () => {
    console.log(`TransChat  →  http://localhost:${PORT}`);
    console.log(`Database   →  ${path.join(DATA, 'chat.db')}`);
});
