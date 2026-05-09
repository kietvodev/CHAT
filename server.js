const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = 3000;
const DATA = path.join(__dirname, 'data');
const UPLOADS = path.join(__dirname, 'uploads');
const CONF = path.join(DATA, 'config.json');
const HIST = path.join(DATA, 'history.json');

if (!fs.existsSync(DATA)) fs.mkdirSync(DATA, { recursive: true });
if (!fs.existsSync(UPLOADS)) fs.mkdirSync(UPLOADS, { recursive: true });

// ─── CONFIG ──────────────────────────────────────────────────────────────────
const DEF_CONFIG = {
    adminPass: 'admin123',
    apiKey: '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',
    apiUrl: 'https://api.groq.com/openai/v1/chat/completions',
    models: ['llama-3.3-70b-versatile', 'llama-3.1-8b-instant', 'openai/gpt-oss-120b', 'openai/gpt-oss-20b']
};
let cfg;
try { cfg = JSON.parse(fs.readFileSync(CONF, 'utf8')); }
catch { cfg = { ...DEF_CONFIG }; fs.writeFileSync(CONF, JSON.stringify(cfg, null, 2)); }
function saveCfg() { fs.writeFileSync(CONF, JSON.stringify(cfg, null, 2)); }

// ─── HISTORY ─────────────────────────────────────────────────────────────────
let hist;
try { hist = JSON.parse(fs.readFileSync(HIST, 'utf8')); } catch { hist = []; }
function saveHist() { hist = hist.slice(-500); fs.writeFileSync(HIST, JSON.stringify(hist)); }

// ─── CLIENTS ─────────────────────────────────────────────────────────────────
const clients = new Map();   // ws -> {id, name, lang}
const allUsers = new Map();  // name -> {lang, online, lastSeen}

const LANGS = {
    vi: 'Vietnamese', ja: 'Japanese', en: 'English', zh: 'Chinese',
    ko: 'Korean', fr: 'French', de: 'German', es: 'Spanish',
    th: 'Thai', ar: 'Arabic', ru: 'Russian', pt: 'Portuguese',
    it: 'Italian', hi: 'Hindi', id: 'Indonesian'
};

function uid() { return crypto.randomBytes(8).toString('hex'); }
function send(ws, o) { if (ws.readyState === 1) ws.send(JSON.stringify(o)); }
function sendAll(o, ex) { for (const [w] of clients) if (w !== ex) send(w, o); }
function sendEvery(o) { for (const [w] of clients) send(w, o); }

function userList() {
    const list = [];
    for (const [n, i] of allUsers) list.push({ name: n, lang: i.lang, online: i.online, lastSeen: i.lastSeen });
    return list;
}

// ─── TRANSLATION (auto-fallback) ─────────────────────────────────────────────
async function translate(text, from, to, idx = 0) {
    if (from === to) return { text, model: null };
    const models = cfg.models;
    if (idx >= models.length) return { text, model: null, error: 'All models failed' };
    const model = models[idx];
    const fromN = LANGS[from] || from, toN = LANGS[to] || to;
    try {
        const r = await fetch(cfg.apiUrl, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + cfg.apiKey },
            body: JSON.stringify({
                model,
                messages: [{ role: 'user', content: `Translate from ${fromN} to ${toN}. Output ONLY the translated text, nothing else.\n\n${text}` }],
                temperature: 0.1, max_tokens: 1024
            })
        });
        if (!r.ok) { console.log(`[${model}] HTTP ${r.status}, fallback...`); return translate(text, from, to, idx + 1); }
        const d = await r.json();
        const t = d.choices?.[0]?.message?.content?.trim();
        if (!t) throw new Error('Empty response');
        return { text: t, model };
    } catch (e) {
        console.log(`[${model}] ${e.message}, fallback...`);
        return translate(text, from, to, idx + 1);
    }
}

// ─── HTTP SERVER ─────────────────────────────────────────────────────────────
const MIME = { '.html': 'text/html', '.css': 'text/css', '.js': 'application/javascript', '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.gif': 'image/gif', '.webp': 'image/webp', '.svg': 'image/svg+xml' };
const ALLOWED_IMG = new Set(['.png', '.jpg', '.jpeg', '.gif', '.webp']);
const MAX_UPLOAD = 10 * 1024 * 1024; // 10MB

const srv = http.createServer((req, res) => {
    // CORS headers for uploads
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
    if (req.method === 'OPTIONS') { res.writeHead(200); res.end(); return; }

    // Image upload endpoint
    if (req.method === 'POST' && req.url === '/upload') {
        let size = 0;
        const chunks = [];
        req.on('data', chunk => {
            size += chunk.length;
            if (size > MAX_UPLOAD) { req.destroy(); res.writeHead(413); res.end(JSON.stringify({ error: 'File too large (max 10MB)' })); return; }
            chunks.push(chunk);
        });
        req.on('end', () => {
            const body = Buffer.concat(chunks);
            // Parse multipart boundary
            const ct = req.headers['content-type'] || '';
            const bMatch = ct.match(/boundary=(.+)/);
            if (!bMatch) { res.writeHead(400); res.end(JSON.stringify({ error: 'Invalid upload' })); return; }
            const boundary = bMatch[1];
            const parts = parseMultipart(body, boundary);
            const filePart = parts.find(p => p.filename);
            if (!filePart) { res.writeHead(400); res.end(JSON.stringify({ error: 'No file' })); return; }
            const ext = path.extname(filePart.filename).toLowerCase();
            if (!ALLOWED_IMG.has(ext)) { res.writeHead(400); res.end(JSON.stringify({ error: 'Only images allowed' })); return; }
            const fname = uid() + ext;
            fs.writeFileSync(path.join(UPLOADS, fname), filePart.data);
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ url: '/uploads/' + fname, filename: filePart.filename }));
        });
        return;
    }

    // Serve uploaded files
    if (req.url.startsWith('/uploads/')) {
        const fname = path.basename(req.url);
        const fp = path.join(UPLOADS, fname);
        const ext = path.extname(fname).toLowerCase();
        if (!ALLOWED_IMG.has(ext)) { res.writeHead(403); res.end(); return; }
        fs.readFile(fp, (e, d) => {
            if (e) { res.writeHead(404); res.end('Not found'); return; }
            res.writeHead(200, { 'Content-Type': MIME[ext] || 'application/octet-stream', 'Cache-Control': 'public, max-age=86400' });
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
        res.writeHead(200, { 'Content-Type': (MIME[ext] || 'text/plain') + '; charset=utf-8' });
        res.end(d);
    });
});

// Simple multipart parser
function parseMultipart(buf, boundary) {
    const parts = [];
    const sep = Buffer.from('--' + boundary);
    let start = 0;
    while (true) {
        const idx = buf.indexOf(sep, start);
        if (idx === -1) break;
        if (start > 0) {
            const chunk = buf.slice(start, idx - 2); // -2 for \r\n before boundary
            const headerEnd = chunk.indexOf('\r\n\r\n');
            if (headerEnd !== -1) {
                const header = chunk.slice(0, headerEnd).toString();
                const data = chunk.slice(headerEnd + 4);
                const fnMatch = header.match(/filename="([^"]+)"/);
                parts.push({ header, data, filename: fnMatch ? fnMatch[1] : null });
            }
        }
        start = idx + sep.length + 2; // +2 for \r\n
    }
    return parts;
}

// ─── WEBSOCKET ───────────────────────────────────────────────────────────────
const wss = new WebSocketServer({ server: srv });

wss.on('connection', ws => {
    ws.on('message', async raw => {
        let m; try { m = JSON.parse(raw); } catch { return; }

        // JOIN
        if (m.type === 'join') {
            const name = String(m.name || '').trim().slice(0, 30);
            if (!name) return send(ws, { type: 'error', text: 'Name required' });
            // Kick old session with same name (refresh/reconnect)
            for (const [oldWs, c] of clients) {
                if (c.name === name && oldWs !== ws) {
                    send(oldWs, { type: 'error', text: 'Logged in from another tab' });
                    oldWs.terminate();
                    clients.delete(oldWs);
                }
            }
            const lang = LANGS[m.lang] ? m.lang : 'en';
            clients.set(ws, { id: uid(), name, lang });
            allUsers.set(name, { lang, online: true, lastSeen: Date.now() });
            send(ws, { type: 'joined', name, lang });
            // Send history - only private msgs involving this user
            const myHist = hist.filter(h => h.to === name || h.from === name);
            send(ws, { type: 'history', messages: myHist.slice(-200).map(h => {
                const isOwn = h.from === name;
                if (h.msgType === 'image') {
                    // Recipient sees translated caption, sender sees original
                    const cap = !isOwn && h.translatedCaption ? h.translatedCaption : h.caption;
                    const origCap = !isOwn && h.translatedCaption && h.toLang !== h.fromLang ? h.caption : null;
                    return { ...h, type: 'image', caption: cap, originalCaption: origCap, isOwn, status: 'delivered', isHistory: true };
                }
                // Recipient sees translated text, sender sees original
                const displayText = !isOwn && h.translatedText ? h.translatedText : h.text;
                const displayOriginal = !isOwn && h.translatedText && h.toLang !== h.fromLang ? h.text : null;
                return { ...h, type: 'chat', text: displayText, original: displayOriginal, isOwn, status: 'delivered', isHistory: true };
            }) });
            sendEvery({ type: 'users', users: userList() });
            return;
        }

        const me = clients.get(ws);
        if (!me) return;

        // CHAT
        if (m.type === 'chat') {
            const text = String(m.text || '').trim().slice(0, 4000);
            if (!text) return;
            const id = uid(), ts = Date.now();
            const to = m.to ? String(m.to).trim() : null; // null = group, string = private

            // Immediately confirm "sent" to sender
            send(ws, { type: 'chat', id, from: me.name, fromLang: me.lang, text, original: null, isOwn: true, status: 'sent', ts, model: null, to });

            if (to) {
                // ── PRIVATE MESSAGE ──────────────────────────────────────────
                // Find recipient lang: online first, then fall back to allUsers (offline)
                let recipientLang = null, recipientWs = null;
                for (const [cws, c] of clients) {
                    if (c.name === to) { recipientLang = c.lang; recipientWs = cws; break; }
                }
                if (!recipientLang) {
                    const u = allUsers.get(to);
                    recipientLang = u ? u.lang : 'en';
                }

                // Always translate regardless of online/offline
                let translatedText = text, model = null;
                if (recipientLang !== me.lang) {
                    const r = await translate(text, me.lang, recipientLang);
                    translatedText = r.text;
                    model = r.model;
                }

                // Deliver to recipient if online
                if (recipientWs) {
                    send(recipientWs, { type: 'chat', id, from: me.name, fromLang: me.lang,
                        text: translatedText,
                        original: recipientLang !== me.lang ? text : null,
                        isOwn: false, status: 'delivered', ts, model, to });
                    send(ws, { type: 'status', id, status: 'delivered' });
                }

                // Save both original + translated to history
                hist.push({ id, from: me.name, fromLang: me.lang,
                    text,               // original (what sender typed)
                    translatedText,     // translated to recipient's lang
                    toLang: recipientLang,
                    ts, model, to });
            } else {
                // GROUP MESSAGE - send to everyone
                const targetLangs = new Set();
                for (const [, c] of clients) if (c.name !== me.name) targetLangs.add(c.lang);
                const cache = { [me.lang]: text };
                let usedModel = null;
                for (const l of targetLangs) {
                    if (!cache[l]) {
                        const r = await translate(text, me.lang, l);
                        cache[l] = r.text;
                        if (r.model) usedModel = r.model;
                    }
                }
                let delivered = false;
                for (const [cws, c] of clients) {
                    if (cws === ws) continue;
                    const tr = cache[c.lang] || text;
                    send(cws, { type: 'chat', id, from: me.name, fromLang: me.lang, text: tr, original: c.lang !== me.lang ? text : null, isOwn: false, status: 'delivered', ts, model: usedModel, to: null });
                    delivered = true;
                }
                if (delivered) send(ws, { type: 'status', id, status: 'delivered' });
                hist.push({ id, from: me.name, fromLang: me.lang, text, ts, model: usedModel, to: null });
            }
            saveHist();
            return;
        }

        // IMAGE
        if (m.type === 'image') {
            const url = String(m.url || '').trim();
            const caption = String(m.caption || '').trim().slice(0, 500);
            if (!url || !url.startsWith('/uploads/')) return;
            const id = uid(), ts = Date.now();
            const to = m.to ? String(m.to).trim() : null;

            send(ws, { type: 'image', id, from: me.name, fromLang: me.lang, url, caption, isOwn: true, status: 'sent', ts, to });

            if (to) {
                // ── PRIVATE IMAGE ────────────────────────────────────────────
                let recipientLang = null, recipientWs = null;
                for (const [cws, c] of clients) {
                    if (c.name === to) { recipientLang = c.lang; recipientWs = cws; break; }
                }
                if (!recipientLang) {
                    const u = allUsers.get(to);
                    recipientLang = u ? u.lang : 'en';
                }

                // Always translate caption
                let translatedCaption = caption, capModel = null;
                if (caption && recipientLang !== me.lang) {
                    const r = await translate(caption, me.lang, recipientLang);
                    translatedCaption = r.text;
                    capModel = r.model;
                }

                if (recipientWs) {
                    send(recipientWs, { type: 'image', id, from: me.name, fromLang: me.lang, url,
                        caption: translatedCaption,
                        originalCaption: recipientLang !== me.lang ? caption : null,
                        isOwn: false, status: 'delivered', ts, to });
                    send(ws, { type: 'status', id, status: 'delivered' });
                }

                hist.push({ id, from: me.name, fromLang: me.lang, url,
                    caption,                  // original
                    translatedCaption,        // translated to recipient lang
                    toLang: recipientLang,
                    ts, msgType: 'image', model: capModel, to });
            } else {
                // GROUP IMAGE
                let delivered = false;
                for (const [cws, c] of clients) {
                    if (cws === ws) continue;
                    let trCaption = caption;
                    if (caption && c.lang !== me.lang) {
                        const r = await translate(caption, me.lang, c.lang);
                        trCaption = r.text;
                    }
                    send(cws, { type: 'image', id, from: me.name, fromLang: me.lang, url, caption: trCaption, originalCaption: c.lang !== me.lang ? caption : null, isOwn: false, status: 'delivered', ts, to: null });
                    delivered = true;
                }
                if (delivered) send(ws, { type: 'status', id, status: 'delivered' });
                hist.push({ id, from: me.name, fromLang: me.lang, url, caption, ts, msgType: 'image', to: null });
            }
            saveHist();
            return;
        }

        // TYPING
        if (m.type === 'typing') {
            if (m.to) {
                // Private typing - only send to target
                for (const [cws, c] of clients) {
                    if (c.name === m.to) { send(cws, { type: 'typing', from: me.name, active: !!m.active, to: me.name }); break; }
                }
            } else {
                sendAll({ type: 'typing', from: me.name, active: !!m.active, to: null }, ws);
            }
            return;
        }

        // DELETE
        if (m.type === 'delete') {
            const id = String(m.id || '');
            const entry = hist.find(h => h.id === id);
            if (!entry || entry.from !== me.name) return; // only own messages
            hist = hist.filter(h => h.id !== id);
            saveHist();
            sendEvery({ type: 'deleted', id });
            return;
        }

        // REACT
        if (m.type === 'react') {
            const id = String(m.id || '');
            const emoji = String(m.emoji || '').slice(0, 8);
            const to = m.to ? String(m.to).trim() : null;
            const action = m.action === 'remove' ? 'remove' : 'add';
            if (!id || !emoji) return;

            // Update reaction in history entry
            const entry = hist.find(h => h.id === id);
            if (entry) {
                if (!entry.reactions) entry.reactions = {};
                if (!entry.reactions[emoji]) entry.reactions[emoji] = [];
                const arr = entry.reactions[emoji];
                const idx = arr.indexOf(me.name);
                if (action === 'remove') {
                    if (idx !== -1) arr.splice(idx, 1);
                } else {
                    // Toggle: if already reacted, remove; else add
                    if (idx !== -1) { arr.splice(idx, 1); }
                    else { arr.push(me.name); }
                }
                if (arr.length === 0) delete entry.reactions[emoji];
                saveHist();
            }

            // Broadcast to both users in the conversation
            const payload = { type: 'react', id, emoji, from: me.name,
                action: entry && entry.reactions && (entry.reactions[emoji] || []).indexOf(me.name) !== -1 ? 'add' : 'remove' };
            send(ws, payload); // confirm to sender
            if (to) {
                for (const [cws, c] of clients) {
                    if (c.name === to) { send(cws, payload); break; }
                }
            } else {
                sendAll(payload, ws);
            }
            return;
        }

        // DELETE ALL MESSAGES FROM A USER
        if (m.type === 'delete-user-msgs') {
            const target = String(m.user || '').trim();
            if (!target) return;
            if (target === me.name) {
                // Delete own messages from server
                const ids = hist.filter(h => h.from === target).map(h => h.id);
                hist = hist.filter(h => h.from !== target);
                saveHist();
                ids.forEach(id => sendEvery({ type: 'deleted', id }));
            } else {
                // Just remove from requester's view (client-side only)
                send(ws, { type: 'deleted-user', user: target });
            }
            return;
        }

        // READ
        if (m.type === 'read') { sendAll({ type: 'status', id: m.id, status: 'read', by: me.name }, ws); return; }

        // ─── VIDEO CALL SIGNALING ────────────────────────────────────────────
        if (m.type === 'call-request') {
            const target = String(m.to || '').trim();
            for (const [cws, c] of clients) {
                if (c.name === target) {
                    if (c.inCall) {
                        // Target is busy — reject immediately, notify caller
                        send(ws, { type: 'call-busy', from: target });
                    } else {
                        send(cws, { type: 'call-request', from: me.name, fromLang: me.lang });
                    }
                    break;
                }
            }
            return;
        }
        if (m.type === 'call-accept') {
            me.inCall = true;
            for (const [cws, c] of clients) {
                if (c.name === m.to) {
                    c.inCall = true;
                    send(cws, { type: 'call-accept', from: me.name, fromLang: me.lang });
                    break;
                }
            }
            // Broadcast in-call status for both users
            sendEvery({ type: 'user-call-status', name: me.name, inCall: true });
            sendEvery({ type: 'user-call-status', name: m.to, inCall: true });
            return;
        }
        if (m.type === 'call-reject') {
            for (const [cws, c] of clients) {
                if (c.name === m.to) { send(cws, { type: 'call-reject', from: me.name }); break; }
            }
            return;
        }
        if (m.type === 'call-end') {
            me.inCall = false;
            for (const [cws, c] of clients) {
                if (c.name === m.to) {
                    c.inCall = false;
                    send(cws, { type: 'call-end', from: me.name });
                    sendEvery({ type: 'user-call-status', name: c.name, inCall: false });
                    break;
                }
            }
            sendEvery({ type: 'user-call-status', name: me.name, inCall: false });
            return;
        }
        if (m.type === 'webrtc-offer') {
            for (const [cws, c] of clients) {
                if (c.name === m.to) { send(cws, { type: 'webrtc-offer', from: me.name, offer: m.offer }); break; }
            }
            return;
        }
        if (m.type === 'webrtc-answer') {
            for (const [cws, c] of clients) {
                if (c.name === m.to) { send(cws, { type: 'webrtc-answer', from: me.name, answer: m.answer }); break; }
            }
            return;
        }
        if (m.type === 'webrtc-ice') {
            for (const [cws, c] of clients) {
                if (c.name === m.to) { send(cws, { type: 'webrtc-ice', from: me.name, candidate: m.candidate }); break; }
            }
            return;
        }
        // Translate speech text from call
        if (m.type === 'call-translate') {
            const text = String(m.text || '').trim();
            if (!text) return;
            const targetName = String(m.to || '').trim();
            for (const [cws, c] of clients) {
                if (c.name === targetName) {
                    const r = await translate(text, me.lang, c.lang);
                    send(cws, { type: 'call-subtitle', from: me.name, original: text, translated: r.text, fromLang: me.lang });
                    break;
                }
            }
            return;
        }
        // Sync subtitle on/off state with call peer
        if (m.type === 'call-subtitle-sync') {
            const targetName = String(m.to || '').trim();
            for (const [cws, c] of clients) {
                if (c.name === targetName) {
                    send(cws, { type: 'call-subtitle-sync', from: me.name, enabled: !!m.enabled });
                    break;
                }
            }
            return;
        }

        // ADMIN LOGIN
        if (m.type === 'admin-login') {
            const ok = m.pass === cfg.adminPass;
            send(ws, { type: 'admin-auth', ok, cfg: ok ? { apiKey: cfg.apiKey, models: cfg.models, apiUrl: cfg.apiUrl, adminPass: cfg.adminPass } : null });
            return;
        }

        // ADMIN SAVE
        if (m.type === 'admin-save') {
            if (m.pass !== cfg.adminPass) return send(ws, { type: 'admin-result', ok: false });
            if (m.cfg.apiKey) cfg.apiKey = m.cfg.apiKey;
            if (m.cfg.models && m.cfg.models.length) cfg.models = m.cfg.models;
            if (m.cfg.apiUrl) cfg.apiUrl = m.cfg.apiUrl;
            if (m.cfg.adminPass) cfg.adminPass = m.cfg.adminPass;
            saveCfg();
            send(ws, { type: 'admin-result', ok: true });
            return;
        }
    });

    ws.on('close', () => {
        const c = clients.get(ws);
        if (c) {
            const wasInCall = c.inCall;
            c.inCall = false;
            clients.delete(ws);
            const u = allUsers.get(c.name);
            if (u) { u.online = false; u.lastSeen = Date.now(); }
            sendEvery({ type: 'system', text: c.name + ' left', ts: Date.now() });
            sendEvery({ type: 'users', users: userList() });
            if (wasInCall) sendEvery({ type: 'user-call-status', name: c.name, inCall: false });
        }
    });
});

srv.listen(PORT, () => console.log('Server: http://localhost:' + PORT));
