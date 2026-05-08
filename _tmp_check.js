

(function() {
var WS_URL = (location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host;

var LANG_CODES = { vi:'VN', en:'EN', ja:'JP', ko:'KR', zh:'CN', fr:'FR', de:'DE', es:'ES', th:'TH', ar:'AR', ru:'RU', pt:'PT', it:'IT', hi:'HI', id:'ID' };

// DOM
var $ = function(id) { return document.getElementById(id); };
var loginScreen = $('login-screen'), chatScreen = $('chat-screen');
var inpName = $('inp-name'), inpLang = $('inp-lang'), btnJoin = $('btn-join'), loginError = $('login-error');
var statusBar = $('status-bar'), myBadge = $('my-badge');
var btnMenu = $('btn-menu'), sidebar = $('sidebar'), sidebarOverlay = $('sidebar-overlay');
var onlineLabel = $('online-label'), offlineLabel = $('offline-label');
var onlineList = $('online-list'), offlineList = $('offline-list');
var messagesWrap = $('messages-wrap'), newMsgBadge = $('new-msg-badge');
var typingBar = $('typing-bar'), typingText = $('typing-text');
var msgInput = $('msg-input'), btnSend = $('btn-send');
var btnAttach = $('btn-attach'), fileInput = $('file-input');
var imgPreviewBar = $('img-preview-bar'), imgPreviewThumb = $('img-preview-thumb'), imgPreviewName = $('img-preview-name'), imgPreviewRemove = $('img-preview-remove');
var lightbox = $('lightbox'), lightboxImg = $('lightbox-img');
var btnLogout = $('btn-logout');
var adminOverlay = $('admin-overlay'), adminLoginForm = $('admin-login-form'), adminConfig = $('admin-config');
var admPass = $('adm-pass'), admKey = $('adm-key'), admModels = $('adm-models');
var admUrl = $('adm-url'), admNewpass = $('adm-newpass'), adminMsg = $('admin-msg');

var ws, myName, myLang, adminLoggedPass = null;
var typingUsers = {};
var typingTimeout = null;
var lastTypingSent = 0;
var msgStatuses = {}; // id -> status element
var pendingFile = null;

// ═══ AUTO LOGIN ═══
var saved = null;
try { saved = JSON.parse(localStorage.getItem('transchat_user')); } catch {}
if (saved && saved.name) {
    myName = saved.name;
    myLang = saved.lang || 'en';
    inpName.value = myName;
    inpLang.value = myLang;
    connect();
}

// ═══ JOIN ═══
btnJoin.onclick = doJoin;
inpName.onkeydown = function(e) { if (e.key === 'Enter') doJoin(); };

function doJoin() {
    var name = inpName.value.trim();
    if (!name) { showLoginErr('Please enter your name'); return; }
    myName = name;
    myLang = inpLang.value;
    localStorage.setItem('transchat_user', JSON.stringify({ name: myName, lang: myLang }));
    connect();
}

function showLoginErr(t) { loginError.textContent = t; loginError.style.display = 'block'; }

// ═══ WEBSOCKET ═══
function connect() {
    ws = new WebSocket(WS_URL);
    ws.onopen = function() {
        statusBar.style.display = 'none';
        ws.send(JSON.stringify({ type: 'join', name: myName, lang: myLang }));
    };
    ws.onmessage = function(e) {
        var m; try { m = JSON.parse(e.data); } catch { return; }
        handle(m);
    };
    ws.onclose = function() {
        if (chatScreen.style.display === 'flex') {
            statusBar.style.display = 'block';
            setTimeout(connect, 3000);
        } else {
            showLoginErr('Cannot connect to server');
        }
    };
    ws.onerror = function() { ws.close(); };
}

function wsSend(o) { if (ws && ws.readyState === 1) ws.send(JSON.stringify(o)); }

// ═══ MESSAGE HANDLER ═══
function handle(m) {
    if (m.type === 'error') { showLoginErr(m.text); return; }
    if (m.type === 'joined') {
        loginScreen.style.display = 'none';
        chatScreen.style.display = 'flex';
        myBadge.textContent = myName + ' (' + (LANG_CODES[myLang] || myLang) + ')';
        return;
    }
    if (m.type === 'history') { renderHistory(m.messages); return; }
    if (m.type === 'users') { renderUsers(m.users); return; }
    if (m.type === 'system') { appendSystem(m.text, m.ts); return; }
    if (m.type === 'chat') { appendChat(m); return; }
    if (m.type === 'image') { appendImage(m); return; }
    if (m.type === 'status') { updateStatus(m); return; }
    if (m.type === 'deleted') { removeMsg(m.id); return; }
    if (m.type === 'deleted-user') { removeMsgsByUser(m.user); return; }
    if (m.type === 'typing') { handleTyping(m); return; }
    if (m.type === 'admin-auth') { handleAdminAuth(m); return; }
    if (m.type === 'admin-result') { handleAdminResult(m); return; }
}

// ═══ RENDER USERS ═══
function renderUsers(users) {
    var on = users.filter(function(u) { return u.online; });
    var off = users.filter(function(u) { return !u.online; });
    onlineLabel.textContent = 'Online (' + on.length + ')';
    offlineLabel.textContent = 'Offline (' + off.length + ')';
    onlineList.innerHTML = '';
    offlineList.innerHTML = '';
    on.forEach(function(u) { onlineList.appendChild(makeUserItem(u, true)); });
    off.forEach(function(u) { offlineList.appendChild(makeUserItem(u, false)); });
}

function makeUserItem(u, online) {
    var div = document.createElement('div');
    div.className = 'user-item';
    var extra = '';
    if (!online && u.lastSeen) extra = '<span class="user-seen">' + timeAgo(u.lastSeen) + '</span>';
    var clearBtn = '<button class="user-clear" title="Delete messages from ' + esc(u.name) + '" onclick="clearUserMsgs(\'' + esc(u.name).replace(/'/g, "\\'") + '\')">&times;</button>';
    div.innerHTML =
        '<div class="user-dot ' + (online ? 'on' : 'off') + '"></div>' +
        '<span class="user-name">' + esc(u.name) + (u.name === myName ? ' (you)' : '') + '</span>' +
        '<span class="user-lang">' + (LANG_CODES[u.lang] || u.lang) + '</span>' + extra + clearBtn;
    return div;
}

// ═══ MESSAGES ═══
function renderHistory(msgs) {
    msgs.forEach(function(m) {
        m.isHistory = true;
        if (m.type === 'image') {
            appendImage(m);
        } else {
            appendChat({
                id: m.id, from: m.from, fromLang: m.fromLang,
                text: m.text, original: null, isOwn: m.from === myName,
                status: 'delivered', ts: m.ts, model: m.model,
                isHistory: true
            });
        }
    });
}

var lastMsgFrom = null, lastMsgTs = 0;

function appendChat(msg) {
    var grouped = (msg.from === lastMsgFrom && msg.ts - lastMsgTs < 120000);
    lastMsgFrom = msg.from;
    lastMsgTs = msg.ts;

    var div = document.createElement('div');
    div.className = 'msg' + (msg.isOwn ? ' own' : '') + (grouped ? ' grouped' : '');
    div.dataset.from = msg.from;
    div.dataset.ts = msg.ts;
    div.dataset.id = msg.id;
    if (msg.isHistory) div.style.animation = 'none';

    var initials = msg.from.charAt(0).toUpperCase();
    var nameHtml = '<div class="msg-name">' + esc(msg.from) + ' <span style="color:var(--text-3);font-weight:400">' + (LANG_CODES[msg.fromLang] || '') + '</span></div>';

    var statusHtml = '';
    if (msg.isOwn) {
        var cls = msg.status === 'read' ? ' read' : '';
        var checks = msg.status === 'sent' ? '&#10003;' : '&#10003;&#10003;';
        statusHtml = '<span class="msg-status' + cls + '" data-status-id="' + msg.id + '">' + checks + '</span>';
    }

    var modelHtml = '';
    if (msg.model && !msg.isOwn) {
        var short = msg.model.split('/').pop().split('-').slice(0, 2).join('-');
        modelHtml = '<span class="msg-model">' + esc(short) + '</span>';
    }

    var originalHtml = '';
    if (msg.original) {
        originalHtml = '<div class="original-text">' + esc(msg.original) + '</div>';
    }

    div.innerHTML =
        '<div class="avatar">' + initials + '</div>' +
        '<div class="msg-body">' +
            nameHtml +
            '<div class="bubble">' + esc(msg.text) + '</div>' +
            originalHtml +
            '<div class="msg-footer">' +
                '<span class="msg-time">' + formatTime(msg.ts) + '</span>' +
                statusHtml + modelHtml +
                (msg.isOwn ? '<button class="msg-delete" onclick="deleteMsg(\'' + msg.id + '\')" title="Delete">&times;</button>' : '') +
            '</div>' +
        '</div>';

    var atBottom = isNearBottom();
    messagesWrap.appendChild(div);

    if (atBottom || msg.isOwn) {
        scrollDown();
    } else if (!msg.isHistory) {
        newMsgBadge.style.display = 'block';
    }

    if (!msg.isOwn && !msg.isHistory && document.hasFocus()) {
        wsSend({ type: 'read', id: msg.id });
    }
    if (!msg.isOwn && !msg.isHistory) showNotification(msg.from, msg.text);

function appendSystem(text, ts) {
    var div = document.createElement('div');
    div.className = 'system-msg';
    div.innerHTML = '<div class="system-bubble">' + esc(text) + (ts ? ' <span style="opacity:0.6">' + formatTime(ts) + '</span>' : '') + '</div>';
    messagesWrap.appendChild(div);
    lastMsgFrom = null;
    if (isNearBottom()) scrollDown();
}

function updateStatus(m) {
    var el = document.querySelector('[data-status-id="' + m.id + '"]');
    if (!el) return;
    if (m.status === 'delivered') { el.innerHTML = '&#10003;&#10003;'; }
    if (m.status === 'read') { el.innerHTML = '&#10003;&#10003;'; el.classList.add('read'); }
}

// ═══ TYPING ═══
function handleTyping(m) {
    if (m.active) {
        typingUsers[m.from] = Date.now();
    } else {
        delete typingUsers[m.from];
    }
    showTyping();
}

function showTyping() {
    var names = Object.keys(typingUsers);
    // Remove stale (>4s)
    var now = Date.now();
    names = names.filter(function(n) { return now - typingUsers[n] < 4000; });
    if (names.length === 0) {
        typingBar.style.display = 'none';
    } else {
        typingBar.style.display = 'flex';
        if (names.length === 1) typingText.textContent = names[0] + ' is typing...';
        else if (names.length === 2) typingText.textContent = names[0] + ' and ' + names[1] + ' are typing...';
        else typingText.textContent = names.length + ' people typing...';
    }
}

setInterval(showTyping, 2000);

// ═══ SENDING ═══
function doSend() {
    var text = msgInput.value.trim();
    if (pendingFile) {
        uploadAndSend(pendingFile, text);
        return;
    }
    if (!text) return;
    wsSend({ type: 'chat', text: text });
    msgInput.value = '';
    msgInput.style.height = 'auto';
    wsSend({ type: 'typing', active: false });
}

btnSend.onclick = doSend;
msgInput.onkeydown = function(e) {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); doSend(); }
};

// Auto-resize textarea
msgInput.oninput = function() {
    this.style.height = 'auto';
    this.style.height = Math.min(this.scrollHeight, 120) + 'px';

    // Typing indicator
    var now = Date.now();
    if (now - lastTypingSent > 2000) {
        wsSend({ type: 'typing', active: true });
        lastTypingSent = now;
    }
    clearTimeout(typingTimeout);
    typingTimeout = setTimeout(function() { wsSend({ type: 'typing', active: false }); }, 2500);
};

// ═══ SCROLL ═══
function isNearBottom() { return messagesWrap.scrollHeight - messagesWrap.scrollTop - messagesWrap.clientHeight < 100; }
function scrollDown() { messagesWrap.scrollTop = messagesWrap.scrollHeight; }

messagesWrap.onscroll = function() {
    if (isNearBottom()) newMsgBadge.style.display = 'none';
};
newMsgBadge.onclick = function() { scrollDown(); newMsgBadge.style.display = 'none'; };

// Read receipts on focus
document.addEventListener('visibilitychange', function() {
    if (!document.hidden) {
        var msgs = messagesWrap.querySelectorAll('.msg:not(.own)');
        if (msgs.length) {
            var last = msgs[msgs.length - 1];
            if (last.dataset.id) wsSend({ type: 'read', id: last.dataset.id });
        }
    }
});

// ═══ SIDEBAR (mobile) ═══
btnMenu.onclick = function() { sidebar.classList.add('open'); sidebarOverlay.classList.add('show'); };
sidebarOverlay.onclick = function() { sidebar.classList.remove('open'); sidebarOverlay.classList.remove('show'); };

// ═══ IMAGE MESSAGES ═══
function appendImage(msg) {
    var grouped = (msg.from === lastMsgFrom && msg.ts - lastMsgTs < 120000);
    lastMsgFrom = msg.from;
    lastMsgTs = msg.ts;

    var div = document.createElement('div');
    div.className = 'msg' + (msg.isOwn ? ' own' : '') + (grouped ? ' grouped' : '');
    div.dataset.id = msg.id;
    if (msg.isHistory) div.style.animation = 'none';

    var initials = msg.from.charAt(0).toUpperCase();
    var nameHtml = '<div class="msg-name">' + esc(msg.from) + ' <span style="color:var(--text-3);font-weight:400">' + (LANG_CODES[msg.fromLang] || '') + '</span></div>';

    var statusHtml = '';
    if (msg.isOwn) {
        var cls = msg.status === 'read' ? ' read' : '';
        var checks = msg.status === 'sent' ? '&#10003;' : '&#10003;&#10003;';
        statusHtml = '<span class="msg-status' + cls + '" data-status-id="' + msg.id + '">' + checks + '</span>';
    }

    var captionHtml = '';
    if (msg.caption) captionHtml = '<div class="msg-caption">' + esc(msg.caption) + '</div>';
    var origHtml = '';
    if (msg.originalCaption) origHtml = '<div class="original-text">' + esc(msg.originalCaption) + '</div>';

    div.innerHTML =
        '<div class="avatar">' + initials + '</div>' +
        '<div class="msg-body">' +
            nameHtml +
            '<div class="bubble" style="padding:6px"><img class="msg-img" src="' + esc(msg.url) + '" loading="lazy" onclick="openLightbox(this.src)">' + captionHtml + '</div>' +
            origHtml +
            '<div class="msg-footer">' +
                '<span class="msg-time">' + formatTime(msg.ts) + '</span>' +
                statusHtml +
                (msg.isOwn ? '<button class="msg-delete" onclick="deleteMsg(\'' + msg.id + '\')" title="Delete">&times;</button>' : '') +
            '</div>' +
        '</div>';

    var atBottom = isNearBottom();
    messagesWrap.appendChild(div);
    if (atBottom || msg.isOwn) scrollDown();
    else if (!msg.isHistory) newMsgBadge.style.display = 'block';
    if (!msg.isOwn && !msg.isHistory && document.hasFocus()) wsSend({ type: 'read', id: msg.id });
    if (!msg.isOwn && !msg.isHistory) showNotification(msg.from, msg.caption ? '[Photo] ' + msg.caption : '[Photo]');
}

function openLightbox(src) {
    lightboxImg.src = src;
    lightbox.style.display = 'flex';
}

// ═══ DELETE MESSAGE ═══
function deleteMsg(id) {
    if (!confirm('Delete this message?')) return;
    wsSend({ type: 'delete', id: id });
}
function removeMsg(id) {
    var el = document.querySelector('.msg[data-id="' + id + '"]');
    if (el) {
        el.style.transition = 'opacity 0.3s, transform 0.3s';
        el.style.opacity = '0';
        el.style.transform = 'scale(0.95)';
        setTimeout(function() { el.remove(); }, 300);
    }
}

function clearUserMsgs(userName) {
    var label = userName === myName ? 'Delete ALL your messages? (permanent)' : 'Hide all messages from ' + userName + '?';
    if (!confirm(label)) return;
    wsSend({ type: 'delete-user-msgs', user: userName });
    if (userName !== myName) {
        // Immediately hide from view for other users' messages
        removeMsgsByUser(userName);
    }
}

function removeMsgsByUser(userName) {
    var msgs = messagesWrap.querySelectorAll('.msg[data-from="' + userName + '"]');
    msgs.forEach(function(el) {
        el.style.transition = 'opacity 0.3s, transform 0.3s';
        el.style.opacity = '0';
        el.style.transform = 'scale(0.95)';
        setTimeout(function() { el.remove(); }, 300);
    });
}

// ═══ IMAGE UPLOAD ═══
btnAttach.onclick = function() { fileInput.click(); };
fileInput.onchange = function() {
    var file = this.files[0];
    if (!file) return;
    if (!file.type.startsWith('image/')) { alert('Only images allowed'); return; }
    if (file.size > 10 * 1024 * 1024) { alert('Max 10MB'); return; }
    pendingFile = file;
    imgPreviewThumb.src = URL.createObjectURL(file);
    imgPreviewName.textContent = file.name;
    imgPreviewBar.style.display = 'flex';
    fileInput.value = '';
    msgInput.focus();
};
imgPreviewRemove.onclick = function() {
    pendingFile = null;
    imgPreviewBar.style.display = 'none';
    imgPreviewThumb.src = '';
};

function uploadAndSend(file, caption) {
    var fd = new FormData();
    fd.append('file', file);
    btnSend.disabled = true;
    fetch('/upload', { method: 'POST', body: fd })
    .then(function(r) { return r.json(); })
    .then(function(data) {
        if (data.error) { alert(data.error); return; }
        wsSend({ type: 'image', url: data.url, caption: caption || '' });
        msgInput.value = '';
        msgInput.style.height = 'auto';
        pendingFile = null;
        imgPreviewBar.style.display = 'none';
        imgPreviewThumb.src = '';
    })
    .catch(function(e) { alert('Upload failed: ' + e.message); })
    .finally(function() { btnSend.disabled = false; });
}

// ═══ LOGOUT ═══
btnLogout.onclick = function() {
    localStorage.removeItem('transchat_user');
    if (ws) ws.close();
    chatScreen.style.display = 'none';
    loginScreen.style.display = 'flex';
    messagesWrap.innerHTML = '';
    inpName.value = '';
    inpName.focus();
};

// ═══ MOBILE: fix keyboard pushing layout ═══
if ('visualViewport' in window) {
    visualViewport.addEventListener('resize', function() {
        var offset = window.innerHeight - visualViewport.height;
        var inputArea = document.getElementById('input-area');
        var previewBar = document.getElementById('img-preview-bar');
        if (inputArea) inputArea.style.paddingBottom = (offset > 50 ? '8px' : '10px');
        // Scroll to bottom when keyboard opens
        if (offset > 50) setTimeout(scrollDown, 100);
    });
}

// ═══ PUSH NOTIFICATIONS ═══
var notifEnabled = false;
$('btn-notif').onclick = function() {
    if (!('Notification' in window)) { alert('Browser does not support notifications'); return; }
    if (Notification.permission === 'granted') {
        notifEnabled = !notifEnabled;
        $('btn-notif').style.color = notifEnabled ? 'var(--green)' : 'var(--text-3)';
        return;
    }
    Notification.requestPermission().then(function(p) {
        notifEnabled = p === 'granted';
        $('btn-notif').style.color = notifEnabled ? 'var(--green)' : 'var(--text-3)';
    });
};

function showNotification(from, text) {
    if (!notifEnabled || document.hasFocus()) return;
    try {
        var n = new Notification(from, { body: text.slice(0, 100), icon: '', tag: 'transchat', silent: false });
        n.onclick = function() { window.focus(); n.close(); };
        setTimeout(function() { n.close(); }, 5000);
    } catch (e) {}
}

// ═══ AI ASSISTANT ═══
var aiOverlay = $('ai-overlay');
var aiMessages = $('ai-messages');
var aiInput = $('ai-input');
var aiSend = $('ai-send');
var aiHistory = []; // conversation with AI

$('btn-ai').onclick = function() {
    aiOverlay.style.display = 'flex';
    aiInput.focus();
};
$('ai-close').onclick = function() { aiOverlay.style.display = 'none'; };
aiOverlay.addEventListener('click', function(e) { if (e.target === aiOverlay) aiOverlay.style.display = 'none'; });

aiSend.onclick = doAskAI;
aiInput.onkeydown = function(e) { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); doAskAI(); } };

function doAskAI() {
    var q = aiInput.value.trim();
    if (!q) return;
    aiInput.value = '';
    appendAIMsg('user', q);

    // Build context from chat history (last 30 messages)
    var msgs = messagesWrap.querySelectorAll('.msg:not(.system-msg)');
    var chatCtx = [];
    msgs.forEach(function(el) {
        var from = el.dataset.from || '?';
        var bubble = el.querySelector('.bubble');
        if (bubble) {
            var img = bubble.querySelector('img');
            chatCtx.push('[' + from + ']: ' + (img ? '[image]' : bubble.textContent.trim()));
        }
    });
    var contextText = chatCtx.slice(-30).join('\n');

    var systemPrompt = 'You are a helpful AI assistant reading a group chat conversation. Here is the recent chat history:\n\n' + contextText + '\n\nAnswer the user\'s question about this conversation. Be concise.';

    aiHistory.push({ role: 'user', content: q });

    var thinkingEl = appendAIMsg('assistant', '...');
    aiSend.disabled = true;

    // Read config from server via admin or just use stored key
    var apiKey = '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';
    var apiUrl = 'https://api.groq.com/openai/v1/chat/completions';
    var model  = 'llama-3.3-70b-versatile';

    fetch(apiUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + apiKey },
        body: JSON.stringify({
            model: model,
            messages: [{ role: 'system', content: systemPrompt }].concat(aiHistory),
            temperature: 0.7, max_tokens: 1024
        })
    })
    .then(function(r) { return r.json(); })
    .then(function(d) {
        var reply = d.choices?.[0]?.message?.content?.trim() || 'No response';
        thinkingEl.textContent = reply;
        aiHistory.push({ role: 'assistant', content: reply });
        aiMessages.scrollTop = aiMessages.scrollHeight;
    })
    .catch(function(e) { thinkingEl.textContent = 'Error: ' + e.message; })
    .finally(function() { aiSend.disabled = false; });
}

function appendAIMsg(role, text) {
    var div = document.createElement('div');
    div.style.cssText = 'padding:8px 12px;border-radius:10px;font-size:0.88em;line-height:1.5;white-space:pre-wrap;word-break:break-word;' +
        (role === 'user'
            ? 'background:linear-gradient(135deg,#4338ca,#6366f1);color:white;align-self:flex-end;max-width:80%'
            : 'background:var(--bg-2);color:var(--text);align-self:flex-start;max-width:90%');
    div.textContent = text;
    aiMessages.style.display = 'flex';
    aiMessages.style.flexDirection = 'column';
    aiMessages.appendChild(div);
    aiMessages.scrollTop = aiMessages.scrollHeight;
    return div;
}

// ═══ ADMIN ═══
$('btn-admin').onclick = function() { adminOverlay.style.display = 'flex'; adminLoginForm.style.display = 'block'; adminConfig.style.display = 'none'; adminMsg.textContent = ''; admPass.value = ''; };
$('admin-close-x').onclick = closeAdmin;
$('btn-adm-cancel1').onclick = closeAdmin;
$('btn-adm-close').onclick = closeAdmin;
adminOverlay.onclick = function(e) { if (e.target === adminOverlay) closeAdmin(); };

function closeAdmin() { adminOverlay.style.display = 'none'; adminLoggedPass = null; }

$('btn-adm-login').onclick = function() {
    var pass = admPass.value.trim();
    if (!pass) return;
    adminLoggedPass = pass;
    wsSend({ type: 'admin-login', pass: pass });
};
admPass.onkeydown = function(e) { if (e.key === 'Enter') $('btn-adm-login').click(); };

function handleAdminAuth(m) {
    if (m.ok && m.cfg) {
        adminLoginForm.style.display = 'none';
        adminConfig.style.display = 'block';
        admKey.value = m.cfg.apiKey || '';
        admModels.value = (m.cfg.models || []).join('\n');
        admUrl.value = m.cfg.apiUrl || '';
        admNewpass.value = '';
    } else {
        adminLoggedPass = null;
        adminMsg.textContent = 'Wrong password';
        adminMsg.className = 'err';
        adminLoginForm.style.display = 'block';
    }
}

$('btn-adm-save').onclick = function() {
    var models = admModels.value.split('\n').map(function(s) { return s.trim(); }).filter(Boolean);
    wsSend({
        type: 'admin-save', pass: adminLoggedPass,
        cfg: {
            apiKey: admKey.value.trim(),
            models: models,
            apiUrl: admUrl.value.trim(),
            adminPass: admNewpass.value.trim() || undefined
        }
    });
};

function handleAdminResult(m) {
    adminMsg.textContent = m.ok ? 'Saved successfully!' : 'Save failed';
    adminMsg.className = m.ok ? 'ok' : 'err';
    if (m.ok) setTimeout(function() { adminMsg.textContent = ''; }, 3000);
}

// ═══ HELPERS ═══
function esc(s) { if (!s) return ''; return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;'); }
function formatTime(ts) {
    var d = new Date(ts);
    return d.getHours().toString().padStart(2, '0') + ':' + d.getMinutes().toString().padStart(2, '0');
}
function timeAgo(ts) {
    var diff = Date.now() - ts;
    if (diff < 60000) return 'just now';
    if (diff < 3600000) return Math.floor(diff / 60000) + 'm ago';
    if (diff < 86400000) return Math.floor(diff / 3600000) + 'h ago';
    return Math.floor(diff / 86400000) + 'd ago';
}

// Expose for inline onclick
window.openLightbox = openLightbox;
window.deleteMsg = deleteMsg;
window.clearUserMsgs = clearUserMsgs;

// Focus name input only if not auto-logged in
if (!saved || !saved.name) inpName.focus();
})();


