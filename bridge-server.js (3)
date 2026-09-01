// ============================================================
//  Safelink Bridge Server (v3 - with persistent storage)
//  Pairings and messages survive server restarts
// ============================================================

const WebSocket = require('ws');
const https = require('https');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

// ---- Config ----
const BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || 'YOUR_BOT_TOKEN_HERE';
const PORT = parseInt(process.env.PORT || '8080');
const POLL_INTERVAL = 1000;
const TG_API = `https://api.telegram.org/bot${BOT_TOKEN}`;

if (BOT_TOKEN === 'YOUR_BOT_TOKEN_HERE') {
  console.error('❌ No TELEGRAM_BOT_TOKEN set!');
  process.exit(1);
}

// ---- Persistent Storage (JSON file) ----
const DATA_FILE = path.join(__dirname, 'safelink-data.json');

function loadData() {
  try {
    if (fs.existsSync(DATA_FILE)) {
      return JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
    }
  } catch(e) { console.error('Load data error:', e.message); }
  return { pairs: [], queuedMessages: {} };
}

function saveData() {
  try {
    const data = {
      pairs: Array.from(tgChatToSafelink.entries()).map(([tg, sl]) => ({ tg, sl })),
      queuedMessages: Object.fromEntries(
        Array.from(messageQueue.entries()).map(([k, v]) => [k, v])
      ),
    };
    fs.writeFileSync(DATA_FILE, JSON.stringify(data, null, 2));
  } catch(e) { console.error('Save data error:', e.message); }
}

// ---- State ----
const clients = new Map(); // userId → ws connection (live, not persisted)
const tgChatToSafelink = new Map(); // tgChatId → safelinkUserId (persisted)
const safelinkToTgChat = new Map(); // safelinkUserId → tgChatId (persisted)
const pendingPairs = new Map(); // pairCode → { safelinkUserId, safelinkName, createdAt }
const messageQueue = new Map(); // userId → array of undelivered messages (persisted)
let lastUpdateId = 0;

// ---- Load saved data on startup ----
const savedData = loadData();
if (savedData.pairs) {
  for (const pair of savedData.pairs) {
    tgChatToSafelink.set(pair.tg, pair.sl);
    safelinkToTgChat.set(pair.sl, pair.tg);
  }
  console.log(`📂 Loaded ${savedData.pairs.length} saved pairings`);
}
if (savedData.queuedMessages) {
  for (const [userId, msgs] of Object.entries(savedData.queuedMessages)) {
    messageQueue.set(userId, msgs);
  }
  console.log(`📂 Loaded queued messages for ${Object.keys(savedData.queuedMessages).length} users`);
}

// Save data every 10 seconds if there are changes
let dataDirty = false;
setInterval(() => {
  if (dataDirty) { saveData(); dataDirty = false; }
}, 10000);

// Save on shutdown
process.on('SIGINT', () => { saveData(); process.exit(0); });
process.on('SIGTERM', () => { saveData(); process.exit(0); });

// ---- WebSocket Server ----
const wss = new WebSocket.Server({ port: PORT });
console.log(`🚀 Safelink Bridge Server v3 running on port ${PORT}`);

wss.on('connection', (ws, req) => {
  console.log(`📥 New WebSocket connection from ${req.socket.remoteAddress}`);

  ws.on('message', async (data) => {
    let msg;
    try { msg = JSON.parse(data.toString()); } catch (e) { return; }

    // ---- Client registration ----
    if (msg.type === 'register') {
      const userId = msg.userId;
      clients.set(userId, ws);
      ws.userId = userId;
      ws.isRegistered = true;
      console.log(`✅ Registered Safelink user: ${userId}`);
      ws.send(JSON.stringify({ type: 'registered', userId }));

      // If this user is already paired on Telegram, tell them
      if (safelinkToTgChat.has(userId)) {
        const tgChatId = safelinkToTgChat.get(userId);
        console.log(`🔗 User ${userId} already paired with Telegram`);
        ws.send(JSON.stringify({
          type: 'telegram-paired',
          tgChatId: tgChatId,
        }));
      }

      // Deliver any queued messages
      const queued = messageQueue.get(userId);
      if (queued && queued.length > 0) {
        console.log(`📬 Delivering ${queued.length} queued messages to ${userId}`);
        for (const qmsg of queued) {
          ws.send(JSON.stringify(qmsg));
        }
        messageQueue.delete(userId);
        dataDirty = true;
        saveData();
      }
      return;
    }

    // ---- Pairing: Safelink user requests a pairing code ----
    if (msg.type === 'request-pair-code') {
      const pairCode = crypto.randomBytes(3).toString('hex').toUpperCase();
      pendingPairs.set(pairCode, {
        safelinkUserId: ws.userId,
        safelinkName: msg.safelinkName || 'User',
        createdAt: Date.now(),
      });
      setTimeout(() => pendingPairs.delete(pairCode), 600000); // 10 min
      ws.send(JSON.stringify({ type: 'pair-code', code: pairCode }));
      console.log(`🔑 Pair code generated: ${pairCode} for user ${ws.userId}`);
      return;
    }

    // ---- Safelink → Telegram message ----
    if (msg.type === 'send-to-telegram') {
      const tgChatId = safelinkToTgChat.get(ws.userId);
      if (tgChatId) {
        const text = `${msg.text}`;
        await sendTelegramMessage(tgChatId, text);
        console.log(`📤 Safelink → Telegram: ${msg.text}`);
      }
      return;
    }

    // ---- Safelink → Telegram image ----
    if (msg.type === 'send-image-to-telegram') {
      const tgChatId = safelinkToTgChat.get(ws.userId);
      if (tgChatId && msg.image) {
        await sendTelegramMessage(tgChatId, '📷 Photo from Safelink');
        console.log(`📤 Safelink → Telegram: [Photo]`);
      }
      return;
    }
  });

  ws.on('close', () => {
    if (ws.userId) {
      clients.delete(ws.userId);
      console.log(`📤 Disconnected: ${ws.userId}`);
    }
  });

  ws.on('error', (err) => {
    console.error('WebSocket error:', err.message);
  });
});

// ---- Telegram Bot API Polling ----
async function pollTelegram() {
  try {
    const url = `${TG_API}/getUpdates?offset=${lastUpdateId + 1}&timeout=25`;
    const data = await fetchJSON(url);
    if (data.ok && data.result.length > 0) {
      for (const update of data.result) {
        lastUpdateId = update.update_id;
        if (update.message) {
          await handleTelegramMessage(update.message);
        }
      }
    }
  } catch (e) {
    if (e.code !== 'ETIMEDOUT' && e.code !== 'ECONNRESET') {
      console.error('Telegram poll error:', e.message);
    }
  }
}

async function handleTelegramMessage(message) {
  const chatId = message.chat.id;
  const userName = message.from?.first_name || message.from?.username || 'Unknown';
  const text = message.text || '';

  console.log(`📥 Telegram message from ${userName} (chat ${chatId}): ${text || '[media]'}`);

  // ---- /start ----
  if (text === '/start') {
    await sendTelegramMessage(chatId,
      `🤖 Welcome to Safelink Bridge!\n\n` +
      `This bot connects Telegram to Safelink — a privacy-first messenger.\n\n` +
      `To pair with your Safelink account:\n` +
      `1. Open Safelink app\n` +
      `2. Go to Settings → Telegram Bridge\n` +
      `3. Get a pairing code\n` +
      `4. Send the code here like: PAIR ABC123`
    );
    return;
  }

  // ---- /help ----
  if (text === '/help') {
    await sendTelegramMessage(chatId,
      `📖 Safelink Bridge Commands:\n\n` +
      `/start - Get started\n` +
      `/help - Show this help\n` +
      `/status - Check connection status\n` +
      `/unpair - Disconnect from Safelink\n` +
      `PAIR ABC123 - Pair with a Safelink account`
    );
    return;
  }

  // ---- /status ----
  if (text === '/status') {
    const paired = tgChatToSafelink.has(chatId.toString());
    const slId = tgChatToSafelink.get(chatId.toString());
    const online = slId && clients.has(slId);
    const queued = slId && messageQueue.has(slId) ? messageQueue.get(slId).length : 0;
    await sendTelegramMessage(chatId,
      `📊 Status:\n\n` +
      `Bridge: ✅ Online\n` +
      `Paired: ${paired ? '✅ Connected' : '❌ Not paired'}\n` +
      `App Online: ${online ? '✅ Yes' : '❌ No'}\n` +
      `Queued Messages: ${queued}\n` +
      `Connected Users: ${clients.size}`
    );
    return;
  }

  // ---- /unpair ----
  if (text === '/unpair') {
    const slId = tgChatToSafelink.get(chatId.toString());
    if (slId) {
      tgChatToSafelink.delete(chatId.toString());
      safelinkToTgChat.delete(slId);
      dataDirty = true; saveData();
      await sendTelegramMessage(chatId, '✅ Disconnected from Safelink.');
    } else {
      await sendTelegramMessage(chatId, '❌ Not currently paired.');
    }
    return;
  }

  // ---- Pairing ----
  if (text.toUpperCase().startsWith('PAIR ')) {
    const code = text.substring(5).trim().toUpperCase();
    const pair = pendingPairs.get(code);
    if (pair) {
      // Store pairing permanently
      tgChatToSafelink.set(chatId.toString(), pair.safelinkUserId);
      safelinkToTgChat.set(pair.safelinkUserId, chatId.toString());
      pendingPairs.delete(code);
      dataDirty = true; saveData();

      // Try to notify the Safelink client
      const slWs = clients.get(pair.safelinkUserId);
      if (slWs && slWs.readyState === WebSocket.OPEN) {
        slWs.send(JSON.stringify({
          type: 'telegram-paired',
          tgChatId: chatId.toString(),
        }));
        console.log(`✅ Notified app immediately`);
      } else {
        console.log(`⚠️ App offline. Will notify on reconnect.`);
      }

      await sendTelegramMessage(chatId,
        `✅ Paired with Safelink!\n\n` +
        `Your Safelink contact: ${pair.safelinkName}\n\n` +
        `Now you can:\n` +
        `• Send messages → they appear in Safelink\n` +
        `• Forward photos, files, videos → they appear in Safelink\n\n` +
        `Use /status anytime to check connection.`
      );
      console.log(`🔗 Paired Telegram ${chatId} ↔ Safelink ${pair.safelinkUserId}`);
    } else {
      await sendTelegramMessage(chatId,
        `❌ Invalid or expired code.\n\n` +
        `Get a fresh code from Safelink app: Settings → Telegram Bridge.`
      );
    }
    return;
  }

  // ---- Forwarded content / regular messages ----
  const slId = tgChatToSafelink.get(chatId.toString());
  if (slId) {
    const slWs = clients.get(slId);
    const isOnline = slWs && slWs.readyState === WebSocket.OPEN;
    let content = '';
    let mediaType = null;
    let mediaUrl = null;
    let fileName = null;

    if (message.text) {
      content = message.text;
    } else if (message.photo) {
      const photo = message.photo[message.photo.length - 1];
      mediaUrl = await getTelegramFile(photo.file_id);
      if (mediaUrl) { content = '[Photo]'; mediaType = 'photo'; }
    } else if (message.document) {
      mediaUrl = await getTelegramFile(message.document.file_id);
      if (mediaUrl) { content = `[File: ${message.document.file_name}]`; mediaType = 'document'; fileName = message.document.file_name; }
    } else if (message.video) {
      mediaUrl = await getTelegramFile(message.video.file_id);
      if (mediaUrl) { content = '[Video]'; mediaType = 'video'; }
    } else if (message.voice) {
      mediaUrl = await getTelegramFile(message.voice.file_id);
      if (mediaUrl) { content = '[Voice message]'; mediaType = 'voice'; }
    } else if (message.sticker) {
      mediaUrl = await getTelegramFile(message.sticker.file_id);
      if (mediaUrl) { content = `[Sticker ${message.sticker.emoji || '🙂'}]`; mediaType = 'sticker'; }
    } else if (message.location) {
      content = `[Location: ${message.location.latitude}, ${message.location.longitude}]`;
    }

    const msgObj = {
      type: 'telegram-message',
      from: userName,
      text: content,
      mediaType,
      mediaUrl,
      fileName,
      timestamp: Date.now(),
    };

    if (isOnline) {
      slWs.send(JSON.stringify(msgObj));
      console.log(`📤 Telegram → Safelink: "${content}"`);
      await sendTelegramMessage(chatId, '✅ Sent to Safelink.');
    } else {
      // Queue for later
      if (!messageQueue.has(slId)) messageQueue.set(slId, []);
      messageQueue.get(slId).push(msgObj);
      dataDirty = true; saveData();
      console.log(`📬 Queued for offline user (${messageQueue.get(slId).length} total)`);
      await sendTelegramMessage(chatId, '✅ Saved! Open Safelink app to see it.');
    }
  } else {
    await sendTelegramMessage(chatId,
      `👋 Hi ${userName}!\n\n` +
      `Pair with Safelink:\n` +
      `1. Open Safelink app\n` +
      `2. Settings → Telegram Bridge\n` +
      `3. Send code here like: PAIR ABC123`
    );
  }
}

// ---- Telegram API helpers ----
async function sendTelegramMessage(chatId, text) {
  try {
    await fetchJSON(`${TG_API}/sendMessage`, 'POST', {
      chat_id: chatId, text: text,
    });
  } catch (e) { console.error('Send TG error:', e.message); }
}

async function getTelegramFile(fileId) {
  try {
    const data = await fetchJSON(`${TG_API}/getFile?file_id=${fileId}`);
    if (data.ok) {
      return `https://api.telegram.org/file/bot${BOT_TOKEN}/${data.result.file_path}`;
    }
  } catch (e) { console.error('Get file error:', e.message); }
  return null;
}

// ---- HTTP helper ----
function fetchJSON(url, method = 'GET', body = null) {
  return new Promise((resolve, reject) => {
    const urlObj = new URL(url);
    const options = {
      hostname: urlObj.hostname,
      path: urlObj.pathname + urlObj.search,
      method: method,
      headers: {}
    };
    if (body) {
      const bodyStr = JSON.stringify(body);
      options.headers['Content-Type'] = 'application/json';
      options.headers['Content-Length'] = Buffer.byteLength(bodyStr);
      const req = https.request(options, (res) => {
        let data = '';
        res.on('data', chunk => data += chunk);
        res.on('end', () => { try { resolve(JSON.parse(data)); } catch(e) { reject(e); } });
      });
      req.on('error', reject);
      req.write(bodyStr);
      req.end();
    } else {
      https.get(url, (res) => {
        let data = '';
        res.on('data', chunk => data += chunk);
        res.on('end', () => { try { resolve(JSON.parse(data)); } catch(e) { reject(e); } });
      }).on('error', reject);
    }
  });
}

// ---- Start polling ----
console.log('📡 Starting Telegram polling...');
setInterval(pollTelegram, POLL_INTERVAL);
pollTelegram();

// ---- Stats ----
setInterval(() => {
  console.log(`📊 Stats: ${clients.size} online, ${tgChatToSafelink.size} paired, ${messageQueue.size > 0 ? Array.from(messageQueue.values()).reduce((a,b)=>a+b.length,0) : 0} queued`);
}, 30000);
