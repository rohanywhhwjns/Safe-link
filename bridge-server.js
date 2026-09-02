// ============================================================
//  Safelink Bridge Server (v4 - Cloud Database)
//  Pairings stored in JSONBin.io — survives ALL restarts
// ============================================================

const WebSocket = require('ws');
const https = require('https');
const crypto = require('crypto');

// ---- Config ----
const BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || 'YOUR_BOT_TOKEN_HERE';
const PORT = parseInt(process.env.PORT || '8080');
const POLL_INTERVAL = 1000;
const TG_API = `https://api.telegram.org/bot${BOT_TOKEN}`;

// ---- JSONBin.io Config ----
// You'll set these in Render Environment Variables
const JSONBIN_URL = process.env.JSONBIN_URL || ''; // e.g. https://api.jsonbin.io/v3/b/YOUR_BIN_ID
const JSONBIN_KEY = process.env.JSONBIN_KEY || ''; // Your JSONBin API key

if (BOT_TOKEN === 'YOUR_BOT_TOKEN_HERE') {
  console.error('❌ No TELEGRAM_BOT_TOKEN set!');
  process.exit(1);
}

// ---- Cloud Database (JSONBin) ----
async function dbLoad() {
  if (!JSONBIN_URL || !JSONBIN_KEY) {
    console.log('⚠️ No JSONBin configured — using memory only');
    return { pairs: [], contacts: {}, queuedMessages: {} };
  }
  try {
    const data = await fetchJSON(JSONBIN_URL + '/latest', 'GET', null, {
      'X-Master-Key': JSONBIN_KEY,
    });
    console.log('📂 Loaded from cloud DB');
    return data.record || { pairs: [], contacts: {}, queuedMessages: {} };
  } catch(e) {
    console.error('DB load error:', e.message);
    return { pairs: [], contacts: {}, queuedMessages: {} };
  }
}

async function dbSave(data) {
  if (!JSONBIN_URL || !JSONBIN_KEY) return;
  try {
    await fetchJSON(JSONBIN_URL, 'PUT', data, {
      'X-Master-Key': JSONBIN_KEY,
      'Content-Type': 'application/json',
    });
    console.log('💾 Saved to cloud DB');
  } catch(e) {
    console.error('DB save error:', e.message);
  }
}

// ---- State ----
const clients = new Map(); // userId → ws connection (live only)
const tgChatToSafelink = new Map(); // tgChatId → safelinkUserId (persisted in cloud)
const safelinkToTgChat = new Map(); // safelinkUserId → tgChatId (persisted in cloud)
const pendingPairs = new Map(); // pairCode → { safelinkUserId, safelinkName }
const messageQueue = new Map(); // userId → messages (persisted in cloud)
let lastUpdateId = 0;
let dbData = { pairs: [], contacts: {}, queuedMessages: {} };

// ---- Load from cloud DB on startup ----
async function initDB() {
  dbData = await dbLoad();
  if (dbData.pairs) {
    for (const pair of dbData.pairs) {
      tgChatToSafelink.set(pair.tg, pair.sl);
      safelinkToTgChat.set(pair.sl, pair.tg);
    }
    console.log(`📂 Loaded ${dbData.pairs.length} pairings from cloud`);
  }
  // Load contacts/groups (names only, no messages)
  if (dbData.contacts) {
    console.log(`📂 Loaded contacts for ${Object.keys(dbData.contacts).length} users`);
  }
  // Queue stays in memory only (not persisted) for privacy
  if (dbData.queuedMessages) {
    for (const [userId, msgs] of Object.entries(dbData.queuedMessages)) {
      messageQueue.set(userId, msgs);
    }
  }
}

// ---- Save to cloud DB (debounced) ----
let saveTimeout = null;
function saveDB() {
  if (saveTimeout) clearTimeout(saveTimeout);
  saveTimeout = setTimeout(async () => {
    dbData.pairs = Array.from(tgChatToSafelink.entries()).map(([tg, sl]) => ({ tg, sl }));
    // Save only contacts metadata (names, IDs) — NO messages
    dbData.queuedMessages = Object.fromEntries(messageQueue.entries());
    await dbSave(dbData);
  }, 2000);
}

// ---- WebSocket Server ----
const wss = new WebSocket.Server({ port: PORT });
console.log(`🚀 Safelink Bridge Server v4 running on port ${PORT}`);

// Initialize DB
initDB().then(() => {
  console.log('✅ Database initialized');
});

wss.on('connection', (ws, req) => {
  console.log(`📥 New WebSocket connection`);

  ws.on('message', async (data) => {
    let msg;
    try { msg = JSON.parse(data.toString()); } catch (e) { return; }

    // ---- Client registration ----
    if (msg.type === 'register') {
      const userId = msg.userId;
      clients.set(userId, ws);
      ws.userId = userId;
      console.log(`✅ Registered: ${userId}`);
      ws.send(JSON.stringify({ type: 'registered', userId }));

      // Check if already paired
      if (safelinkToTgChat.has(userId)) {
        ws.send(JSON.stringify({
          type: 'telegram-paired',
          tgChatId: safelinkToTgChat.get(userId),
        }));
        console.log(`🔗 Already paired`);
      }

      // Send saved contacts/groups list (names only, no messages)
      const savedContacts = dbData.contacts && dbData.contacts[userId];
      if (savedContacts && savedContacts.length > 0) {
        ws.send(JSON.stringify({
          type: 'contacts-sync',
          contacts: savedContacts,
        }));
        console.log(`📋 Sent ${savedContacts.length} contacts to ${userId}`);
      }

      // Deliver queued messages (temporary, auto-deleted after delivery)
      const queued = messageQueue.get(userId);
      if (queued && queued.length > 0) {
        console.log(`📬 Delivering ${queued.length} queued messages`);
        for (const qmsg of queued) {
          ws.send(JSON.stringify(qmsg));
        }
        messageQueue.delete(userId);
        saveDB();
      }
      return;
    }

    // ---- Sync contacts/groups list (names only, no messages) ----
    if (msg.type === 'sync-contacts') {
      if (!dbData.contacts) dbData.contacts = {};
      // Save only chat names and IDs — NO messages
      dbData.contacts[msg.userId] = (msg.contacts || []).map(c => ({
        id: c.id,
        name: c.name,
        type: c.type,
        members: c.members || 0,
      }));
      saveDB();
      console.log(`📋 Saved ${dbData.contacts[msg.userId].length} contacts for ${msg.userId}`);
      return;
    }

    // ---- Request pair code ----
    if (msg.type === 'request-pair-code') {
      const pairCode = crypto.randomBytes(3).toString('hex').toUpperCase();
      pendingPairs.set(pairCode, {
        safelinkUserId: ws.userId,
        safelinkName: msg.safelinkName || 'User',
        createdAt: Date.now(),
      });
      setTimeout(() => pendingPairs.delete(pairCode), 600000);
      ws.send(JSON.stringify({ type: 'pair-code', code: pairCode }));
      console.log(`🔑 Pair code: ${pairCode} for ${ws.userId}`);
      return;
    }

    // ---- Safelink → Telegram (text) ----
    if (msg.type === 'send-to-telegram') {
      const tgChatId = safelinkToTgChat.get(ws.userId);
      if (tgChatId) {
        await sendTelegramMessage(tgChatId, msg.text);
        console.log(`📤 Safelink → Telegram: ${msg.text}`);
      }
      return;
    }

    // ---- Safelink → Telegram (image) ----
    if (msg.type === 'send-to-telegram-image') {
      const tgChatId = safelinkToTgChat.get(ws.userId);
      if (tgChatId) {
        await sendTelegramPhoto(tgChatId, msg.image);
        console.log(`📤 Safelink → Telegram: [image]`);
      }
      return;
    }

    // ---- Get sticker pack from Telegram ----
    if (msg.type === 'get-sticker-pack') {
      try {
        const data = await fetchJSON(`${TG_API}/getStickerSet?name=${encodeURIComponent(msg.setName)}`);
        if (data.ok && data.result && data.result.stickers) {
          // Get URL for each sticker
          const stickers = [];
          for (const sticker of data.result.stickers) {
            let url = null;
            // Try thumbnail first
            if (sticker.thumb && sticker.thumb.file_id) {
              url = await getTelegramFile(sticker.thumb.file_id);
            }
            if (!url) {
              url = await getTelegramFile(sticker.file_id);
            }
            stickers.push({
              url: url,
              emoji: sticker.emoji || '',
              isAnimated: sticker.is_animated || false,
              isVideo: sticker.is_video || false,
            });
          }
          ws.send(JSON.stringify({
            type: 'sticker-pack',
            setName: msg.setName,
            title: data.result.title,
            stickers: stickers,
          }));
          console.log(`📋 Sent sticker pack: ${data.result.title} (${stickers.length} stickers)`);
        } else {
          ws.send(JSON.stringify({ type: 'sticker-pack-error', error: 'Pack not found' }));
        }
      } catch(e) {
        console.error('Sticker pack error:', e.message);
        ws.send(JSON.stringify({ type: 'sticker-pack-error', error: e.message }));
      }
      return;
    }

    // ---- Send sticker to Telegram by file_id ----
    if (msg.type === 'send-sticker-to-telegram') {
      const tgChatId = safelinkToTgChat.get(ws.userId);
      if (tgChatId) {
        try {
          await fetchJSON(`${TG_API}/sendSticker`, 'POST', { chat_id: tgChatId, sticker: msg.fileId }, null);
          console.log(`📤 Safelink → Telegram: [sticker]`);
        } catch(e) { console.error('Send sticker error:', e.message); }
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
    console.error('WS error:', err.message);
  });
});

// ---- Telegram Polling ----
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
      console.error('Poll error:', e.message);
    }
  }
}

async function handleTelegramMessage(message) {
  const chatId = message.chat.id;
  const userName = message.from?.first_name || message.from?.username || 'Unknown';
  const text = message.text || '';

  console.log(`📥 TG from ${userName}: ${text || '[media]'}`);

  if (text === '/start') {
    await sendTelegramMessage(chatId,
      `🤖 Welcome to Safelink Bridge!\n\n` +
      `To pair:\n1. Open Safelink app\n2. Settings → Telegram Bridge\n3. Send code here: PAIR ABC123`
    );
    return;
  }

  if (text === '/help') {
    await sendTelegramMessage(chatId,
      `📖 Commands:\n/start - Start\n/help - Help\n/status - Status\n/unpair - Unpair\nPAIR ABC123 - Pair`
    );
    return;
  }

  if (text === '/status') {
    const paired = tgChatToSafelink.has(chatId.toString());
    const slId = tgChatToSafelink.get(chatId.toString());
    const online = slId && clients.has(slId);
    const queued = slId && messageQueue.has(slId) ? messageQueue.get(slId).length : 0;
    await sendTelegramMessage(chatId,
      `📊 Status:\nBridge: ✅ Online\nPaired: ${paired ? '✅' : '❌'}\nApp: ${online ? '✅ Online' : '❌ Offline'}\nQueued: ${queued}\nUsers: ${clients.size}`
    );
    return;
  }

  if (text === '/unpair') {
    const slId = tgChatToSafelink.get(chatId.toString());
    if (slId) {
      tgChatToSafelink.delete(chatId.toString());
      safelinkToTgChat.delete(slId);
      saveDB();
      await sendTelegramMessage(chatId, '✅ Disconnected.');
    } else {
      await sendTelegramMessage(chatId, '❌ Not paired.');
    }
    return;
  }

  // ---- Pairing ----
  if (text.toUpperCase().startsWith('PAIR ')) {
    const code = text.substring(5).trim().toUpperCase();
    const pair = pendingPairs.get(code);
    if (pair) {
      tgChatToSafelink.set(chatId.toString(), pair.safelinkUserId);
      safelinkToTgChat.set(pair.safelinkUserId, chatId.toString());
      pendingPairs.delete(code);
      saveDB(); // Save to cloud!

      const slWs = clients.get(pair.safelinkUserId);
      if (slWs && slWs.readyState === WebSocket.OPEN) {
        slWs.send(JSON.stringify({
          type: 'telegram-paired',
          tgChatId: chatId.toString(),
        }));
      }

      await sendTelegramMessage(chatId,
        `✅ Paired with Safelink!\n\nContact: ${pair.safelinkName}\n\nNow you can send messages, photos, files, videos.`
      );
      console.log(`🔗 Paired ${chatId} ↔ ${pair.safelinkUserId}`);
    } else {
      await sendTelegramMessage(chatId,
        `❌ Invalid or expired code.\nGet a fresh code from Safelink app.`
      );
    }
    return;
  }

  // ---- Messages / Media ----
  const slId = tgChatToSafelink.get(chatId.toString());
  if (slId) {
    const slWs = clients.get(slId);
    const isOnline = slWs && slWs.readyState === WebSocket.OPEN;
    let content = '', mediaType = null, mediaUrl = null, fileName = null;
    let stickerSetName = '', stickerFileId = '';

    if (message.text) {
      content = message.text;
    } else if (message.photo) {
      const photo = message.photo[message.photo.length - 1];
      mediaUrl = await getTelegramFile(photo.file_id);
      if (mediaUrl) { content = ''; mediaType = 'photo'; }
    } else if (message.document) {
      mediaUrl = await getTelegramFile(message.document.file_id);
      if (mediaUrl) { content = ''; mediaType = 'document'; fileName = message.document.file_name; }
    } else if (message.video) {
      mediaUrl = await getTelegramFile(message.video.file_id);
      if (mediaUrl) { content = ''; mediaType = 'video'; }
    } else if (message.voice) {
      mediaUrl = await getTelegramFile(message.voice.file_id);
      if (mediaUrl) { content = ''; mediaType = 'voice'; }
    } else if (message.sticker) {
      // Get the actual sticker image
      if (message.sticker.thumb && message.sticker.thumb.file_id) {
        mediaUrl = await getTelegramFile(message.sticker.thumb.file_id);
      }
      if (!mediaUrl) {
        mediaUrl = await getTelegramFile(message.sticker.file_id);
      }
      if (mediaUrl) { content = ''; mediaType = 'sticker'; }
      if (message.sticker.emoji) fileName = message.sticker.emoji;
      // Store sticker set name and file_id for sticker pack feature
      stickerSetName = message.sticker.set_name || '';
      stickerFileId = message.sticker.file_id;
    } else if (message.location) {
      content = `📍 Location: ${message.location.latitude}, ${message.location.longitude}`;
    }

    const msgObj = {
      type: 'telegram-message',
      from: userName,
      text: content,
      mediaType,
      mediaUrl,
      fileName,
      stickerSetName: stickerSetName || '',
      stickerFileId: stickerFileId || '',
      timestamp: Date.now(),
    };

    if (isOnline) {
      slWs.send(JSON.stringify(msgObj));
      console.log(`📤 → Safelink: ${content || '[' + mediaType + ']'}`);
      await sendTelegramMessage(chatId, '✅ Sent to Safelink.');
    } else {
      if (!messageQueue.has(slId)) messageQueue.set(slId, []);
      messageQueue.get(slId).push(msgObj);
      saveDB(); // Save to cloud!
      console.log(`📬 Queued (${messageQueue.get(slId).length} total)`);
      await sendTelegramMessage(chatId, '✅ Saved! Open Safelink app to see it.');
    }
  } else {
    await sendTelegramMessage(chatId,
      `👋 Hi ${userName}!\nPair with Safelink:\n1. Open Safelink app\n2. Settings → Telegram Bridge\n3. Send code: PAIR ABC123`
    );
  }
}

// ---- Telegram API helpers ----
async function sendTelegramMessage(chatId, text) {
  try {
    await fetchJSON(`${TG_API}/sendMessage`, 'POST', { chat_id: chatId, text }, null);
  } catch (e) { console.error('TG send error:', e.message); }
}

async function sendTelegramPhoto(chatId, base64Image) {
  try {
    // Convert base64 data URL to buffer
    const base64Data = base64Image.split(',')[1];
    const buffer = Buffer.from(base64Data, 'base64');
    // Send photo via multipart form data using https
    const boundary = '----FormBoundary' + crypto.randomBytes(8).toString('hex');
    const fileName = 'photo_' + Date.now() + '.jpg';
    
    const parts = [];
    // chat_id field
    parts.push(Buffer.from(`--${boundary}\r\nContent-Disposition: form-data; name="chat_id"\r\n\r\n${chatId}\r\n`));
    // photo field
    parts.push(Buffer.from(`--${boundary}\r\nContent-Disposition: form-data; name="photo"; filename="${fileName}"\r\nContent-Type: image/jpeg\r\n\r\n`));
    parts.push(buffer);
    parts.push(Buffer.from(`\r\n--${boundary}--\r\n`));
    
    const body = Buffer.concat(parts);
    
    await new Promise((resolve, reject) => {
      const urlObj = new URL(`${TG_API}/sendPhoto`);
      const req = https.request({
        hostname: urlObj.hostname,
        path: urlObj.pathname,
        method: 'POST',
        headers: {
          'Content-Type': `multipart/form-data; boundary=${boundary}`,
          'Content-Length': body.length,
        },
      }, (res) => {
        let data = '';
        res.on('data', chunk => data += chunk);
        res.on('end', () => { resolve(); });
      });
      req.on('error', reject);
      req.write(body);
      req.end();
    });
    console.log('📸 Photo sent to Telegram');
  } catch (e) { 
    console.error('TG photo error:', e.message); 
    // Fallback: send text message
    await sendTelegramMessage(chatId, '[Image could not be sent]');
  }
}

async function getTelegramFile(fileId) {
  try {
    const data = await fetchJSON(`${TG_API}/getFile?file_id=${fileId}`);
    if (data.ok) return `https://api.telegram.org/file/bot${BOT_TOKEN}/${data.result.file_path}`;
  } catch (e) { console.error('File error:', e.message); }
  return null;
}

// ---- HTTP helper (with custom headers) ----
function fetchJSON(url, method = 'GET', body = null, headers = null) {
  return new Promise((resolve, reject) => {
    const urlObj = new URL(url);
    const options = {
      hostname: urlObj.hostname,
      path: urlObj.pathname + urlObj.search,
      method: method,
      headers: headers || {},
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
      https.get(url, { headers: headers || {} }, (res) => {
        let data = '';
        res.on('data', chunk => data += chunk);
        res.on('end', () => { try { resolve(JSON.parse(data)); } catch(e) { reject(e); } });
      }).on('error', reject);
    }
  });
}

// ---- Start ----
console.log('📡 Starting Telegram polling...');
setInterval(pollTelegram, POLL_INTERVAL);
pollTelegram();

setInterval(() => {
  console.log(`📊 ${clients.size} online, ${tgChatToSafelink.size} paired, ${Array.from(messageQueue.values()).reduce((a,b)=>a+b.length,0)} queued`);
}, 30000);
