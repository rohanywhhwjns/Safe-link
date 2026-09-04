// ============================================================
//  Safelink Bridge Server (v5 - Firebase Firestore)
//  Pairings stored in Firebase Firestore — survives ALL restarts
// ============================================================

const WebSocket = require('ws');
const https = require('https');
const crypto = require('crypto');
const { initializeApp, cert } = require('firebase-admin/app');
const { getFirestore, doc, setDoc, getDoc, updateDoc, deleteDoc, collection } = require('firebase-admin/firestore');

// ---- Config ----
const BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || 'YOUR_BOT_TOKEN_HERE';
const PORT = parseInt(process.env.PORT || '8080');
const POLL_INTERVAL = 1000;
const TG_API = `https://api.telegram.org/bot${BOT_TOKEN}`;

// ---- Firebase Config ----
// Set these in Render Environment Variables
const FIREBASE_PROJECT_ID = process.env.FIREBASE_PROJECT_ID || 'safelink-6bd86';
const FIREBASE_CLIENT_EMAIL = process.env.FIREBASE_CLIENT_EMAIL || '';
const FIREBASE_PRIVATE_KEY = process.env.FIREBASE_PRIVATE_KEY || '';

if (BOT_TOKEN === 'YOUR_BOT_TOKEN_HERE') {
  console.error('❌ No TELEGRAM_BOT_TOKEN set!');
  process.exit(1);
}

// ---- Initialize Firebase Admin ----
let db = null;
let dbReady = false;

async function initFirebase() {
  try {
    if (FIREBASE_CLIENT_EMAIL && FIREBASE_PRIVATE_KEY) {
      const serviceAccount = {
        projectId: FIREBASE_PROJECT_ID,
        clientEmail: FIREBASE_CLIENT_EMAIL,
        privateKey: FIREBASE_PRIVATE_KEY.replace(/\\n/g, '\n'),
      };
      initializeApp({ credential: cert(serviceAccount) });
      db = getFirestore();
      console.log('✅ Firebase Firestore initialized');
    } else {
      console.log('⚠️ No Firebase credentials — using memory only');
    }
  } catch(e) {
    console.error('Firebase init error:', e.message);
  }
  dbReady = true;
}

// ---- Firestore DB functions ----
async function dbLoadPairings() {
  if (!db) return [];
  try {
    const ref = db.collection('safelink').doc('pairings');
    const snap = await ref.get();
    if (snap.exists) {
      const data = snap.data();
      return data.pairs || [];
    }
    return [];
  } catch(e) { console.error('DB load pairings error:', e.message); return []; }
}

async function dbSavePairings(pairs) {
  if (!db) return;
  try {
    const ref = db.collection('safelink').doc('pairings');
    await ref.set({ pairs: pairs });
    console.log('💾 Saved pairings to Firestore');
  } catch(e) { console.error('DB save pairings error:', e.message); }
}

async function dbLoadContacts(userId) {
  if (!db) return [];
  try {
    const ref = db.collection('safelink').doc('contacts');
    const snap = await ref.get();
    if (snap.exists) {
      const data = snap.data();
      return data[userId] || [];
    }
    return [];
  } catch(e) { console.error('DB load contacts error:', e.message); return []; }
}

async function dbSaveContacts(userId, contacts) {
  if (!db) return;
  try {
    const ref = db.collection('safelink').doc('contacts');
    const snap = await ref.get();
    let data = snap.exists ? snap.data() : {};
    data[userId] = contacts;
    await ref.set(data);
    console.log(`💾 Saved ${contacts.length} contacts for ${userId} to Firestore`);
  } catch(e) { console.error('DB save contacts error:', e.message); }
}

async function dbLoadQueuedMessages(userId) {
  if (!db) return [];
  try {
    const ref = db.collection('safelink').doc('queuedMessages');
    const snap = await ref.get();
    if (snap.exists) {
      const data = snap.data();
      return data[userId] || [];
    }
    return [];
  } catch(e) { console.error('DB load queued error:', e.message); return []; }
}

async function dbSaveQueuedMessages(userId, messages) {
  if (!db) return;
  try {
    const ref = db.collection('safelink').doc('queuedMessages');
    const snap = await ref.get();
    let data = snap.exists ? snap.data() : {};
    if (messages && messages.length > 0) {
      data[userId] = messages;
    } else {
      delete data[userId];
    }
    await ref.set(data);
    console.log(`💾 Saved ${messages?.length || 0} queued messages for ${userId}`);
  } catch(e) { console.error('DB save queued error:', e.message); }
}

// ---- State ----
const clients = new Map(); // userId → ws connection (live only)
const tgChatToSafelink = new Map(); // tgChatId → safelinkUserId (persisted in Firestore)
const safelinkToTgChat = new Map(); // safelinkUserId → tgChatId (persisted in Firestore)
const pendingPairs = new Map(); // pairCode → { safelinkUserId, safelinkName }
let lastUpdateId = 0;

// ---- Load from Firestore on startup ----
async function initDB() {
  await initFirebase();
  const pairs = await dbLoadPairings();
  for (const pair of pairs) {
    tgChatToSafelink.set(pair.tg, pair.sl);
    safelinkToTgChat.set(pair.sl, pair.tg);
  }
  console.log(`📂 Loaded ${pairs.length} pairings from Firestore`);
}

// ---- Save pairings to Firestore (debounced) ----
let saveTimeout = null;
function savePairings() {
  if (saveTimeout) clearTimeout(saveTimeout);
  saveTimeout = setTimeout(async () => {
    const pairs = Array.from(tgChatToSafelink.entries()).map(([tg, sl]) => ({ tg, sl }));
    await dbSavePairings(pairs);
  }, 2000);
}

// ---- WebSocket Server ----
const wss = new WebSocket.Server({ port: PORT });
console.log(`🚀 Safelink Bridge Server v5 (Firebase) running on port ${PORT}`);

// Initialize DB before accepting connections
initDB().then(() => {
  console.log('✅ Database initialized — ready for connections');
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
      const checkPairing = async (retries = 0) => {
        if (!dbReady && retries < 10) {
          setTimeout(() => checkPairing(retries + 1), 500);
          return;
        }
        if (safelinkToTgChat.has(userId)) {
          ws.send(JSON.stringify({
            type: 'telegram-paired',
            tgChatId: safelinkToTgChat.get(userId),
          }));
          console.log(`🔗 Already paired`);
        }
      };
      checkPairing();

      // Send saved contacts/groups list (names only, no messages)
      const sendContacts = async (retries = 0) => {
        if (!dbReady && retries < 10) {
          setTimeout(() => sendContacts(retries + 1), 500);
          return;
        }
        const savedContacts = await dbLoadContacts(userId);
        if (savedContacts && savedContacts.length > 0) {
          ws.send(JSON.stringify({
            type: 'contacts-sync',
            contacts: savedContacts,
          }));
          console.log(`📋 Sent ${savedContacts.length} contacts to ${userId}`);
        }
      };
      sendContacts();

      // Deliver queued messages (temporary, auto-deleted after delivery)
      const deliverQueued = async (retries = 0) => {
        if (!dbReady && retries < 10) {
          setTimeout(() => deliverQueued(retries + 1), 500);
          return;
        }
        const queued = await dbLoadQueuedMessages(userId);
        if (queued && queued.length > 0) {
          console.log(`📬 Delivering ${queued.length} queued messages`);
          for (const qmsg of queued) {
            ws.send(JSON.stringify(qmsg));
          }
          await dbSaveQueuedMessages(userId, []);
        }
      };
      deliverQueued();
      return;
    }

    // ---- Sync contacts/groups list (names only, no messages) ----
    if (msg.type === 'sync-contacts') {
      const contactsList = (msg.contacts || []).map(c => ({
        id: c.id, name: c.name, type: c.type, members: c.members || 0,
      }));
      await dbSaveContacts(msg.userId, contactsList);
      console.log(`📋 Saved ${contactsList.length} contacts for ${msg.userId}`);
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
          const stickers = [];
          const stickerPromises = data.result.stickers.map(async (sticker) => {
            let url = null;
            const isVideo = sticker.is_video || false;
            const isAnimated = sticker.is_animated || false;
            
            if (isVideo) {
              url = await getTelegramFile(sticker.file_id);
            } else {
              if (sticker.thumb && sticker.thumb.file_id) {
                url = await getTelegramFile(sticker.thumb.file_id);
              }
              if (!url) {
                url = await getTelegramFile(sticker.file_id);
              }
            }
            return {
              url: url,
              emoji: sticker.emoji || '',
              isAnimated: isAnimated,
              isVideo: isVideo,
              fileId: sticker.file_id,
            };
          });
          const results = await Promise.all(stickerPromises);
          for (const s of results) {
            if (s.url) stickers.push(s);
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
    await sendTelegramMessage(chatId,
      `📊 Status:\nBridge: ✅ Online\nPaired: ${paired ? '✅' : '❌'}\nApp: ${online ? '✅ Online' : '❌ Offline'}\nUsers: ${clients.size}`
    );
    return;
  }

  if (text === '/unpair') {
    const slId = tgChatToSafelink.get(chatId.toString());
    if (slId) {
      tgChatToSafelink.delete(chatId.toString());
      safelinkToTgChat.delete(slId);
      savePairings();
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
      savePairings();

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
    let stickerSetName = '', stickerFileId = '', stickerKind = '';

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
      const isAnimated = message.sticker.is_animated || false;
      const isVideo = message.sticker.is_video || false;
      
      if (isVideo) {
        mediaUrl = await getTelegramFile(message.sticker.file_id);
        if (mediaUrl) { content = ''; mediaType = 'sticker'; stickerKind = 'video'; }
      } else if (isAnimated) {
        if (message.sticker.thumb && message.sticker.thumb.file_id) {
          mediaUrl = await getTelegramFile(message.sticker.thumb.file_id);
        }
        if (!mediaUrl) {
          mediaUrl = await getTelegramFile(message.sticker.file_id);
        }
        if (mediaUrl) { content = ''; mediaType = 'sticker'; stickerKind = 'animated'; }
      } else {
        if (message.sticker.thumb && message.sticker.thumb.file_id) {
          mediaUrl = await getTelegramFile(message.sticker.thumb.file_id);
        }
        if (!mediaUrl) {
          mediaUrl = await getTelegramFile(message.sticker.file_id);
        }
        if (mediaUrl) { content = ''; mediaType = 'sticker'; stickerKind = 'static'; }
      }
      
      if (message.sticker.emoji) fileName = message.sticker.emoji;
      stickerSetName = message.sticker.set_name || '';
      stickerFileId = message.sticker.file_id;
    } else if (message.location) {
      content = `📍 Location: ${message.location.latitude}, ${message.location.longitude}`;
    }

    const msgObj = {
      type: 'telegram-message',
      from: userName,
      text: content,
      mediaType, mediaUrl, fileName,
      stickerSetName: stickerSetName || '',
      stickerFileId: stickerFileId || '',
      stickerKind: stickerKind || '',
      timestamp: Date.now(),
    };

    if (isOnline) {
      slWs.send(JSON.stringify(msgObj));
      console.log(`📤 → Safelink: ${content || '[' + mediaType + ']'}`);
      await sendTelegramMessage(chatId, '✅ Sent to Safelink.');
    } else {
      // Queue message in Firestore
      const queued = await dbLoadQueuedMessages(slId);
      queued.push(msgObj);
      await dbSaveQueuedMessages(slId, queued);
      console.log(`📬 Queued (${queued.length} total)`);
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
    const base64Data = base64Image.split(',')[1];
    const buffer = Buffer.from(base64Data, 'base64');
    const boundary = '----FormBoundary' + crypto.randomBytes(8).toString('hex');
    const fileName = 'photo_' + Date.now() + '.jpg';
    
    const parts = [];
    parts.push(Buffer.from(`--${boundary}\r\nContent-Disposition: form-data; name="chat_id"\r\n\r\n${chatId}\r\n`));
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
    await sendTelegramMessage(chatId, '[Image could not be sent]');
  }
}

async function getTelegramFile(fileId) {
  try {
    const data = await fetchJSON(`${TG_API}/getFile?file_id=${fileId}`);
    if (data.ok) {
      const fileUrl = `https://api.telegram.org/file/bot${BOT_TOKEN}/${data.result.file_path}`;
      // Fetch the file and convert to base64 so the browser can display it permanently
      const base64 = await fetchUrlAsBase64(fileUrl, data.result.file_path);
      return base64 || fileUrl; // fallback to URL if base64 fails
    }
  } catch (e) { console.error('File error:', e.message); }
  return null;
}

// Fetch a URL and return as base64 data URL (so browser can store it permanently)
async function fetchUrlAsBase64(url, filePath) {
  try {
    const buffer = await fetchBuffer(url);
    const ext = (filePath || '').split('.').pop().toLowerCase();
    let mime = 'image/webp';
    if (ext === 'webm') mime = 'video/webm';
    else if (ext === 'mp4') mime = 'video/mp4';
    else if (ext === 'jpg' || ext === 'jpeg') mime = 'image/jpeg';
    else if (ext === 'png') mime = 'image/png';
    else if (ext === 'gif') mime = 'image/gif';
    else if (ext === 'webp') mime = 'image/webp';
    return `data:${mime};base64,${buffer.toString('base64')}`;
  } catch(e) {
    console.error('Base64 fetch error:', e.message);
    return null;
  }
}

// Fetch a URL and return as Buffer
function fetchBuffer(url) {
  return new Promise((resolve, reject) => {
    https.get(url, (res) => {
      if (res.statusCode !== 200) { reject(new Error(`HTTP ${res.statusCode}`)); return; }
      const chunks = [];
      res.on('data', chunk => chunks.push(chunk));
      res.on('end', () => resolve(Buffer.concat(chunks)));
    }).on('error', reject);
  });
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
  console.log(`📊 ${clients.size} online, ${tgChatToSafelink.size} paired`);
}, 30000);
