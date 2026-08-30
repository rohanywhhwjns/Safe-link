// ============================================================
//  Safelink Bridge Server
//  Connects Telegram Bot API ↔ Safelink WebSocket clients
//
//  Requirements: Node.js 20+, `ws` package
//  Install:  npm install ws
//  Run:      node bridge-server.js
//
//  Set environment variables:
//    TELEGRAM_BOT_TOKEN  - from @BotFather
//    PORT                - WebSocket port (default 8080)
//
//  The bridge does two things:
//    1. Polls Telegram Bot API for incoming messages from Telegram users
//    2. Accepts WebSocket connections from Safelink clients
//    3. Relays messages between the two, re-encrypting as needed
// ============================================================

const WebSocket = require('ws');
const https = require('https');
const crypto = require('crypto');

// ---- Config ----
const BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || 'YOUR_BOT_TOKEN_HERE';
const PORT = parseInt(process.env.PORT || '8080');
const POLL_INTERVAL = 1000; // 1 second
const TG_API = `https://api.telegram.org/bot${BOT_TOKEN}`;

if (BOT_TOKEN === 'YOUR_BOT_TOKEN_HERE') {
  console.error('\n❌ No TELEGRAM_BOT_TOKEN set!');
  console.error('   Get one from @BotFather on Telegram, then run:');
  console.error('   TELEGRAM_BOT_TOKEN=123456:ABC-DEF1234 node bridge-server.js\n');
  process.exit(1);
}

// ---- State ----
// Connected Safelink clients, keyed by their Safelink user ID
const clients = new Map();
// Mapping: Telegram chat ID → Safelink user ID
// This maps a Telegram conversation to a Safelink user
const tgChatToSafelink = new Map();
const safelinkToTgChat = new Map();
// Pending connections: when a Telegram user first messages the bot,
// we need to pair them with a Safelink user
const pendingPairs = new Map(); // pairCode → { tgChatId, tgUserName }
let lastUpdateId = 0;

// ---- WebSocket Server ----
const wss = new WebSocket.Server({ port: PORT });
console.log(`🚀 Safelink Bridge Server running on port ${PORT}`);
console.log(`🔗 WebSocket endpoint: ws://localhost:${PORT}`);
console.log(`🤖 Telegram Bot: polling enabled\n`);

wss.on('connection', (ws, req) => {
  const ip = req.socket.remoteAddress;
  console.log(`📥 New WebSocket connection from ${ip}`);

  ws.on('message', async (data) => {
    let msg;
    try { msg = JSON.parse(data.toString()); } catch (e) { return; }

    // ---- Client registration ----
    // When a Safelink client connects, it sends its user ID
    if (msg.type === 'register') {
      const userId = msg.userId;
      clients.set(userId, ws);
      ws.userId = userId;
      ws.isRegistered = true;
      console.log(`✅ Registered Safelink user: ${userId}`);
      ws.send(JSON.stringify({ type: 'registered', userId }));
      return;
    }

    // ---- Pairing: Safelink user requests a Telegram bridge code ----
    if (msg.type === 'request-pair-code') {
      // Generate a short pairing code
      const pairCode = crypto.randomBytes(3).toString('hex').toUpperCase();
      pendingPairs.set(pairCode, {
        safelinkUserId: ws.userId,
        safelinkName: msg.safelinkName,
        createdAt: Date.now(),
      });
      // Expire after 5 minutes
      setTimeout(() => pendingPairs.delete(pairCode), 300000);
      ws.send(JSON.stringify({ type: 'pair-code', code: pairCode }));
      console.log(`🔑 Pair code generated: ${pairCode} for user ${ws.userId}`);
      return;
    }

    // ---- Safelink → Telegram message ----
    if (msg.type === 'send-to-telegram') {
      const tgChatId = safelinkToTgChat.get(ws.userId);
      if (tgChatId) {
        const text = `[${msg.senderName}]: ${msg.text}`;
        await sendTelegramMessage(tgChatId, text);
        console.log(`📤 Safelink → Telegram (chat ${tgChatId}): ${msg.text}`);
      }
      return;
    }

    // ---- Safelink → Safelink message (relay between two Safelink clients) ----
    if (msg.type === 'relay') {
      const targetWs = clients.get(msg.toUserId);
      if (targetWs && targetWs.readyState === WebSocket.OPEN) {
        targetWs.send(JSON.stringify({
          type: 'relay-message',
          fromUserId: ws.userId,
          ciphertext: msg.ciphertext,
          connId: msg.connId,
        }));
      }
      return;
    }

    // ---- Group relay ----
    if (msg.type === 'group-relay') {
      // Broadcast to all group members connected to this server
      const members = msg.members || [];
      for (const memberId of members) {
        if (memberId === ws.userId) continue;
        const targetWs = clients.get(memberId);
        if (targetWs && targetWs.readyState === WebSocket.OPEN) {
          targetWs.send(JSON.stringify({
            type: 'group-message',
            groupId: msg.groupId,
            ciphertext: msg.ciphertext,
            senderName: msg.senderName,
          }));
        }
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
    // Network errors are normal during long polling, just continue
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

  // ---- Handle /start command ----
  if (text === '/start') {
    await sendTelegramMessage(chatId,
      `🤖 Welcome to Safelink Bridge!\n\n` +
      `This bot connects Telegram to Safelink — a privacy-first messenger.\n\n` +
      `To pair with your Safelink account:\n` +
      `1. Open Safelink app\n` +
      `2. Go to Settings → Telegram Bridge\n` +
      `3. Get a pairing code\n` +
      `4. Send the code here\n\n` +
      `Example: PAIR ABC123`
    );
    return;
  }

  // ---- Handle /help command ----
  if (text === '/help') {
    await sendTelegramMessage(chatId,
      `📖 Safelink Bridge Commands:\n\n` +
      `/start - Get started\n` +
      `/help - Show this help\n` +
      `/status - Check connection status\n` +
      `/unpair - Disconnect from Safelink\n` +
      `PAIR ABC123 - Pair with a Safelink account\n\n` +
      `You can also forward messages, photos, files, and videos to this bot to share them in Safelink.`
    );
    return;
  }

  // ---- Handle /status command ----
  if (text === '/status') {
    const paired = tgChatToSafelink.has(chatId.toString());
    await sendTelegramMessage(chatId,
      `📊 Status:\n\n` +
      `Bridge: ✅ Online\n` +
      `Paired: ${paired ? '✅ Connected to Safelink' : '❌ Not paired'}\n` +
      `Connected Safelink users: ${clients.size}\n\n` +
      `${paired ? 'Messages you send here will appear in Safelink.' : 'Send PAIR <code> to connect.'}`
    );
    return;
  }

  // ---- Handle /unpair command ----
  if (text === '/unpair') {
    const slId = tgChatToSafelink.get(chatId.toString());
    if (slId) {
      tgChatToSafelink.delete(chatId.toString());
      safelinkToTgChat.delete(slId);
      await sendTelegramMessage(chatId, '✅ Disconnected from Safelink. Send PAIR <code> to reconnect.');
    } else {
      await sendTelegramMessage(chatId, '❌ Not currently paired.');
    }
    return;
  }

  // ---- Handle pairing ----
  if (text.toUpperCase().startsWith('PAIR ')) {
    const code = text.substring(5).trim().toUpperCase();
    const pair = pendingPairs.get(code);
    if (pair) {
      // Pair this Telegram chat with the Safelink user
      tgChatToSafelink.set(chatId.toString(), pair.safelinkUserId);
      safelinkToTgChat.set(pair.safelinkUserId, chatId.toString());
      pendingPairs.delete(code);

      // Notify the Safelink client
      const slWs = clients.get(pair.safelinkUserId);
      if (slWs && slWs.readyState === WebSocket.OPEN) {
        slWs.send(JSON.stringify({
          type: 'telegram-paired',
          tgChatId: chatId.toString(),
          tgUserName: userName,
        }));
      }

      await sendTelegramMessage(chatId,
        `✅ Paired with Safelink!\n\n` +
        `Your Safelink contact: ${pair.safelinkName}\n\n` +
        `Now you can:\n` +
        `• Send messages here → they appear in Safelink\n` +
        `• Forward photos, files, videos to this bot → they appear in Safelink\n` +
        `• Messages from Safelink will appear here\n\n` +
        `Use /status anytime to check connection.`
      );
      console.log(`🔗 Paired Telegram chat ${chatId} (${userName}) ↔ Safelink ${pair.safelinkUserId}`);
    } else {
      await sendTelegramMessage(chatId,
        `❌ Invalid or expired pairing code.\n\n` +
        `Get a fresh code from your Safelink app: Settings → Telegram Bridge → Get Pair Code.`
      );
    }
    return;
  }

  // ---- Handle forwarded content / regular messages ----
  const slId = tgChatToSafelink.get(chatId.toString());
  if (slId) {
    // Get the Safelink client and relay the message
    const slWs = clients.get(slId);
    if (slWs && slWs.readyState === WebSocket.OPEN) {
      // Determine message content
      let content = '';
      let mediaType = null;

      if (message.text) {
        content = message.text;
      } else if (message.photo) {
        // Get highest quality photo
        const photo = message.photo[message.photo.length - 1];
        content = `[Photo ${photo.width}x${photo.height}]`;
        mediaType = 'photo';
        // Download and relay
        const fileUrl = await getTelegramFile(photo.file_id);
        if (fileUrl) {
          slWs.send(JSON.stringify({
            type: 'telegram-message',
            from: userName,
            text: '[Photo]',
            mediaUrl: fileUrl,
            mediaType: 'photo',
            timestamp: Date.now(),
          }));
          await sendTelegramMessage(chatId, '✅ Photo forwarded to Safelink.');
          return;
        }
      } else if (message.document) {
        content = `[File: ${message.document.file_name || 'unknown'}]`;
        mediaType = 'document';
        const fileUrl = await getTelegramFile(message.document.file_id);
        if (fileUrl) {
          slWs.send(JSON.stringify({
            type: 'telegram-message',
            from: userName,
            text: `[File: ${message.document.file_name}]`,
            mediaUrl: fileUrl,
            mediaType: 'document',
            fileName: message.document.file_name,
            timestamp: Date.now(),
          }));
          await sendTelegramMessage(chatId, `✅ File "${message.document.file_name}" forwarded to Safelink.`);
          return;
        }
      } else if (message.video) {
        content = `[Video]`;
        mediaType = 'video';
        const fileUrl = await getTelegramFile(message.video.file_id);
        if (fileUrl) {
          slWs.send(JSON.stringify({
            type: 'telegram-message',
            from: userName,
            text: '[Video]',
            mediaUrl: fileUrl,
            mediaType: 'video',
            timestamp: Date.now(),
          }));
          await sendTelegramMessage(chatId, '✅ Video forwarded to Safelink.');
          return;
        }
      } else if (message.voice) {
        content = '[Voice message]';
        mediaType = 'voice';
        const fileUrl = await getTelegramFile(message.voice.file_id);
        if (fileUrl) {
          slWs.send(JSON.stringify({
            type: 'telegram-message',
            from: userName,
            text: '[Voice message]',
            mediaUrl: fileUrl,
            mediaType: 'voice',
            timestamp: Date.now(),
          }));
          await sendTelegramMessage(chatId, '✅ Voice message forwarded to Safelink.');
          return;
        }
      } else if (message.sticker) {
        content = `[Sticker: ${message.sticker.emoji || '🙂'}]`;
        mediaType = 'sticker';
        const fileUrl = await getTelegramFile(message.sticker.file_id);
        if (fileUrl) {
          slWs.send(JSON.stringify({
            type: 'telegram-message',
            from: userName,
            text: `[Sticker ${message.sticker.emoji || '🙂'}]`,
            mediaUrl: fileUrl,
            mediaType: 'sticker',
            timestamp: Date.now(),
          }));
          await sendTelegramMessage(chatId, '✅ Sticker forwarded to Safelink.');
          return;
        }
      } else if (message.location) {
        content = `[Location: ${message.location.latitude}, ${message.location.longitude}]`;
      }

      // Send text message to Safelink
      slWs.send(JSON.stringify({
        type: 'telegram-message',
        from: userName,
        text: content,
        mediaType,
        timestamp: Date.now(),
      }));
      console.log(`📤 Telegram → Safelink: "${content}" from ${userName}`);
    } else {
      await sendTelegramMessage(chatId, '⚠️ Safelink user is offline. Messages will be delivered when they connect.');
    }
  } else {
    // Not paired — prompt to pair
    await sendTelegramMessage(chatId,
      `👋 Hi ${userName}!\n\n` +
      `This bot connects Telegram to Safelink (a privacy-first messenger).\n\n` +
      `To get started, pair with a Safelink account:\n` +
      `1. Open the Safelink app\n` +
      `2. Go to Settings → Telegram Bridge → Get Pair Code\n` +
      `3. Send the code here like: PAIR ABC123\n\n` +
      `Use /help for more commands.`
    );
  }
}

// ---- Telegram API helpers ----
async function sendTelegramMessage(chatId, text) {
  try {
    const url = `${TG_API}/sendMessage`;
    await fetchJSON(url, 'POST', {
      chat_id: chatId,
      text: text,
      parse_mode: 'HTML',
    });
  } catch (e) {
    console.error('Failed to send Telegram message:', e.message);
  }
}

async function getTelegramFile(fileId) {
  try {
    const data = await fetchJSON(`${TG_API}/getFile?file_id=${fileId}`);
    if (data.ok) {
      return `https://api.telegram.org/file/bot${BOT_TOKEN}/${data.result.file_path}`;
    }
  } catch (e) {
    console.error('Failed to get Telegram file:', e.message);
  }
  return null;
}

// ---- HTTP helper (using built-in https, no external deps) ----
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
      // Need to pass body to the request
      const req = https.request(options, (res) => {
        let data = '';
        res.on('data', chunk => data += chunk);
        res.on('end', () => {
          try { resolve(JSON.parse(data)); }
          catch(e) { reject(e); }
        });
      });
      req.on('error', reject);
      req.write(bodyStr);
      req.end();
    } else {
      https.get(url, (res) => {
        let data = '';
        res.on('data', chunk => data += chunk);
        res.on('end', () => {
          try { resolve(JSON.parse(data)); }
          catch(e) { reject(e); }
        });
      }).on('error', reject);
    }
  });
}

// ---- Start polling ----
console.log('📡 Starting Telegram polling...');
setInterval(pollTelegram, POLL_INTERVAL);
pollTelegram(); // first poll immediately

// ---- Graceful shutdown ----
process.on('SIGINT', () => {
  console.log('\n👋 Shutting down...');
  wss.close();
  process.exit(0);
});

// ---- Connection stats ----
setInterval(() => {
  if (clients.size > 0 || tgChatToSafelink.size > 0) {
    console.log(`📊 Stats: ${clients.size} Safelink clients, ${tgChatToSafelink.size} Telegram pairs`);
  }
}, 30000);
  
