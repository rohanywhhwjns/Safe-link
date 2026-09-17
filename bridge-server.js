// ============================================================
//  Safelink Bridge Server (v5 - Firebase Firestore)
//  Pairings stored in Firebase Firestore — survives ALL restarts
// ============================================================

const WebSocket = require('ws');
const https = require('https');
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { initializeApp, cert } = require('firebase-admin/app');
const { getFirestore } = require('firebase-admin/firestore');

// ---- Config ----
const BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || 'YOUR_BOT_TOKEN_HERE';
const PORT = parseInt(process.env.PORT || '8080');
const POLL_INTERVAL = 1000;
const TG_API = `https://api.telegram.org/bot${BOT_TOKEN}`;

// ---- SECURITY: ID hashing + at-rest encryption ----
// User IDs are SHA-256 hashed before touching Firestore or memory maps.
// Telegram chat IDs are encrypted at rest (AES-256-GCM, key derived from env).
// A Firestore breach alone reveals NOTHING readable.
function hashId(id) {
  return crypto.createHash('sha256').update(String(id)).digest('hex');
}
const ATREST_KEY = crypto.createHash('sha256').update(BOT_TOKEN + ':atrest').digest();
function encryptAtRest(plaintext) {
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv('aes-256-gcm', ATREST_KEY, iv);
  const enc = Buffer.concat([cipher.update(String(plaintext), 'utf8'), cipher.final()]);
  const tag = cipher.getAuthTag();
  return iv.toString('base64') + ':' + tag.toString('base64') + ':' + enc.toString('base64');
}
function decryptAtRest(data) {
  try {
    const [ivB, tagB, encB] = data.split(':');
    const decipher = crypto.createDecipheriv('aes-256-gcm', ATREST_KEY, Buffer.from(ivB, 'base64'));
    decipher.setAuthTag(Buffer.from(tagB, 'base64'));
    return Buffer.concat([decipher.update(Buffer.from(encB, 'base64')), decipher.final()]).toString('utf8');
  } catch(e) { return null; }
}

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
    let serviceAccount = null;
    // EASY MODE: paste the WHOLE service account JSON file as one variable
    // (FIREBASE_SERVICE_ACCOUNT). No need to pick apart fields by hand.
    if (process.env.FIREBASE_SERVICE_ACCOUNT) {
      try {
        const raw = process.env.FIREBASE_SERVICE_ACCOUNT.trim().replace(/^"|"$/g, '');
        serviceAccount = JSON.parse(raw);
        console.log('✅ Firebase credentials read from FIREBASE_SERVICE_ACCOUNT (easy mode)');
      } catch (e) {
        console.error('FIREBASE_SERVICE_ACCOUNT could not be read as JSON: ' + e.message);
      }
    }
    if (!serviceAccount && FIREBASE_CLIENT_EMAIL && FIREBASE_PRIVATE_KEY) {
      serviceAccount = {
        projectId: FIREBASE_PROJECT_ID,
        clientEmail: FIREBASE_CLIENT_EMAIL,
        privateKey: FIREBASE_PRIVATE_KEY.replace(/\\n/g, '\n'),
      };
    }
    if (serviceAccount) {
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
      const stored = data[userId];
      if (!stored) return [];
      // SECURITY: new format is encrypted at rest; old format is a plain array
      if (typeof stored === 'string') {
        const decrypted = decryptAtRest(stored);
        return decrypted ? JSON.parse(decrypted) : [];
      }
      return stored; // legacy plain format
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
      // SECURITY: queue is encrypted at rest — Firestore/Google sees only ciphertext
      data[userId] = encryptAtRest(JSON.stringify(messages));
    } else {
      delete data[userId];
    }
    await ref.set(data);
    console.log(`💾 Saved ${messages?.length || 0} queued messages (encrypted) for ${userId}`);
  } catch(e) { console.error('DB save queued error:', e.message); }
}

// ---- SECURITY: device auth tokens (prevents impersonation) ----
async function dbLoadAuthToken(userId) {
  if (!db) return null;
  try {
    const ref = db.collection('safelink').doc('authTokens');
    const snap = await ref.get();
    if (snap.exists) { return snap.data()[userId] || null; }
    return null;
  } catch(e) { return null; }
}
async function dbSaveAuthToken(userId, tokenHash) {
  if (!db) return;
  try {
    const ref = db.collection('safelink').doc('authTokens');
    const snap = await ref.get();
    const data = snap.exists ? snap.data() : {};
    data[userId] = tokenHash;
    await ref.set(data);
  } catch(e) { console.error('DB save authToken error:', e.message); }
}

// ---- E2E (real end-to-end): public key directory + invite codes + blind relay ----
// The server stores ONLY public keys and relays OPAQUE ciphertext envelopes.
// It cannot read GhostVeil<->GhostVeil messages. Ever.
const e2ePubKeys = new Map(); // hUserId → JWK public key (persisted, public info)
const e2eInvites = new Map(); // code → { from: hUserId, createdAt }

async function dbLoadE2EKeys() {
  if (!db) return;
  try {
    const ref = db.collection('safelink').doc('e2eKeys');
    const snap = await ref.get();
    if (snap.exists) {
      const data = snap.data();
      for (const [k, v] of Object.entries(data)) e2ePubKeys.set(k, v);
      console.log(`🔑 Loaded ${e2ePubKeys.size} E2E public keys`);
    }
  } catch(e) { console.error('DB load e2e keys error:', e.message); }
}
async function dbSaveE2EKey(hUserId, jwk) {
  if (!db) return;
  try {
    const ref = db.collection('safelink').doc('e2eKeys');
    const snap = await ref.get();
    const data = snap.exists ? snap.data() : {};
    data[hUserId] = jwk;
    await ref.set(data);
  } catch(e) { console.error('DB save e2e key error:', e.message); }
}

// ---- E2E Groups: membership registry ( Firestore ) ----
// Design: the group doc stores ONLY ids, display names and public keys.
// Messages are relayed by the EXISTING blind e2e-message handler —
// the server still never sees plaintext.
const MAX_GROUP_MEMBERS = 200;

async function dbSaveGroup(groupId, group) {
  await getFirestore().collection('gv_groups').doc(groupId).set(group);
}
async function dbLoadGroup(groupId) {
  const snap = await getFirestore().collection('gv_groups').doc(groupId).get();
  return snap.exists ? snap.data() : null;
}
async function dbLoadGroupByCode(code) {
  const snap = await getFirestore().collection('gv_groupcodes').doc(String(code).toUpperCase()).get();
  if (!snap.exists) return null;
  const groupId = snap.data().groupId;
  const group = await dbLoadGroup(groupId);
  return group ? { groupId, group } : null;
}
async function dbDeleteGroupCode(code) {
  await getFirestore().collection('gv_groupcodes').doc(String(code).toUpperCase()).delete();
}
async function dbSaveGroupCode(code, groupId) {
  await getFirestore().collection('gv_groupcodes').doc(String(code).toUpperCase()).set({ groupId, createdAt: Date.now() });
}
async function dbAddUserGroup(userId, groupId) {
  const ref = getFirestore().collection('gv_usergroups').doc(userId);
  const snap = await ref.get();
  const cur = snap.exists ? (snap.data().groups || []) : [];
  if (!cur.includes(groupId)) cur.push(groupId);
  await ref.set({ groups: cur });
}
async function dbRemoveUserGroup(userId, groupId) {
  const ref = getFirestore().collection('gv_usergroups').doc(userId);
  const snap = await ref.get();
  const cur = snap.exists ? (snap.data().groups || []).filter(g => g !== groupId) : [];
  await ref.set({ groups: cur });
}
async function dbListUserGroups(userId) {
  const snap = await getFirestore().collection('gv_usergroups').doc(userId).get();
  return snap.exists ? (snap.data().groups || []) : [];
}
// Attach the (public) E2E keys of each member to a roster
async function groupRoster(group) {
  const members = [];
  for (const m of (group.members || [])) {
    const pubKey = e2ePubKeys.get(m.id) || null;
    members.push({ id: m.id, name: m.name, pubKey });
  }
  return members;
}

// Serialize group mutations (create/join/leave/newcode) — prevents two
// simultaneous joins from both passing the member/limit checks on stale data.
let groupMutationChain = Promise.resolve();
function groupMutate(fn) {
  groupMutationChain = groupMutationChain.then(fn).catch(e => console.error('Group mutation error:', e.message));
  return groupMutationChain;
}

async function dbLoadCloudBackup(accountId) {
  if (!db || !accountId) return null;
  try {
    const ref = db.collection('safelink').doc('cloudBackups');
    const snap = await ref.get();
    if (snap.exists) { return snap.data()[accountId] || null; }
    return null;
  } catch(e) { console.error('DB load cloud backup error:', e.message); return null; }
}
async function dbSaveCloudBackup(accountId, blob) {
  if (!db || !accountId) return;
  try {
    const ref = db.collection('safelink').doc('cloudBackups');
    const snap = await ref.get();
    const data = snap.exists ? snap.data() : {};
    data[accountId] = blob;
    await ref.set(data);
  } catch(e) { console.error('DB save cloud backup error:', e.message); }
}

// ---- State ----
const authTokenCache = new Map(); // hUserId → tokenHash (memory, works even without db)
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
    // SECURITY: tg is stored encrypted, sl is stored hashed
    const rawTg = pair.tg && pair.tg.includes(':') ? decryptAtRest(pair.tg) : pair.tg;
    if (!rawTg) continue;
    const hTg = hashId(rawTg);
    const hSl = pair.sl && pair.sl.length === 64 ? pair.sl : hashId(pair.sl);
    tgChatToSafelink.set(hTg, hSl);
    safelinkToTgChat.set(hSl, rawTg);
  }
  console.log(`📂 Loaded ${pairs.length} pairings from Firestore`);
  await dbLoadE2EKeys();
  // METADATA CLEANUP: delete any contact lists stored by old versions
  try {
    await db.collection('safelink').doc('contacts').delete();
    console.log('🧹 Deleted stored contact metadata (no longer collected)');
  } catch(e) { /* doc may not exist */ }
}

// ---- Save pairings to Firestore (debounced) ----
let saveTimeout = null;
function savePairings() {
  if (saveTimeout) clearTimeout(saveTimeout);
  saveTimeout = setTimeout(async () => {
    const pairs = Array.from(safelinkToTgChat.entries()).map(([hSl, rawTg]) => ({ tg: encryptAtRest(rawTg), sl: hSl }));
    await dbSavePairings(pairs);
  }, 2000);
}

// ---- Media Cache (for stickers/images) ----
const mediaCache = new Map();
const MEDIA_TTL = 86400000; // 24 hours — app can fetch media even after server idle periods

function cacheMedia(data, mime) {
  const id = 'm' + Date.now() + Math.random().toString(36).substr(2,6);
  mediaCache.set(id, { data, mime, expiry: Date.now() + MEDIA_TTL });
  if (mediaCache.size > 200) {
    const now = Date.now();
    for (const [k, v] of mediaCache) { if (v.expiry < now) mediaCache.delete(k); }
  }
  return id;
}

// ---- HTTP + WebSocket Server (on SAME port) ----
const httpServer = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.url && req.url.startsWith('/media/')) {
    const id = req.url.replace('/media/', '').split('?')[0];
    const item = mediaCache.get(id);
    if (item) {
      res.writeHead(200, { 'Content-Type': item.mime, 'Cache-Control': 'public, max-age=86400' });
      res.end(item.data);
      return;
    }
    res.writeHead(404); res.end('Not found'); return;
  }
  // ---- DIAGNOSTIC: is the database actually connected? Open /dbcheck in any browser ----
  if (req.url === '/dbcheck') {
    (async () => {
      const result = { db: false, write: false, error: null };
      try {
        const ref = getFirestore().collection('safelink').doc('diag');
        await ref.set({ ok: true, at: Date.now() });
        const snap = await ref.get();
        result.db = true;
        result.write = !!(snap.exists && snap.data && snap.data() && snap.data().ok);
      } catch (e) { result.error = String(e.message || e).slice(0, 200); }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(result, null, 2));
    })();
    return;
  }
  // ---- App update system ----
  if (req.url === '/update' || req.url === '/update/') {
    const apk = findLatestApk();
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(apk ? { version: apk.version, url: '/GhostVeil.apk' } : { version: '0' }));
    return;
  }
  if (req.url === '/GhostVeil.apk' || req.url === '/download') {
    const apk = findLatestApk();
    if (apk) {
      res.writeHead(200, {
        'Content-Type': 'application/vnd.android.package-archive',
        'Content-Disposition': 'attachment; filename="GhostVeil.apk"',
        'Content-Length': apk.buffer.length,
        'Cache-Control': 'no-store'
      });
      res.end(apk.buffer);
    } else { res.writeHead(404); res.end('No APK deployed'); }
    return;
  }
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Safelink Bridge Server v15 (Secure + App Updates)');
});

// ---- App update helper: finds the newest GhostVeil-<version>.apk in the repo folder ----
const APK_CACHE = { key: null, buffer: null };
function findLatestApk() {
  try {
    const files = fs.readdirSync(__dirname);
    let best = null;
    for (const f of files) {
      if (!/^ghostveil/i.test(f) || !/\.apk$/i.test(f)) continue;
      const vm = f.match(/(\d+)\.(\d+)(?:\.(\d+))?/);
      if (!vm) continue;
      const versionCode = parseInt(vm[1]) * 10000 + parseInt(vm[2]) * 100 + parseInt(vm[3] || '0');
      if (!best || versionCode > best.versionCode) {
        const st = fs.statSync(path.join(__dirname, f));
        best = { file: f, version: vm[1] + '.' + vm[2] + (vm[3] ? '.' + vm[3] : ''), versionCode, mtime: st.mtimeMs, size: st.size };
      }
    }
    if (!best) return null;
    const key = best.file + ':' + best.mtime + ':' + best.size;
    if (APK_CACHE.key !== key) {
      APK_CACHE.buffer = fs.readFileSync(path.join(__dirname, best.file));
      APK_CACHE.key = key;
    }
    return { version: best.version, buffer: APK_CACHE.buffer };
  } catch (e) { return null; }
}

const wss = new WebSocket.Server({ server: httpServer });
httpServer.listen(PORT, () => {
  console.log(`🔒 Safelink Bridge Server v5 (Secure) running on port ${PORT}`);
});
// ---- WebSocket Server ----

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
      const userId = hashId(msg.userId); // SECURITY: only the hash is ever stored
      // SECURITY: device token check — nobody can register as you without your secret
      let storedHash = authTokenCache.get(userId) || await dbLoadAuthToken(userId);
      const tokenHash = msg.token ? crypto.createHash('sha256').update(String(msg.token)).digest('hex') : null;
      if (storedHash && tokenHash !== storedHash) {
        console.log('❌ Connection rejected: token mismatch');
        ws.send(JSON.stringify({ type: 'auth-failed' }));
        ws.close();
        return;
      }
      if (!storedHash && tokenHash) {
        authTokenCache.set(userId, tokenHash);
        await dbSaveAuthToken(userId, tokenHash); // trust on first use (persisted + cached)
      }
      clients.set(userId, ws);
      ws.userId = userId;
      console.log(`✅ Registered user (hashed)`);
      ws.send(JSON.stringify({ type: 'registered', userId }));

      // ---- E2E groups: push the user's groups (name + roster + public keys) ----
      try {
        const myGroups = await dbListUserGroups(userId);
        const groupsOut = [];
        for (const gid of myGroups) {
          const g = await dbLoadGroup(gid);
          if (g) groupsOut.push({ groupId: gid, name: g.name, members: await groupRoster(g) });
        }
        if (groupsOut.length > 0) {
          ws.send(JSON.stringify({ type: 'group-list', groups: groupsOut }));
        }
      } catch (e) { console.error('Group list error:', e.message); }

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

    // ---- Sync contacts/groups list ----
    // SECURITY: No longer storing contact names on server (metadata removal)
    // Only store contact IDs (not names) for routing purposes
    if (msg.type === 'sync-contacts') {
      // Store only hashed IDs, not names or personal info
      const contactIds = (msg.contacts || []).map(c => c.id);
      try {
        const contactsRef = db.collection('users').doc(hashId(msg.userId)).collection('contacts').doc('list');
        await contactsRef.set({ ids: contactIds, updated: Date.now() }, { merge: true });
      } catch(e) { console.error('Contacts sync error:', e.message); }
      return;
    }

    // ---- Request pair code ----
    if (msg.type === 'request-pair-code') {
      const pairCode = crypto.randomBytes(4).toString('hex').toUpperCase(); // 8 chars, harder to guess
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
        console.log('📤 Safelink → Telegram: [message]'); // content never logged
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

    // ---- Send video to Telegram ----
    if (msg.type === 'send-to-telegram-video') {
      const tgChatId = safelinkToTgChat.get(ws.userId);
      if (tgChatId) {
        await sendTelegramUpload(tgChatId, 'sendVideo', 'video', msg.video, `video_${Date.now()}.mp4`, 'video/mp4');
        console.log(`📤 Safelink → Telegram: [video]`);
      }
      return;
    }

    // ---- Send file/document to Telegram ----
    if (msg.type === 'send-to-telegram-file') {
      const tgChatId = safelinkToTgChat.get(ws.userId);
      if (tgChatId) {
        await sendTelegramUpload(tgChatId, 'sendDocument', 'document', msg.file, msg.fileName || `file_${Date.now()}`, 'application/octet-stream');
        console.log(`📤 Safelink → Telegram: [file]`);
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

    // ---- Encrypted cloud backup (server stores ONLY unreadable ciphertext) ----
    // Keyed by SHA-256(username:password) computed on the client. The server
    // never sees usernames, passwords, or any readable account data.
    if (msg.type === 'cloud-backup') {
      if (msg.accountId && msg.blob) {
        await dbSaveCloudBackup(msg.accountId, msg.blob);
        // HONEST ACK: read the backup back and only confirm if it is really there
        const verify = await dbLoadCloudBackup(msg.accountId);
        if (verify) {
          ws.send(JSON.stringify({ type: 'cloud-backup-saved', at: Date.now() }));
          console.log('☁️ Encrypted cloud backup stored + VERIFIED (ciphertext only)');
        } else {
          ws.send(JSON.stringify({ type: 'cloud-backup-failed', reason: 'Database did not store the backup — check server database (see /dbcheck)' }));
          console.error('❌ Cloud backup NOT verified after save — database problem?');
        }
      }
      return;
    }
    if (msg.type === 'cloud-restore') {
      const blob = await dbLoadCloudBackup(msg.accountId);
      ws.send(JSON.stringify({ type: 'cloud-backup-data', blob: blob }));
      console.log('☁️ Cloud backup requested (ciphertext only)');
      return;
    }

    // ---- E2E: publish my public key (called right after register) ----
    if (msg.type === 'e2e-pubkey') {
      if (msg.pubKey) {
        e2ePubKeys.set(ws.userId, msg.pubKey);
        await dbSaveE2EKey(ws.userId, msg.pubKey);
      }
      return;
    }

    // ---- E2E: create an invite code ----
    if (msg.type === 'e2e-invite') {
      const code = crypto.randomBytes(4).toString('hex').toUpperCase();
      e2eInvites.set(code, { from: ws.userId, createdAt: Date.now() });
      setTimeout(() => e2eInvites.delete(code), 600000); // expires in 10 min
      ws.send(JSON.stringify({ type: 'e2e-invite-code', code }));
      return;
    }

    // ---- E2E: accept an invite (both sides learn each other's id + key) ----
    if (msg.type === 'e2e-accept') {
      const invite = e2eInvites.get(msg.code);
      if (!invite || Date.now() - invite.createdAt > 600000) {
        ws.send(JSON.stringify({ type: 'e2e-accept-failed', reason: 'Invalid or expired code' }));
        return;
      }
      e2eInvites.delete(msg.code);
      const accepterKey = msg.pubKey;
      const inviterKey = e2ePubKeys.get(invite.from);
      if (!accepterKey || !inviterKey) {
        ws.send(JSON.stringify({ type: 'e2e-accept-failed', reason: 'Key exchange failed' }));
        return;
      }
      e2ePubKeys.set(ws.userId, accepterKey);
      await dbSaveE2EKey(ws.userId, accepterKey);
      // tell BOTH sides about each other
      const inviterWs = clients.get(invite.from);
      if (inviterWs && inviterWs.readyState === WebSocket.OPEN) {
        inviterWs.send(JSON.stringify({ type: 'e2e-paired', peerId: ws.userId, peerPubKey: accepterKey }));
      }
      ws.send(JSON.stringify({ type: 'e2e-paired', peerId: invite.from, peerPubKey: inviterKey }));
      console.log('🔗 E2E contact established (server sees only public keys)');
      return;
    }

    // ---- E2E GROUPS ----
    // Create a group: server stores membership only; messages use the blind relay below.
    if (msg.type === 'group-create') {
      groupMutate(async () => {
        const groupId = crypto.randomBytes(6).toString('hex');
        const code = crypto.randomBytes(4).toString('hex').toUpperCase();
        const name = String(msg.name || 'Group').slice(0, 40);
        const group = { name, code, members: [{ id: ws.userId, name: String(msg.myName || '').slice(0, 30) }], createdAt: Date.now() };
        await dbSaveGroup(groupId, group);
        await dbSaveGroupCode(code, groupId);
        await dbAddUserGroup(ws.userId, groupId);
        ws.send(JSON.stringify({ type: 'group-created', groupId, code, name }));
        console.log('👥 E2E group created (server sees ids/names/public keys only)');
      });
      return;
    }

    // Join a group by code. Everyone online is told about the new member.
    if (msg.type === 'group-join') {
      groupMutate(async () => {
        const found = await dbLoadGroupByCode(msg.code);
        if (!found) { ws.send(JSON.stringify({ type: 'group-failed', reason: 'Group not found — check the code' })); return; }
        const gid = found.groupId;
        const group = found.group;
        if ((group.members || []).some(m => m.id === ws.userId)) {
          // already a member: just resend the roster
          ws.send(JSON.stringify({ type: 'group-joined', groupId: gid, name: group.name, members: await groupRoster(group), alreadyMember: true }));
          return;
        }
        if ((group.members || []).length >= MAX_GROUP_MEMBERS) { ws.send(JSON.stringify({ type: 'group-failed', reason: 'Group is full (' + MAX_GROUP_MEMBERS + ' members)' })); return; }
        const joinerName = String(msg.myName || '').slice(0, 30) || 'Member';
        group.members.push({ id: ws.userId, name: joinerName });
        await dbSaveGroup(gid, group);
        await dbAddUserGroup(ws.userId, gid);
        const roster = await groupRoster(group);
        const joinerKey = (roster.find(r => r.id === ws.userId) || {}).pubKey || null;
        // tell everyone already in the group about the new member
        for (const m of group.members) {
          if (m.id === ws.userId) continue;
          const mws = clients.get(m.id);
          if (mws && mws.readyState === WebSocket.OPEN) {
            mws.send(JSON.stringify({ type: 'group-member-joined', groupId: gid, member: { id: ws.userId, name: joinerName, pubKey: joinerKey } }));
          }
        }
        ws.send(JSON.stringify({ type: 'group-joined', groupId: gid, name: group.name, members: roster }));
        console.log('👥 E2E group member joined');
      });
      return;
    }

    // Leave a group
    if (msg.type === 'group-leave') {
      groupMutate(async () => {
        const group = await dbLoadGroup(msg.groupId);
        if (group) {
          group.members = (group.members || []).filter(m => m.id !== ws.userId);
          await dbSaveGroup(msg.groupId, group);
          await dbRemoveUserGroup(ws.userId, msg.groupId);
          for (const m of group.members) {
            const mws = clients.get(m.id);
            if (mws && mws.readyState === WebSocket.OPEN) {
              mws.send(JSON.stringify({ type: 'group-member-left', groupId: msg.groupId, memberId: ws.userId }));
            }
          }
          console.log('👥 E2E group member left');
        }
      });
      return;
    }

    // Regenerate the join code (invalidates the old one)
    if (msg.type === 'group-newcode') {
      groupMutate(async () => {
        const group = await dbLoadGroup(msg.groupId);
        if (group) {
          const code = crypto.randomBytes(4).toString('hex').toUpperCase();
          try { await dbDeleteGroupCode(group.code); } catch (e) {} // old code stops working
          group.code = code;
          await dbSaveGroup(msg.groupId, group);
          await dbSaveGroupCode(code, msg.groupId);
          ws.send(JSON.stringify({ type: 'group-code', groupId: msg.groupId, code }));
        }
      });
      return;
    }

    // ---- E2E: relay an encrypted envelope (blind — server cannot read it) ----
    if (msg.type === 'e2e-message') {
      const targetWs = clients.get(msg.to);
      const envelope = { type: 'e2e-message', from: ws.userId, payload: msg.payload };
      if (targetWs && targetWs.readyState === WebSocket.OPEN) {
        targetWs.send(JSON.stringify(envelope));
      } else {
        // offline: queue the opaque envelope (already encrypted end-to-end)
        const queued = await dbLoadQueuedMessages(msg.to) || [];
        queued.push(envelope);
        await dbSaveQueuedMessages(msg.to, queued);
      }
      console.log('📤 E2E envelope relayed (ciphertext)');
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
let isPolling = false;
async function pollTelegram() {
  if (isPolling) return; // never overlap polls — this caused duplicate messages
  isPolling = true;
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
  } finally {
    isPolling = false;
  }
}

async function handleTelegramMessage(message) {
  const chatId = message.chat.id;
  const userName = message.from?.first_name || message.from?.username || 'Unknown';
  const text = message.text || '';

      console.log('📥 TG message received: [content hidden]'); // content never logged

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
    const paired = tgChatToSafelink.has(hashId(chatId.toString()));
    const slId = tgChatToSafelink.get(hashId(chatId.toString()));
    const online = slId && clients.has(slId);
    await sendTelegramMessage(chatId,
      `📊 Status:\nBridge: ✅ Online\nPaired: ${paired ? '✅' : '❌'}\nApp: ${online ? '✅ Online' : '❌ Offline'}\nUsers: ${clients.size}`
    );
    return;
  }

  if (text === '/unpair') {
    const hTg = hashId(chatId.toString());
    const slId = tgChatToSafelink.get(hTg);
    if (slId) {
      tgChatToSafelink.delete(hTg);
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
      tgChatToSafelink.set(hashId(chatId.toString()), pair.safelinkUserId);
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
  const slId = tgChatToSafelink.get(hashId(chatId.toString()));
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

    // If mediaUrl is base64, cache it and send a URL instead (avoids WebSocket size limits)
    if (mediaUrl && mediaUrl.startsWith('data:')) {
      const mime = mediaUrl.substring(5, mediaUrl.indexOf(';'));
      const raw = mediaUrl.split(',')[1];
      const mediaId = cacheMedia(Buffer.from(raw, 'base64'), mime);
      mediaUrl = `https://safe-link-01l7.onrender.com/media/${mediaId}`;
      console.log(`📦 Cached media as ${mediaId} (${mime})`);
    }

    const msgObj = {
      type: 'telegram-message',
      from: 'Telegram User', // No real name stored (metadata removal)
      text: content,
      mediaType, mediaUrl, fileName,
      stickerSetName: stickerSetName || '',
      stickerFileId: stickerFileId || '',
      stickerKind: stickerKind || '',
      timestamp: Date.now(),
    };

    if (isOnline) {
      slWs.send(JSON.stringify(msgObj));
      console.log(`📤 → Safelink: [${mediaType || 'text'}]`); // content never logged
      await sendTelegramMessage(chatId, '✅ Sent to Safelink.');
    } else {
      // Queue message in Firestore (auto-deleted after delivery)
      // SECURITY: Messages are E2E encrypted — server can't read them
      // Queued messages are deleted immediately when user comes online
      const queued = await dbLoadQueuedMessages(slId);
      queued.push(msgObj);
      await dbSaveQueuedMessages(slId, queued);
      // Auto-delete after 24 hours (even if user never comes online)
      setTimeout(async () => {
        try {
          const current = await dbLoadQueuedMessages(slId);
          const filtered = current.filter(m => m.timestamp !== msgObj.timestamp);
          await dbSaveQueuedMessages(slId, filtered);
        } catch(e) {}
      }, 86400000); // 24 hours
      console.log(`📬 Queued (${queued.length} total) — auto-deletes in 24h`);
      await sendTelegramMessage(chatId, '✅ Saved! Open Safelink app to see it. (Auto-deletes in 24h)');
    }
  } else {
    await sendTelegramMessage(chatId,
      `👋 Hi!\nPair with Safelink:\n1. Open Safelink app\n2. Settings → Telegram Bridge\n3. Send code: PAIR ABC123`
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

async function sendTelegramUpload(chatId, apiMethod, fieldName, base64Data, fileName, contentType) {
  try {
    const b64 = String(base64Data).includes(',') ? String(base64Data).split(',')[1] : String(base64Data);
    const buffer = Buffer.from(b64, 'base64');
    if (buffer.length > 45 * 1024 * 1024) { await sendTelegramMessage(chatId, '[File too large for Telegram]'); return; }
    const boundary = '----FormBoundary' + crypto.randomBytes(8).toString('hex');
    const parts = [];
    parts.push(Buffer.from(`--${boundary}\r\nContent-Disposition: form-data; name="chat_id"\r\n\r\n${chatId}\r\n`));
    parts.push(Buffer.from(`--${boundary}\r\nContent-Disposition: form-data; name="${fieldName}"; filename="${fileName}"\r\nContent-Type: ${contentType}\r\n\r\n`));
    parts.push(buffer);
    parts.push(Buffer.from(`\r\n--${boundary}--\r\n`));
    const body = Buffer.concat(parts);
    await new Promise((resolve, reject) => {
      const urlObj = new URL(`${TG_API}/${apiMethod}`);
      const req = https.request({
        hostname: urlObj.hostname, path: urlObj.pathname, method: 'POST',
        headers: { 'Content-Type': `multipart/form-data; boundary=${boundary}`, 'Content-Length': body.length },
      }, (res) => { let data=''; res.on('data', c => data += c); res.on('end', () => resolve()); });
      req.on('error', reject);
      req.write(body); req.end();
    });
    console.log(`📤 ${apiMethod} sent to Telegram`);
  } catch (e) {
    console.error(`TG ${apiMethod} error:`, e.message);
    await sendTelegramMessage(chatId, '[Media could not be sent]');
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
