// TikTok LIVE -> Roblox bridge
// Reads TikTok LIVE comments and lets your Roblox game ask for them over HTTP.
//
// Endpoints (all need ?key=YOUR_BRIDGE_KEY):
//   GET /connect?user=tiktokname          start listening to that LIVE
//   GET /events?user=tiktokname&since=ID  new comments after ID
//   GET /disconnect?user=tiktokname       stop listening

const http = require('http');

const PORT = process.env.PORT || 3000;
const BRIDGE_KEY = process.env.BRIDGE_KEY || 'change-me';
const SIGN_API_KEY = process.env.SIGN_API_KEY || ''; // optional (Euler Stream key) if TikTok blocks connections
const MAX_EVENTS = 500;          // comments kept per channel
const IDLE_MS = 3 * 60 * 1000;   // stop a channel nobody polled for 3 minutes
const RETRY_MS = 20 * 1000;

let Lib = null;
async function loadLib() {
  if (Lib) return Lib;
  const mod = await import('tiktok-live-connector');
  const m = mod.TikTokLiveConnection || mod.WebcastPushConnection ? mod : (mod.default || mod);
  Lib = {
    Conn: m.TikTokLiveConnection || m.WebcastPushConnection,
    CHAT: (m.WebcastEvent && m.WebcastEvent.CHAT) || 'chat',
    STREAM_END: (m.WebcastEvent && m.WebcastEvent.STREAM_END) || 'streamEnd',
    GIFT: (m.WebcastEvent && m.WebcastEvent.GIFT) || 'gift',
    LIKE: (m.WebcastEvent && m.WebcastEvent.LIKE) || 'like',
  };
  return Lib;
}

const rooms = new Map(); // tiktok user -> room

function cleanUser(u) {
  return String(u || '').trim().replace(/^@/, '').toLowerCase();
}
function shortErr(e) {
  const msg = (e && (e.message || e.toString())) || 'unknown';
  if (/offline|not.*live|LIVE has ended/i.test(msg)) return 'offline';
  return msg.slice(0, 120);
}
function push(room, ev) {
  ev.id = room.nextId++;
  room.events.push(ev);
  if (room.events.length > MAX_EVENTS) room.events.splice(0, room.events.length - MAX_EVENTS);
}

async function startConn(user, room) {
  if (room.closed) return;
  let lib;
  try {
    lib = await loadLib();
  } catch (e) {
    room.status = 'error: tiktok-live-connector not installed (run npm install)';
    console.error(e.message);
    return;
  }
  const { Conn, CHAT, STREAM_END, GIFT, LIKE } = lib;
  const opts = { processInitialData: false };
  if (SIGN_API_KEY) opts.signApiKey = SIGN_API_KEY;
  const conn = new Conn(user, opts);
  room.conn = conn;
  room.status = 'connecting';

  conn.on(CHAT, (d) => {
    const u = d.user || {};
    push(room, {
      type: 'comment',
      user: u.uniqueId || d.uniqueId || '',
      nickname: u.nickname || d.nickname || '',
      text: d.comment || '',
    });
  });
  conn.on(GIFT, (d) => {
    const u = d.user || {};
    const det = d.giftDetails || {};
    const giftType = d.giftType != null ? d.giftType : det.giftType;
    // streakable gifts (type 1) fire repeatedly; only count them when the streak ends
    if (giftType === 1 && !d.repeatEnd) return;
    push(room, {
      type: 'gift',
      user: u.uniqueId || d.uniqueId || '',
      nickname: u.nickname || d.nickname || '',
      gift: d.giftName || det.giftName || 'Gift',
      count: d.repeatCount || 1,
      diamonds: d.diamondCount || det.diamondCount || 1,
    });
  });
  conn.on(LIKE, (d) => {
    const u = d.user || {};
    push(room, {
      type: 'like',
      user: u.uniqueId || d.uniqueId || '',
      nickname: u.nickname || d.nickname || '',
      count: d.likeCount || 1,
    });
  });
  conn.on(STREAM_END, () => { room.status = 'offline'; retry(user, room); });
  conn.on('disconnected', () => {
    if (room.status === 'connected') room.status = 'reconnecting';
    retry(user, room);
  });
  conn.on('error', (e) => console.error(`[${user}]`, shortErr(e)));

  try {
    await conn.connect();
    room.status = 'connected';
    console.log(`[${user}] connected`);
  } catch (e) {
    const err = shortErr(e);
    room.status = err === 'offline' ? 'offline' : 'error: ' + err;
    console.log(`[${user}] connect failed: ${err}`);
    retry(user, room);
  }
}

function retry(user, room) {
  if (room.closed || room.retryTimer) return;
  room.retryTimer = setTimeout(() => {
    room.retryTimer = null;
    try { room.conn && room.conn.disconnect(); } catch (_) {}
    startConn(user, room);
  }, RETRY_MS);
}

function stopRoom(user) {
  const room = rooms.get(user);
  if (!room) return;
  room.closed = true;
  clearTimeout(room.retryTimer);
  try { room.conn && room.conn.disconnect(); } catch (_) {}
  rooms.delete(user);
  console.log(`[${user}] stopped`);
}

function getRoom(user) {
  let room = rooms.get(user);
  if (!room) {
    room = { status: 'connecting', events: [], nextId: 1, lastPoll: Date.now(), conn: null, retryTimer: null, closed: false };
    rooms.set(user, room);
    startConn(user, room);
  }
  room.lastPoll = Date.now();
  return room;
}

setInterval(() => {
  const now = Date.now();
  for (const [user, room] of rooms) if (now - room.lastPoll > IDLE_MS) stopRoom(user);
}, 30 * 1000);

function send(res, code, obj) {
  res.writeHead(code, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(obj));
}

http.createServer((req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/') return send(res, 200, { ok: true, service: 'tiktok-roblox-bridge', channels: rooms.size });
  if (url.searchParams.get('key') !== BRIDGE_KEY) return send(res, 403, { error: 'Wrong BRIDGE_KEY' });

  const user = cleanUser(url.searchParams.get('user'));
  if (!/^[a-z0-9_.]{2,30}$/.test(user)) return send(res, 400, { error: 'Bad TikTok username' });

  if (url.pathname === '/connect') {
    const room = getRoom(user);
    return send(res, 200, { ok: true, status: room.status, last: room.nextId - 1 });
  }
  if (url.pathname === '/events') {
    const room = getRoom(user);
    // if the bridge restarted, the game's "since" can be ahead of us: clamp it
    const since = Math.min(Number(url.searchParams.get('since')) || 0, room.nextId - 1);
    const events = room.events.filter((e) => e.id > since).slice(0, 100);
    return send(res, 200, { status: room.status, events, last: events.length ? events[events.length - 1].id : since });
  }
  if (url.pathname === '/disconnect') {
    stopRoom(user);
    return send(res, 200, { ok: true });
  }
  send(res, 404, { error: 'Not found' });
}).listen(PORT, () => console.log(`Bridge running on port ${PORT}`));
