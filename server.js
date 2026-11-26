// Server trung gian: TikTok LIVE (qua Euler Stream) -> JSON qua WebSocket -> game
// Biến môi trường:
//   EULER_API_KEY : API key lấy ở eulerstream.com (nên có để không bị giới hạn)
//   TOKEN         : mã bảo vệ tuỳ chọn, game phải gửi đúng mới được dùng (tránh người lạ xài ké key của ông)
//   PORT          : cổng (Render tự đặt)
import http from 'node:http';

/* ---------- chuẩn hoá sự kiện (thuần, không cần thư viện ngoài) ---------- */
export const num = (...v) => { for (const x of v) { const n = Number(x); if (Number.isFinite(n) && n > 0) return n } return 0 };
const isTrue = v => v === true || v === 1 || v === '1' || v === 'true';
export const userOf = d => {
  const u = d?.user ?? {};
  return u.uniqueId || u.nickname || d?.uniqueId || d?.nickname || 'khách';
};

// Quà combo (streak) gửi nhiều sự kiện với repeatCount tăng dần: chỉ chuyển phần tăng thêm, tránh đếm trùng.
export function makeGiftNorm() {
  const last = new Map();
  return d => {
    const g = d?.giftDetails || d?.extendedGiftInfo || d?.gift || {};
    const name = d?.giftName || g.giftName || g.name || (d?.giftId != null ? `gift_${d.giftId}` : 'unknown');
    const user = userOf(d);
    const key = `${user}|${d?.giftId ?? name}`;
    const rc = Math.max(1, num(d?.repeatCount, d?.repeat_count, 1));
    const end = isTrue(d?.repeatEnd);
    const now = Date.now();
    const prev = last.get(key);
    let n = rc;
    if (prev && now - prev.t < 10000) {
      if (rc > prev.rc) n = rc - prev.rc;
      else if (rc === prev.rc && end) n = 0;
    }
    if (end) last.delete(key); else last.set(key, { rc, t: now });
    if (last.size > 500) for (const k of last.keys()) { last.delete(k); if (last.size < 250) break }
    if (n <= 0) return null;
    return { type: 'gift', user, name: String(name), id: d?.giftId, count: n, diamonds: num(d?.diamondCount, g.diamondCount, g.diamond_count) };
  };
}
export const normLike = d => ({ type: 'like', user: userOf(d), count: Math.max(1, num(d?.likeCount, d?.count, 1)), total: num(d?.totalLikeCount, d?.total) });
export const normFollow = d => ({ type: 'follow', user: userOf(d) });
export const normShare = d => ({ type: 'share', user: userOf(d) });
export const normChat = d => ({ type: 'chat', user: userOf(d), text: String(d?.comment ?? d?.text ?? '') });

/* ---------- server ---------- */
async function start() {
  const { WebSocketServer } = await import('ws');
  const { TikTokLiveConnection, WebcastEvent } = await import('tiktok-live-connector');
  const KEY = process.env.EULER_API_KEY || '';
  const TOKEN = process.env.TOKEN || '';
  const PORT = Number(process.env.PORT) || 8080;
  if (!KEY) console.warn('⚠ Chưa đặt EULER_API_KEY – vẫn chạy nhưng dễ bị giới hạn.');

  const server = http.createServer((req, res) => { res.writeHead(200, { 'content-type': 'text/plain' }); res.end('relay ok'); });
  const wss = new WebSocketServer({ server });
  const rooms = new Map();
  const send = (room, obj) => { const s = JSON.stringify(obj); for (const c of room.clients) if (c.readyState === 1) c.send(s); };

  function connectRoom(room) {
    if (room.connecting || room.connected) return;
    room.connecting = true;
    const conn = new TikTokLiveConnection(room.id, KEY ? { signApiKey: KEY, enableExtendedGiftInfo: true } : { enableExtendedGiftInfo: true });
    room.conn = conn;
    const fwd = (ev, fn) => conn.on(ev, d => {
      try { const m = fn(d); if (m) { if (process.env.DEBUG) console.log(JSON.stringify(m)); send(room, m) } } catch (e) { console.error(e) }
    });
    fwd(WebcastEvent.GIFT, room.norm);
    fwd(WebcastEvent.LIKE, normLike);
    fwd(WebcastEvent.FOLLOW, normFollow);
    fwd(WebcastEvent.SHARE, normShare);
    fwd(WebcastEvent.CHAT, normChat);
    const lost = () => { room.connected = false; room.connecting = false; send(room, { type: 'status', msg: 'Mất kết nối TikTok, đang nối lại…' }) };
    conn.on('disconnected', lost);
    conn.on('streamEnd', lost);
    conn.connect().then(state => {
      room.connected = true; room.connecting = false;
      console.log('connected', room.id, state?.roomId);
      send(room, { type: 'status', msg: `Đã nối @${room.id}` });
    }).catch(err => {
      room.connected = false; room.connecting = false;
      const msg = String(err?.message || err);
      console.warn('connect fail', room.id, msg);
      send(room, { type: 'status', msg: `Chưa nối được @${room.id} (đang live chưa?): ${msg.slice(0, 120)}` });
    });
  }

  wss.on('connection', (ws, req) => {
    const url = new URL(req.url, 'http://x');
    const id = (url.searchParams.get('uniqueId') || '').replace(/^@/, '').trim();
    if (TOKEN && url.searchParams.get('token') !== TOKEN) { ws.send(JSON.stringify({ type: 'status', msg: 'Sai mã bảo vệ' })); return ws.close(4401) }
    if (!id) { ws.send(JSON.stringify({ type: 'status', msg: 'Thiếu tên TikTok (uniqueId)' })); return ws.close(4400) }
    let room = rooms.get(id);
    if (!room) { room = { id, clients: new Set(), connected: false, connecting: false, conn: null, idleSince: 0, norm: makeGiftNorm() }; rooms.set(id, room); connectRoom(room) }
    room.clients.add(ws); room.idleSince = 0;
    ws.send(JSON.stringify({ type: 'status', msg: room.connected ? `Đã nối @${id}` : `Đang nối @${id}…` }));
    ws.on('close', () => { room.clients.delete(ws); if (!room.clients.size) room.idleSince = Date.now() });
    ws.on('error', () => {});
  });

  setInterval(() => { for (const c of wss.clients) { try { c.ping() } catch {} } }, 25000);
  setInterval(() => {
    for (const [id, room] of rooms) {
      if (!room.clients.size && room.idleSince && Date.now() - room.idleSince > 60000) { try { room.conn?.disconnect() } catch {} rooms.delete(id); continue }
      if (room.clients.size && !room.connected && !room.connecting) connectRoom(room);
    }
  }, 20000);

  server.listen(PORT, () => console.log('relay listening on', PORT));
}

if (process.env.NO_START !== '1') start().catch(e => { console.error(e); process.exit(1) });
