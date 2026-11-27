// Server trung gian: dịch vụ TikTok LIVE (JSON qua WebSocket) -> chuẩn hoá -> game
// Biến môi trường (đặt trên Railway → Variables):
//   UPSTREAM_KEY  : API key của dịch vụ nguồn (tik.tools). Có thể để trống nếu dịch vụ cho chạy ẩn danh.
//   UPSTREAM_URL  : (tuỳ chọn) địa chỉ WebSocket nguồn, mặc định wss://api.tik.tools
//   TOKEN         : (tuỳ chọn) mã bảo vệ, game phải gửi đúng mới được dùng
//   PORT          : cổng (Railway tự đặt)
// Cần Node 22+ (dùng WebSocket có sẵn của Node để nối tới nguồn).
import http from 'node:http';

/* ---------- chuẩn hoá sự kiện (thuần, không cần thư viện ngoài) ---------- */
export const num = (...v) => { for (const x of v) { const n = Number(x); if (Number.isFinite(n) && n > 0) return n } return 0 };
const isTrue = v => v === true || v === 1 || v === '1' || v === 'true';
export const imgOf = (d, g = {}) => {
  for (const x of [d?.giftPictureUrl, d?.gift_picture_url, d?.giftImage, d?.image, d?.giftIcon, g.giftPictureUrl, g.giftImage, g.image, g.icon]) {
    if (!x) continue;
    if (typeof x === 'string') { if (/^https?:/i.test(x)) return x.replace(/^http:/i, 'https:'); continue }
    const u = x.urlList || x.url_list || x.urls;
    if (Array.isArray(u) && u[0]) return String(u[0]).replace(/^http:/i, 'https:');
    if (typeof x.url === 'string') return x.url.replace(/^http:/i, 'https:');
  }
  return '';
};
export const userOf = d => {
  const u = d?.user ?? {};
  return u.nickname || u.uniqueId || u.unique_id || d?.nickname || d?.uniqueId || 'khách';
};

// Quà combo (streak) gửi nhiều sự kiện với repeatCount tăng dần: chỉ chuyển phần tăng thêm, tránh đếm trùng.
export function makeGiftNorm() {
  const last = new Map();
  return d => {
    const g = d?.giftDetails || d?.extendedGiftInfo || d?.gift || {};
    const name = d?.giftName || d?.gift_name || g.giftName || g.name || (d?.giftId != null ? `gift_${d.giftId}` : 'unknown');
    const user = userOf(d);
    const key = `${user}|${d?.giftId ?? name}`;
    const rc = Math.max(1, num(d?.repeatCount, d?.repeat_count, 1));
    const end = isTrue(d?.repeatEnd ?? d?.repeat_end);
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
    return { type: 'gift', user, name: String(name), id: d?.giftId, count: n, img: imgOf(d, g), diamonds: num(d?.diamondCount, d?.diamond_count, g.diamondCount, g.diamond_count) };
  };
}
export const normFollow = d => ({ type: 'follow', user: userOf(d) });
export const normShare = d => ({ type: 'share', user: userOf(d) });
export const normChat = d => ({ type: 'chat', user: userOf(d), text: String(d?.comment ?? d?.text ?? d?.content ?? '') });

// Mỗi phòng có 1 bộ chuẩn hoá riêng (nhớ tổng tim để tính phần tăng thêm nếu nguồn chỉ gửi tổng).
export function makeNormalizer() {
  const gift = makeGiftNorm();
  let lastTotal = 0;
  return function normalize(m) {
    const ev = String(m?.event ?? m?.type ?? m?.eventType ?? '').toLowerCase();
    const d = m?.data ?? m?.payload ?? m;
    if (ev.includes('gift')) return gift(d);
    if (ev.includes('like')) {
      const total = num(d?.totalLikeCount, d?.total_like_count, d?.totalLikes, d?.total);
      let count = num(d?.likeCount, d?.like_count, d?.count, d?.likes);
      if (!count && total) count = lastTotal && total > lastTotal ? total - lastTotal : 1;
      if (total) lastTotal = total;
      return { type: 'like', user: userOf(d), count: Math.max(1, Math.min(count || 1, 1000)), total };
    }
    if (ev.includes('follow')) return normFollow(d);
    if (ev.includes('share')) return normShare(d);
    if (ev.includes('chat') || ev.includes('comment')) return normChat(d);
    return undefined; // sự kiện không dùng
  };
}

/* ---------- 1 phòng = 1 kết nối tới nguồn, chia cho nhiều client ---------- */
export class Room {
  constructor(id, cfg, deps = {}) {
    this.id = id; this.cfg = cfg; this.WS = deps.WS || globalThis.WebSocket;
    this.clients = new Set(); this.up = null; this.connected = false;
    this.retryT = null; this.idleT = null; this.closed = false;
    this.norm = makeNormalizer(); this.seen = new Set();
  }
  send(obj) { const s = JSON.stringify(obj); for (const c of this.clients) if (c.readyState === 1) c.send(s) }
  status(msg) { console.log(`[${this.id}] ${msg}`); this.send({ type: 'status', msg }) }
  open() {
    if (this.up || this.closed) return;
    const url = this.cfg.upstream(this.id);
    let up;
    try { up = new this.WS(url) } catch (e) { this.status('Lỗi mở kết nối nguồn: ' + String(e?.message || e).slice(0, 150)); this.retry(); return }
    this.up = up;
    up.addEventListener('open', () => { this.connected = true; this.status(`Đã nối @${this.id}`) });
    up.addEventListener('message', ev => this.onMsg(ev.data));
    up.addEventListener('error', () => { this.status('Lỗi kết nối tới nguồn dữ liệu') });
    up.addEventListener('close', ev => {
      if (this.up !== up) return;
      this.up = null; this.connected = false;
      this.status(`Nguồn đóng kết nối (mã ${ev?.code ?? '?'}${ev?.reason ? ': ' + String(ev.reason).slice(0, 100) : ''})`);
      this.retry();
    });
  }
  retry() {
    if (this.closed || this.retryT || !this.clients.size) return;
    this.retryT = setTimeout(() => { this.retryT = null; this.open() }, 6000);
  }
  onMsg(data) {
    if (typeof data !== 'string') { if (!this.seen.has('#bin')) { this.seen.add('#bin'); this.status('Nguồn gửi dữ liệu nhị phân – server chưa đọc được') } return }
    let m; try { m = JSON.parse(data) } catch { return }
    for (const one of (Array.isArray(m) ? m : [m])) {
      let out;
      try { out = this.norm(one) } catch (e) { console.error('normalize', e); continue }
      if (out) { if (process.env.DEBUG) console.log(JSON.stringify(out)); this.send(out) }
      else if (out === undefined) {
        const ev = String(one?.event ?? one?.type ?? '?');
        if (!this.seen.has(ev)) { this.seen.add(ev); console.log(`[${this.id}] sự kiện bỏ qua: ${ev}`); }
      }
    }
  }
  add(ws) { this.clients.add(ws); clearTimeout(this.idleT); this.idleT = null; if (!this.up) this.open(); else this.send({ type: 'status', msg: this.connected ? `Đã nối @${this.id}` : `Đang nối @${this.id}…` }) }
  remove(ws, onEmpty) {
    this.clients.delete(ws);
    if (!this.clients.size) this.idleT = setTimeout(() => { if (!this.clients.size) { this.close(); onEmpty?.() } }, 60000);
  }
  close() { this.closed = true; clearTimeout(this.retryT); clearTimeout(this.idleT); try { this.up?.close() } catch {} this.up = null }
}

/* ---------- server ---------- */
export function makeConfig(env = process.env) {
  const BASE = env.UPSTREAM_URL || 'wss://api.tik.tools';
  const KEY = env.UPSTREAM_KEY || env.TIKTOOLS_API_KEY || '';
  return { upstream: id => { const u = new URL(BASE); u.searchParams.set('uniqueId', id); if (KEY) u.searchParams.set('apiKey', KEY); return u.toString() } };
}

async function start() {
  const { WebSocketServer } = await import('ws');
  process.on('uncaughtException', e => console.error('uncaughtException', e));
  process.on('unhandledRejection', e => console.error('unhandledRejection', e));
  const TOKEN = process.env.TOKEN || '';
  const PORT = Number(process.env.PORT) || 8080;
  const cfg = makeConfig();
  if (typeof globalThis.WebSocket !== 'function') console.error('⚠ Node quá cũ: cần Node 22+ (đặt "engines": {"node":"22.x"} trong package.json)');
  if (!process.env.UPSTREAM_KEY && !process.env.TIKTOOLS_API_KEY) console.warn('⚠ Chưa đặt UPSTREAM_KEY – sẽ thử chạy không có key.');

  const server = http.createServer((req, res) => { res.writeHead(200, { 'content-type': 'text/plain' }); res.end('relay ok'); });
  const wss = new WebSocketServer({ server });
  const rooms = new Map();

  wss.on('connection', (ws, req) => {
    const url = new URL(req.url, 'http://x');
    const id = (url.searchParams.get('uniqueId') || '').replace(/^@/, '').trim();
    console.log('client connected', id);
    if (TOKEN && url.searchParams.get('token') !== TOKEN) { ws.send(JSON.stringify({ type: 'status', msg: 'Sai mã bảo vệ' })); return ws.close(4401) }
    if (!id) { ws.send(JSON.stringify({ type: 'status', msg: 'Thiếu tên TikTok (uniqueId)' })); return ws.close(4400) }
    let room = rooms.get(id);
    if (!room) { room = new Room(id, cfg); rooms.set(id, room) }
    room.add(ws);
    ws.on('close', () => room.remove(ws, () => rooms.delete(id)));
    ws.on('error', () => {});
  });
  setInterval(() => { for (const c of wss.clients) { try { c.ping() } catch {} } }, 25000);
  server.listen(PORT, () => console.log('relay listening on', PORT));
}

if (process.env.NO_START !== '1') start().catch(e => { console.error(e); process.exit(1) });
