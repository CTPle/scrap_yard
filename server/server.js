// 잔해 구역 멀티플레이 서버
// - 방 만들기 / 참가 / 목록, 방 하나에 최대 12명
// - 플레이어 위치를 1초에 15번 묶어서 보냄 (멀리 있는 플레이어는 덜 자주)
// - AI 적은 방장 컴퓨터가 계산하고, 서버는 다른 사람에게 전달만 함
// - 사격, 피해, 쓰러짐, 부활, 탈출 같은 사건 전달
// - 이번 달 보낸 데이터 양 기록 (주소 뒤에 /stats 를 붙이면 확인 가능)
'use strict';
const http = require('node:http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
// 접속을 허용할 사이트. 비워 두면 GitHub Pages(ctple.github.io)와 내 컴퓨터(localhost)만 허용
const ORIGINS = (process.env.ALLOWED_ORIGINS || 'https://ctple.github.io')
  .split(',').map(s => s.trim()).filter(Boolean);

const MAX_ROOM = 12;            // 방 하나 최대 인원
const MAX_ROOMS = 30;           // 서버 전체 최대 방 개수
const TICK = 1000 / 15;         // 위치 전송 간격 (1초에 15번)
const FAR = 150;                // 이 거리(m)보다 멀면 3번에 1번만 보냄
const MAX_MSG = 16 * 1024;      // 메시지 하나 최대 크기 (AI 적 정보 포함)
const RATE = 120;               // 1초에 받을 수 있는 최대 메시지 수
const MAPS = ['lab', 'canyon'];
const MODES = ['normal', 'coop', 'team'];
const NICK_RE = /^[가-힣A-Za-z0-9_]{2,12}$/;
const MAX_DMG = 400;            // 한 번에 줄 수 있는 최대 피해 (비정상 값 차단)

// ---------- 데이터 사용량 기록 ----------
const usage = { month: '', bytes: 0, peak: 0 };
function monthKey() { const d = new Date(); return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}`; }
function countBytes(n) {
  const m = monthKey();
  if (usage.month !== m) { usage.month = m; usage.bytes = 0; }
  usage.bytes += n + 6; // 6 = WebSocket 머리 부분 대략치
}

// ---------- 방과 플레이어 ----------
const rooms = new Map();   // code -> room
let nextId = 1;

function makeCode() {
  const A = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  for (;;) {
    let c = '';
    for (let i = 0; i < 4; i++) c += A[Math.floor(Math.random() * A.length)];
    if (!rooms.has(c)) return c;
  }
}

function send(c, obj) {
  if (c.ws.readyState !== 1) return;
  const s = JSON.stringify(obj);
  c.ws.send(s);
  countBytes(Buffer.byteLength(s));
}
function sendRaw(c, s) {
  if (c.ws.readyState !== 1) return;
  c.ws.send(s);
  countBytes(Buffer.byteLength(s));
}
function broadcast(room, obj, except) {
  const s = JSON.stringify(obj);
  for (const c of room.players.values()) if (c !== except) sendRaw(c, s);
}

function roomInfo(room) {
  return {
    code: room.code, map: room.map, mode: room.mode, priv: room.priv, host: room.host, started: room.started,
    players: [...room.players.values()].map(c => ({ id: c.id, nick: c.nick, team: c.team, ready: c.ready, ver: c.ver || '' })),
  };
}
function sendRoom(room) { broadcast(room, { t: 'room', room: roomInfo(room) }); }

// 팀전: 팀을 고르지 않았으면 인원이 적은 팀에 넣음
function autoTeam(room) {
  let a = 0, b = 0;
  for (const c of room.players.values()) { if (c.team === 1) a++; else if (c.team === 2) b++; }
  return a <= b ? 1 : 2;
}

// AI 적 계산 담당이 출격에서 빠지면 아직 출격 중인 다른 사람에게 넘김
function passAI(room, c) {
  if (room.ai !== c.id) return;
  const next = [...room.players.values()].find(p => p !== c && p.inRaid);
  room.ai = next ? next.id : 0;
  if (next) broadcast(room, { t: 'aihost', id: room.ai });
}
function leaveRoom(c) {
  const room = c.room;
  if (!room) return;
  room.players.delete(c.id);
  c.room = null; c.state = null;
  if (!room.players.size) { rooms.delete(room.code); return; }
  // 방장이 나가면 다음 사람이 방장 (AI 적 계산도 넘겨받음)
  if (room.host === c.id) room.host = room.players.keys().next().value;
  passAI(room, c);
  if (room.started && ![...room.players.values()].some(p => p.inRaid)) room.started = false;
  broadcast(room, { t: 'left', id: c.id, host: room.host });
  sendRoom(room);
}

// ---------- 메시지 처리 ----------
function onMessage(c, msg) {
  const t = msg.t;

  if (t === 'hello') {
    const nick = typeof msg.nick === 'string' ? msg.nick.trim() : '';
    if (!NICK_RE.test(nick)) return send(c, { t: 'err', m: '닉네임은 2~12자의 한글, 영문, 숫자, _ 만 쓸 수 있습니다' });
    c.nick = nick;
    c.ver = typeof msg.ver === 'string' ? msg.ver.slice(0, 20) : '';
    if (c.room) sendRoom(c.room);
    return send(c, { t: 'welcome', id: c.id, now: Date.now() }); // now: 세션 시계를 맞추는 서버 시간
  }
  if (!c.nick) return send(c, { t: 'err', m: '먼저 닉네임을 보내야 합니다' });

  if (t === 'list') {
    // 비밀방은 목록에 보이지 않음 (코드로만 입장)
    const list = [...rooms.values()].filter(r => !r.started && !r.priv).map(r => ({
      code: r.code, map: r.map, mode: r.mode, n: r.players.size, max: MAX_ROOM,
    }));
    return send(c, { t: 'list', rooms: list });
  }

  if (t === 'create') {
    if (c.room) leaveRoom(c);
    if (rooms.size >= MAX_ROOMS) return send(c, { t: 'err', m: '서버에 파티가 너무 많습니다. 잠시 뒤 다시 시도해 주십시오' });
    if (!MAPS.includes(msg.map) || !MODES.includes(msg.mode)) return send(c, { t: 'err', m: '맵 또는 모드가 올바르지 않습니다' });
    if (msg.mode === 'team' && msg.map !== 'canyon') return send(c, { t: 'err', m: '팀전은 협곡에서만 할 수 있습니다' });
    const room = { code: makeCode(), map: msg.map, mode: msg.mode, priv: !!msg.priv, host: c.id, started: false, players: new Map(), tick: 0, seed: 0 };
    rooms.set(room.code, room);
    joinRoom(c, room, msg.team);
    return;
  }

  if (t === 'join') {
    const room = rooms.get(String(msg.code || '').toUpperCase());
    if (!room) return send(c, { t: 'err', m: '파티를 찾을 수 없습니다' });
    if (room.started) return send(c, { t: 'err', m: '이미 출격한 파티입니다' });
    if (room.players.size >= MAX_ROOM) return send(c, { t: 'err', m: `파티가 가득 찼습니다 (최대 ${MAX_ROOM}명)` });
    for (const o of room.players.values()) if (o.nick === c.nick) return send(c, { t: 'err', m: '같은 닉네임이 이미 파티에 있습니다' });
    if (c.room) leaveRoom(c);
    joinRoom(c, room, msg.team);
    return;
  }

  const room = c.room;
  if (!room) return;

  if (t === 'leave') return leaveRoom(c);

  if (t === 'team') {
    if (room.mode !== 'team' || room.started) return;
    if (msg.team !== 1 && msg.team !== 2) return;
    c.team = msg.team;
    return sendRoom(room);
  }

  if (t === 'ready') { c.ready = !!msg.v; return sendRoom(room); }

  // 출격이 끝나 은신처로 돌아옴. 모두 돌아오면 방이 다시 대기 상태가 됨
  if (t === 'lobby') {
    c.inRaid = false; c.state = null; c.down = false;
    passAI(room, c);
    if (room.started && ![...room.players.values()].some(p => p.inRaid)) room.started = false;
    return sendRoom(room);
  }

  if (t === 'start') {
    if (room.host !== c.id || room.started) return;
    room.started = true;
    room.seed = Math.floor(Math.random() * 2 ** 31);
    // 출발 위치는 게임 쪽에서 seed 로 섞어 정함. 서버는 플레이어마다 순번만 정해 줌
    const order = {};
    let i = 0;
    for (const p of room.players.values()) { order[p.id] = i++; p.inRaid = true; }
    room.ai = room.host; // AI 적 계산은 처음엔 방장이 맡음
    return broadcast(room, { t: 'start', seed: room.seed, order, ai: room.ai, room: roomInfo(room) });
  }

  if (!room.started) return;

  // 내 상태: [x, y, z, yaw, pitch, flags, weapon, hp]
  if (t === 's') {
    const p = msg.p;
    if (!Array.isArray(p) || p.length > 10 || !p.every(v => typeof v === 'number' && Number.isFinite(v))) return;
    c.state = p;
    return;
  }

  // AI 적 정보: AI 담당만 보낼 수 있고, 서버는 그대로 전달
  if (t === 'ai') {
    if (room.ai !== c.id || !msg.d || typeof msg.d !== 'object') return;
    return broadcast(room, { t: 'ai', d: msg.d }, c);
  }

  // 사건
  if (t === 'e') {
    const k = msg.k;
    if (k === 'hit') {
      // 다른 플레이어에게 피해 (팀킬 포함). 맞은 사람에게만 보냄
      const target = room.players.get(msg.to);
      const dmg = Number(msg.dmg);
      if (!target || !(dmg > 0) || dmg > MAX_DMG) return;
      return send(target, { t: 'e', k: 'hit', from: c.id, dmg, head: !!msg.head });
    }
    if (k === 'ehit') {
      // AI 적에게 준 피해 묶음 [[적 번호, 피해], ...]. AI 담당에게만 보냄
      const ai = room.players.get(room.ai);
      if (!ai || ai === c || !Array.isArray(msg.h) || msg.h.length > 60) return;
      const h = msg.h.filter(a => Array.isArray(a) && Number.isFinite(a[0]) && Number(a[1]) > 0 && Number(a[1]) <= 20000).map(a => [a[0] | 0, Number(a[1])]);
      if (h.length) send(ai, { t: 'e', k: 'ehit', from: c.id, h });
      return;
    }
    if (k === 'revive') {
      const target = room.players.get(msg.to);
      if (!target || !target.down) return;
      target.down = false;
      return broadcast(room, { t: 'e', k: 'revive', from: c.id, to: target.id });
    }
    if (k === 'down') c.down = true;
    if (k === 'up') c.down = false;
    if (k === 'dead' || k === 'extract') { c.down = false; c.inRaid = false; c.state = null; passAI(room, c); }
    // 그 밖의 사건(사격, 쓰러짐, 사망, 탈출, 아이템 줍기, 폭발 등)은 다른 사람에게 그대로 전달
    const ALLOWED = ['shot', 'down', 'up', 'dead', 'extract', 'loot', 'boom', 'door', 'say'];
    if (!ALLOWED.includes(k)) return;
    const out = { t: 'e', k, from: c.id };
    if (msg.d !== undefined) out.d = msg.d;
    return broadcast(room, out, c);
  }
}

function joinRoom(c, room, team) {
  c.room = room; c.ready = false; c.down = false; c.state = null;
  c.team = room.mode === 'team' ? (team === 1 || team === 2 ? team : autoTeam(room)) : 0;
  room.players.set(c.id, c);
  send(c, { t: 'joined', code: room.code });
  sendRoom(room);
}

// ---------- 위치 묶어서 보내기 ----------
setInterval(() => {
  for (const room of rooms.values()) {
    if (!room.started) continue;
    room.tick++;
    const list = [...room.players.values()].filter(c => c.state);
    for (const me of room.players.values()) {
      const out = [];
      for (const o of list) {
        if (o === me) continue;
        if (me.state) {
          const dx = o.state[0] - me.state[0], dz = o.state[2] - me.state[2];
          if (dx * dx + dz * dz > FAR * FAR && room.tick % 3) continue;
        }
        out.push([o.id, ...o.state.map(v => Math.round(v * 100) / 100)]);
      }
      if (out.length) sendRaw(me, JSON.stringify({ t: 'S', p: out }));
    }
  }
}, TICK);

// ---------- HTTP 와 WebSocket ----------
const server = http.createServer((req, res) => {
  if (req.url === '/stats') {
    const players = [...rooms.values()].reduce((n, r) => n + r.players.size, 0);
    res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' });
    res.end(`이번 달(${usage.month || monthKey()}) 보낸 데이터: 약 ${(usage.bytes / 1048576).toFixed(1)}MB\n` +
      `(서버가 다시 켜지면 0부터 다시 셉니다. 정확한 값은 Render 대시보드에서 확인)\n` +
      `방 ${rooms.size}개, 접속 ${players}명, 최대 동시 접속 ${usage.peak}명\n`);
    return;
  }
  res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' });
  res.end('잔해 구역 서버가 켜져 있습니다');
});

const wss = new WebSocketServer({
  server,
  maxPayload: MAX_MSG,
  perMessageDeflate: false, // 압축 끔
  verifyClient: ({ origin }) => {
    if (!origin) return false;
    if (/^https?:\/\/(localhost|127\.0\.0\.1)(:\d+)?$/.test(origin)) return true;
    return ORIGINS.includes(origin);
  },
});

wss.on('connection', ws => {
  const c = { id: nextId++, ws, nick: '', room: null, team: 0, ready: false, down: false, state: null, msgs: 0, msgT: Date.now() };
  usage.peak = Math.max(usage.peak, wss.clients.size);
  ws.on('pong', () => { ws._dead = false; });
  ws.on('message', (data, isBinary) => {
    // 너무 자주 보내면 끊음
    const now = Date.now();
    if (now - c.msgT > 1000) { c.msgT = now; c.msgs = 0; }
    if (++c.msgs > RATE) { ws.close(1008, 'too many messages'); return; }
    if (isBinary) return;
    let msg;
    try { msg = JSON.parse(data.toString()); } catch (_) { return; }
    if (!msg || typeof msg !== 'object' || typeof msg.t !== 'string') return;
    try { onMessage(c, msg); } catch (err) { console.error('message error', err); }
  });
  ws.on('close', () => leaveRoom(c));
  ws.on('error', () => {});
});

// 응답 없는 접속 정리 (30초마다 확인)
setInterval(() => {
  for (const ws of wss.clients) {
    if (ws._dead) { ws.terminate(); continue; }
    ws._dead = true;
    ws.ping();
  }
}, 30000);

server.listen(PORT, () => console.log(`잔해 구역 서버 시작: 포트 ${PORT}`));
