// 잔해 구역 멀티플레이 서버
// - 파티 꾸리기 / 참가 / 모집 목록, 파티 하나에 최대 6명
// - 판(match): 같은 맵, 같은 50분 세션에 출격한 협동전 파티들을 최대 12명까지 한 판으로 묶음
//   같은 판의 다른 파티와 마주치고 서로 싸울 수 있음
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

const MAX_ROOM = 6;             // 파티 하나 최대 인원
const MATCH_MAX = 12;           // 한 판 최대 인원
const SESSION_MS = 3000 * 1000; // 세션 길이 50분 (게임과 같아야 함)
const SPAWNS = { lab: 4, canyon: 2 }; // 맵별 출발 위치 개수 (파티마다 다른 위치를 주는 데 씀)
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

// ---------- 파티, 판, 플레이어 ----------
const rooms = new Map();   // code -> 파티
const matches = new Set(); // 진행 중인 판
let nextId = 1;

function matchBroadcast(m, obj, except) {
  const s = JSON.stringify(obj);
  for (const c of m.players) if (c !== except) sendRaw(c, s);
}
// 파티가 출격할 판 찾기: 같은 맵, 같은 세션, 자리가 남은 판. 없으면 새로 만듦
function findMatch(room) {
  const sidx = Math.floor(Date.now() / SESSION_MS), n = room.players.size;
  for (const m of matches) if (m.map === room.map && m.sidx === sidx && m.players.size + n <= MATCH_MAX) return m;
  const m = { map: room.map, sidx, seed: Math.floor(Math.random() * 2 ** 31), players: new Set(), ai: 0, slot: 0, tick: 0 };
  matches.add(m);
  return m;
}
// 판에서 빠짐 (탈출, 사망, 은신처 복귀, 접속 끊김)
function leaveMatch(c) {
  const m = c.match;
  if (!m) return;
  m.players.delete(c); c.match = null; c.inRaid = false; c.state = null; c.down = false;
  if (!m.players.size) { matches.delete(m); return; }
  if (m.ai === c.id) {
    // AI 적 계산 담당이 빠지면 판에 남은 다른 사람에게 넘김
    const next = m.players.values().next().value;
    m.ai = next.id;
    matchBroadcast(m, { t: 'aihost', id: m.ai });
  }
  matchBroadcast(m, { t: 'gone', id: c.id });
}

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

function leaveRoom(c) {
  leaveMatch(c);
  const room = c.room;
  if (!room) return;
  room.players.delete(c.id);
  c.room = null; c.state = null;
  if (!room.players.size) { rooms.delete(room.code); return; }
  // 파티장이 나가면 다음 사람이 파티장
  if (room.host === c.id) room.host = room.players.keys().next().value;
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

  // 출격이 끝나 은신처로 돌아옴. 모두 돌아오면 파티가 다시 대기 상태가 됨
  if (t === 'lobby') {
    leaveMatch(c);
    if (room.started && ![...room.players.values()].some(p => p.inRaid)) room.started = false;
    return sendRoom(room);
  }

  if (t === 'start') {
    if (room.host !== c.id || room.started) return;
    room.started = true;
    // 같은 세션의 판에 들어감. 파티마다 다른 출발 위치 번호(slot), 파티 안에서는 순번(order)
    const m = findMatch(room), slot = m.slot++ % (SPAWNS[room.map] || 1);
    const order = {};
    let i = 0;
    for (const p of room.players.values()) { order[p.id] = i++; p.inRaid = true; p.match = m; p.state = null; p.down = false; m.players.add(p); }
    if (!m.ai) m.ai = room.host; // 판을 처음 연 파티의 파티장이 AI 적 계산을 맡음
    return broadcast(room, { t: 'start', seed: m.seed, slot, order, ai: m.ai, room: roomInfo(room) });
  }

  const m = c.match;
  if (!room.started || !m) return;

  // 내 상태: [x, y, z, yaw, pitch, flags, weapon, hp]
  if (t === 's') {
    const p = msg.p;
    if (!Array.isArray(p) || p.length > 10 || !p.every(v => typeof v === 'number' && Number.isFinite(v))) return;
    c.state = p;
    return;
  }

  // AI 적 정보: AI 담당만 보낼 수 있고, 서버는 그대로 전달
  if (t === 'ai') {
    if (m.ai !== c.id || !msg.d || typeof msg.d !== 'object') return;
    return matchBroadcast(m, { t: 'ai', d: msg.d }, c);
  }

  // 사건
  if (t === 'e') {
    const k = msg.k;
    if (k === 'hit') {
      // 다른 플레이어에게 피해 (같은 파티 팀킬, 다른 파티 공격). 같은 판의 맞은 사람에게만 보냄
      const target = [...m.players].find(p => p.id === msg.to);
      const dmg = Number(msg.dmg);
      if (!target || !(dmg > 0) || dmg > MAX_DMG) return;
      return send(target, { t: 'e', k: 'hit', from: c.id, dmg, head: !!msg.head });
    }
    if (k === 'ehit') {
      // AI 적에게 준 피해 묶음 [[적 번호, 피해], ...]. AI 담당에게만 보냄
      const ai = [...m.players].find(p => p.id === m.ai);
      if (!ai || ai === c || !Array.isArray(msg.h) || msg.h.length > 60) return;
      const h = msg.h.filter(a => Array.isArray(a) && Number.isFinite(a[0]) && Number(a[1]) > 0 && Number(a[1]) <= 20000).map(a => [a[0] | 0, Number(a[1])]);
      if (h.length) send(ai, { t: 'e', k: 'ehit', from: c.id, h });
      return;
    }
    if (k === 'revive') {
      // 부활은 같은 파티끼리만
      const target = room.players.get(msg.to);
      if (!target || !target.down || target.match !== m) return;
      target.down = false;
      return matchBroadcast(m, { t: 'e', k: 'revive', from: c.id, to: target.id });
    }
    if (k === 'down') c.down = true;
    if (k === 'up') c.down = false;
    // 그 밖의 사건(사격, 쓰러짐, 사망, 탈출 등)은 같은 판의 다른 사람에게 그대로 전달
    const ALLOWED = ['shot', 'down', 'up', 'dead', 'extract', 'loot', 'boom', 'door', 'say'];
    if (!ALLOWED.includes(k)) return;
    const out = { t: 'e', k, from: c.id };
    if (msg.d !== undefined) out.d = msg.d;
    matchBroadcast(m, out, c);
    if (k === 'dead' || k === 'extract') leaveMatch(c);
    return;
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
  for (const m of matches) {
    m.tick++;
    const list = [...m.players].filter(c => c.state);
    for (const me of m.players) {
      const out = [];
      for (const o of list) {
        if (o === me) continue;
        if (me.state) {
          const dx = o.state[0] - me.state[0], dz = o.state[2] - me.state[2];
          if (dx * dx + dz * dz > FAR * FAR && m.tick % 3) continue;
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
      `파티 ${rooms.size}개, 진행 중인 판 ${matches.size}개, 접속 ${players}명, 최대 동시 접속 ${usage.peak}명\n`);
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
  const c = { id: nextId++, ws, nick: '', room: null, match: null, team: 0, ready: false, down: false, state: null, msgs: 0, msgT: Date.now() };
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
