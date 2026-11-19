// 잔해 구역 멀티플레이 서버
// - 파티 꾸리기 / 참가 / 모집 목록, 파티 하나에 최대 6명
// - 판(match): 같은 맵, 같은 50분 세션에 출격한 협동전 파티들을 최대 12명까지 한 판으로 묶음
//   같은 판의 다른 파티와 마주치고 서로 싸울 수 있음
// - 플레이어 위치를 1초에 15번 묶어서 보냄 (멀리 있는 플레이어는 덜 자주)
// - AI 적은 방장 컴퓨터가 계산하고, 서버는 다른 사람에게 전달만 함
// - 사격, 피해, 쓰러짐, 부활, 탈출 같은 사건 전달
// - 이번 달 보낸 데이터 양 기록 (주소 뒤에 /stats 를 붙이면 확인 가능)
// - 접속, 출격, 종료 기록: Render 로그에 한 줄씩 남기고, GITHUB_TOKEN 이 있으면 깃허브 logs 브랜치에 날짜별 파일로 올림
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
const JOIN_MIN_MS = 8 * 60 * 1000; // 남은 시간이 8분 이하인 세션에는 새로 들어갈 수 없음
const SPAWNS = { lab: 4, canyon: 2 }; // 맵별 출발 위치 개수 (파티마다 다른 위치를 주는 데 씀)
const MAX_ROOMS = 30;           // 서버 전체 최대 방 개수
const TICK = 1000 / 15;         // 위치 전송 간격 (1초에 15번)
const FAR = 150;                // 이 거리(m)보다 멀면 3번에 1번만 보냄
const MAX_MSG = 16 * 1024;      // 메시지 하나 최대 크기 (AI 적 정보 포함)
const RATE = 120;               // 1초에 받을 수 있는 최대 메시지 수
const MAPS = ['lab', 'canyon'];
const MODES = ['normal', 'coop', 'team'];
const NICK_RE = /^[가-힣A-Za-z0-9_]{2,12}$/;
const MAX_DMG = 400;
const MAP_NAME = { lab: '지하 연구 시설', canyon: '장거리 협곡' };            // 한 번에 줄 수 있는 최대 피해 (비정상 값 차단)

// ---------- 접속 기록 ----------
// 시각은 한국 시간. 깃허브 저장은 환경 변수 GITHUB_TOKEN(저장소 Contents 읽기/쓰기 권한)이 있을 때만 함
const LOG_REPO = process.env.LOG_REPO || 'CTPle/scrap_yard';
const LOG_BRANCH = process.env.LOG_BRANCH || 'logs';
const LOG_TOKEN = process.env.GITHUB_TOKEN || '';
const LOG_API = process.env.LOG_API || 'https://api.github.com'; // 시험용으로 바꿀 수 있음
const LOG_EVERY = 5 * 60 * 1000;  // 깃허브에 올리는 간격 (5분, 쌓인 기록이 있을 때만)
const logBuf = new Map();         // 날짜 -> 아직 올리지 않은 줄들
const kst = (t = Date.now()) => new Date(t + 9 * 3600 * 1000).toISOString();
function logLine(text) {
  const iso = kst(), day = iso.slice(0, 10), line = `${iso.slice(11, 19)}  ${text}`;
  console.log(`[기록] ${day} ${line}`);
  if (!LOG_TOKEN) return;
  if (!logBuf.has(day)) logBuf.set(day, []);
  logBuf.get(day).push(line);
}
let flushing = false;
async function flushLogs() {
  if (!LOG_TOKEN || flushing || !logBuf.size) return;
  flushing = true;
  try {
    for (const [day, lines] of [...logBuf]) {
      if (!lines.length) { logBuf.delete(day); continue; }
      const url = `${LOG_API}/repos/${LOG_REPO}/contents/logs/${day}.txt`;
      const head = { authorization: `Bearer ${LOG_TOKEN}`, accept: 'application/vnd.github+json', 'user-agent': 'scrapyard-server', 'x-github-api-version': '2022-11-28' };
      // 그날 파일이 이미 있으면 뒤에 이어 붙임
      let sha, prev = '';
      const g = await fetch(`${url}?ref=${LOG_BRANCH}`, { headers: head });
      if (g.ok) { const j = await g.json(); sha = j.sha; prev = Buffer.from(j.content || '', 'base64').toString('utf8'); }
      else if (g.status !== 404) throw new Error(`읽기 실패 ${g.status}`);
      const take = lines.slice();
      const body = { message: `접속 기록 ${day} (${take.length}줄)`, branch: LOG_BRANCH, content: Buffer.from(prev + take.join('\n') + '\n', 'utf8').toString('base64') };
      if (sha) body.sha = sha;
      const r = await fetch(url, { method: 'PUT', headers: { ...head, 'content-type': 'application/json' }, body: JSON.stringify(body) });
      if (!r.ok) throw new Error(`쓰기 실패 ${r.status} ${(await r.text()).slice(0, 200)}`);
      lines.splice(0, take.length); // 올린 줄만 지움 (올리는 사이에 새로 쌓인 줄은 다음 번에)
      if (!lines.length) logBuf.delete(day);
    }
  } catch (err) { console.error('기록 올리기 실패 (다음 번에 다시 시도):', err.message); }
  finally { flushing = false; }
}
setInterval(flushLogs, LOG_EVERY);
// Render 가 서버를 끄거나 재배포할 때 남은 기록을 올리고 종료
let stopping = false;
for (const sig of ['SIGTERM', 'SIGINT']) process.on(sig, async () => {
  if (stopping) return; stopping = true;
  logLine('서버 종료');
  for (const ws of (typeof wss !== 'undefined' ? wss.clients : [])) { const c = ws._c; if (c && c.nick) logLine(`종료  ${c.nick}  (서버 종료로 끊김, ${Math.round((Date.now() - c.t0) / 60000)}분 머묾, 출격 ${c.raids || 0}회)`); }
  await Promise.race([flushLogs(), new Promise(r => setTimeout(r, 8000))]);
  process.exit(0);
});

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
// 세션: 누군가 출격할 때만 새로 시작 (아무도 없으면 기다림). 50분 동안 이어지고,
// 남은 시간이 8분을 넘는 동안에만 다른 파티가 같은 세션에 들어올 수 있음. 그보다 적게 남았으면 새 세션을 염
const sessions = new Map(); // 맵 -> [{ id, map, start }]
let nextSess = 1;
function joinableSession(map, now = Date.now()) {
  const list = (sessions.get(map) || []).filter(ss => ss.start + SESSION_MS > now);
  sessions.set(map, list);
  return list.find(ss => ss.start + SESSION_MS - now > JOIN_MIN_MS) || null;
}
function sessionFor(map) {
  let ss = joinableSession(map);
  if (!ss) { ss = { id: nextSess++, map, start: Date.now() }; sessions.get(map).push(ss); }
  return ss;
}
// 은신처 표시용: 맵마다 지금 들어갈 수 있는 세션의 끝나는 시각 (없으면 0 = 출격하면 새 세션)
function sessInfo() {
  const now = Date.now(), maps = {};
  for (const k of Object.keys(SPAWNS)) { const ss = joinableSession(k, now); maps[k] = ss ? ss.start + SESSION_MS : 0; }
  return { t: 'sess', now, len: SESSION_MS, joinMin: JOIN_MIN_MS, maps };
}
// 파티가 출격할 판 찾기: 같은 맵, 같은 세션, 자리가 남은 판. 없으면 새로 만듦
function findMatch(room) {
  const ss = sessionFor(room.map), n = room.players.size;
  for (const m of matches) if (m.map === room.map && m.sess === ss && m.players.size + n <= MATCH_MAX) return m;
  const m = { map: room.map, sess: ss, seed: Math.floor(Math.random() * 2 ** 31), players: new Set(), ai: 0, slot: 0, tick: 0 };
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
    const ver = typeof msg.ver === 'string' ? msg.ver.slice(0, 20) : '';
    if (!c.nick) logLine(`접속  ${nick}  ${ver}`);
    else if (c.nick !== nick) logLine(`닉네임 변경  ${c.nick} -> ${nick}`);
    c.nick = nick;
    c.ver = ver;
    if (c.room) sendRoom(c.room);
    return send(c, { t: 'welcome', id: c.id, now: Date.now() }); // now: 세션 시계를 맞추는 서버 시간
  }
  if (!c.nick) return send(c, { t: 'err', m: '먼저 닉네임을 보내야 합니다' });

  if (t === 'sess') return send(c, sessInfo());

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
    for (const p of room.players.values()) { order[p.id] = i++; p.inRaid = true; p.match = m; p.state = null; p.down = false; m.players.add(p); p.raids = (p.raids || 0) + 1; }
    logLine(`출격  ${[...room.players.values()].map(p => p.nick).join(', ')}  (${MAP_NAME[room.map] || room.map}, ${room.players.size}명 파티)`);
    if (!m.ai) m.ai = room.host; // 판을 처음 연 파티의 파티장이 AI 적 계산을 맡음
    return broadcast(room, { t: 'start', seed: m.seed, slot, order, ai: m.ai, sessEnd: m.sess.start + SESSION_MS, now: Date.now(), room: roomInfo(room) });
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
    const ALLOWED = ['shot', 'throw', 'down', 'up', 'dead', 'extract', 'loot', 'boom', 'door', 'say'];
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

// 지금 접속 중인 사람 목록 (협동 모드 화면을 연 사람만 서버에 접속함)
function onlineList() {
  const now = Date.now(), rows = [];
  for (const ws of wss.clients) {
    const c = ws._c; if (!c || !c.nick) continue;
    const where = c.match ? `출격 중 (${MAP_NAME[c.match.map] || c.match.map})` : c.room ? `파티 대기 (${c.room.players.size}명)` : '로비';
    rows.push(`- ${c.nick}  ${where}  접속 ${Math.floor((now - c.t0) / 60000)}분째  ${c.ver || ''}`);
  }
  return `\n지금 접속 중 (${rows.length}명)\n` + (rows.length ? rows.join('\n') : '(없음)') + '\n';
}

// ---------- HTTP 와 WebSocket ----------
// ---------- 일반 모드 실행 기록 ----------
// 일반 모드는 서버에 계속 연결하지 않음. 게임을 켤 때(start), 창을 닫을 때(end) 한 번씩만 /hit 으로 알려 옴
// 닉네임이 없으면 '익명의 유저'로 남김. 같은 곳에서 1분에 6번 넘게 오면 무시
const hitLog = new Map();
setInterval(() => { const now = Date.now(); for (const [ip, a] of hitLog) if (!a.some(t => now - t < 60000)) hitLog.delete(ip); }, 10 * 60 * 1000);
function onHit(req, res) {
  req.resume();
  const done = () => { res.writeHead(204, { 'access-control-allow-origin': '*' }); res.end(); };
  const from = req.headers.origin || req.headers.referer || '';
  if (!ORIGINS.some(o => from.startsWith(o)) && !/^https?:\/\/(localhost|127\.0\.0\.1)(:\d+)?/.test(from)) return done();
  const ip = String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
  const now = Date.now(), times = (hitLog.get(ip) || []).filter(t => now - t < 60000);
  if (times.length >= 6) return done();
  times.push(now); hitLog.set(ip, times);
  const q = new URL(req.url, 'http://x').searchParams;
  const nick = q.get('nick') || '', dev = q.get('dev') || '', ver = (q.get('ver') || '').slice(0, 20).replace(/[^\w.]/g, '');
  const who = NICK_RE.test(nick) ? nick : /^[0-9a-f]{6}$/.test(dev) ? '익명의 유저' : '';
  if (!who) return done();
  const num = (k, max) => Math.min(max, Math.max(0, parseInt(q.get(k), 10) || 0));
  if (q.get('ev') === 'start') logLine(`실행  ${who}  ${ver}  (일반 모드)`);
  else if (q.get('ev') === 'end') logLine(`종료  ${who}  (${num('min', 1440)}분 머묾, 출격 ${num('raids', 999)}회, 일반 모드)`);
  done();
}

const server = http.createServer((req, res) => {
  if (req.url.startsWith('/hit')) return onHit(req, res);
  if (req.url === '/stats') {
    const players = [...rooms.values()].reduce((n, r) => n + r.players.size, 0);
    res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' });
    res.end(`이번 달(${usage.month || monthKey()}) 보낸 데이터: 약 ${(usage.bytes / 1048576).toFixed(1)}MB\n` +
      `(서버가 다시 켜지면 0부터 다시 셉니다. 정확한 값은 Render 대시보드에서 확인)\n` +
      `파티 ${rooms.size}개, 진행 중인 판 ${matches.size}개, 접속 ${players}명, 최대 동시 접속 ${usage.peak}명\n` +
      onlineList());
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
  const c = { id: nextId++, ws, nick: '', room: null, match: null, team: 0, ready: false, down: false, state: null, msgs: 0, msgT: Date.now(), t0: Date.now() };
  ws._c = c;
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
  ws.on('close', () => { if (c.nick && !stopping) logLine(`종료  ${c.nick}  (${Math.round((Date.now() - c.t0) / 60000)}분 머묾, 출격 ${c.raids || 0}회)`); leaveRoom(c); });
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

server.listen(PORT, () => { console.log(`잔해 구역 서버 시작: 포트 ${PORT}, 깃허브 기록 ${LOG_TOKEN ? '켜짐' : '꺼짐 (GITHUB_TOKEN 없음)'}`); logLine('서버 시작'); });
