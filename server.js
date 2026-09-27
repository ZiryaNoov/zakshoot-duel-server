// ZAKSHOOT DUEL — online relay server (0-dep node, hand-rolled WebSocket).
// Quick-match pairing: first two clients that send {t:'find'} within 90s get paired
// into a private room; all further NDJSON lines are routed 1:1 between them.
// Rooms are isolated; idle sockets (no data 60s) are pruned.
// Deploy: node duel-server.mjs [port]   (default 8801; /health for monitoring)
import http from 'node:http';
import crypto from 'node:crypto';

const PORT = parseInt(process.env.PORT || process.argv[2] || '8801');
const log = (...a) => console.log(new Date().toISOString().slice(11, 19), ...a);

const server = http.createServer((req, res) => {
  if (req.url === '/health') { res.writeHead(200); res.end('ok v3 ' + new Date().toISOString()); }
  else { res.writeHead(404); res.end(); }
});

let nextRoom = 1;
const rooms = new Map();   // roomId -> {a, b}
const waiting = [];        // sockets in find queue

function accept(key) {
  return crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
}
function ctrl(op, payload) {
  const len = payload.length;
  return Buffer.from([op | 0x80, len, ...payload]);
}
function frame(payload) {
  const len = payload.length;
  let head;
  if (len < 126) head = Buffer.from([0x81, len]);
  else { head = Buffer.alloc(4); head[0] = 0x81; head[1] = 126; head.writeUInt16BE(len, 2); }
  return Buffer.concat([head, payload]);
}

server.on('upgrade', (req, sock) => {
  const key = req.headers['sec-websocket-key'];
  if (!key) { sock.destroy(); return; }
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept(key) + '\r\n\r\n');
  const c = { sock, room: null, buf: Buffer.alloc(0), alive: true, lastSeen: Date.now() };
  ALL.add(c);
  const send = o => { try { sock.write(frame(Buffer.from(typeof o === 'string' ? o : JSON.stringify(o)))); } catch {} };
  const bye = () => {
    ALL.delete(c);
    c.alive = false;
    const w = waiting.indexOf(c); if (w >= 0) waiting.splice(w, 1);
    if (c.room) {
      const r = rooms.get(c.room);
      if (r) {
        const other = r.a === c ? r.b : r.a;
        rooms.delete(c.room);
        if (other && other.alive) { other.send({t: 'srv', ev: 'peer_left'}); other.room = null; }
      }
    }
    try { sock.destroy(); } catch {}
    log('bye', c.id || '?', '| rooms', rooms.size, '| waiting', waiting.length);
  };
  c.send = send; c.bye = bye;
  c.id = 'c' + Math.random().toString(36).slice(2, 6);

  const handle = line => {
    c.lastSeen = Date.now();
    let m; try { m = JSON.parse(line); } catch { return; }
    if (m.t === 'find') {
      if (c.room) return;
      waiting.push(c);
      send({t: 'srv', ev: 'queued', n: waiting.length});
      log('find', c.id, '| waiting', waiting.length);
      while (waiting.length >= 2) {
        const a = waiting.shift(), b = waiting.shift();
        const id = 'r' + (nextRoom++);
        rooms.set(id, {a, b});
        a.room = id; b.room = id;
        a.send({t: 'srv', ev: 'paired', role: 'host'});
        b.send({t: 'srv', ev: 'paired', role: 'join'});
        log('paired', a.id, '+', b.id, '->', id, '| rooms', rooms.size);
      }
    } else if (m.t === 'ping') send({t: 'pong'});
    else if (c.room) {
      const r = rooms.get(c.room);
      if (r) (r.a === c ? r.b : r.a).send(line);
    }
  };

  sock.on('data', d => {
    c.buf = Buffer.concat([c.buf, d]);
    while (true) {
      if (c.buf.length < 2) break;
      const op = c.buf[0] & 0x0f;
      let len = c.buf[1] & 0x7f, off = 2;
      if (len === 126) { if (c.buf.length < 4) break; len = c.buf.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (c.buf.length < 10) break; len = Number(c.buf.readBigUInt64BE(2)); off = 10; }
      const mask = c.buf.slice(off, off + 4); off += 4;
      if (c.buf.length < off + len) break;
      const payload = Buffer.alloc(len);
      for (let i = 0; i < len; i++) payload[i] = c.buf[off + i] ^ mask[i & 3];
      c.buf = c.buf.slice(off + len);
      if (op === 9) { try { sock.write(ctrl(0x0A, payload)); } catch {} }
      else if (op === 8) { bye(); return; }
      if (op === 1) { const line = payload.toString('utf8'); if (line.trim()) handle(line); }
    }
  });
  sock.on('error', bye);
  sock.on('close', bye);
  log('open', c.id);
});

const ALL = new Set();
// liveness prune: game pings every 3s — silence >15s = player gone (Render's proxy
// keeps dead upstream sockets open, so TCP close alone can't be trusted)
setInterval(() => {
  const now = Date.now();
  for (const c of [...ALL]) {
    const limit = waiting.includes(c) ? 90000 : 15000;  // finders get 90s (human-paced); in-room = game pings 3s
    if (now - c.lastSeen > limit) {
      if (waiting.includes(c)) c.send({t: 'srv', ev: 'timeout'});
      c.bye();
    }
  }
}, 5000);

server.listen(PORT, () => log('duel-server on :' + PORT));
