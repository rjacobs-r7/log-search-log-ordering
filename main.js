const Net = require('net');

const PORT = 10000;
const HOST = '0.0.0.0';
const WINDOW_SECONDS = 10;
const FLUSH_INTERVAL_SECONDS = 5;
let _socket = null;

const buffer = {};
const getTime = () => Math.round(new Date().getTime() / 1000);
const addToBuffer = (token, timestamp, event) => {
  if (buffer[token] === undefined) {
    buffer[token] = [];
  }

  const received = getTime();
  buffer[token].push({ timestamp, event, received, sent: false });
  buffer[token].sort(({ timestamp: a }, { timestamp: b }) => a - b);
};
const getSocket = () => {
  if (!_socket) {
    console.log(`Opening socket...`);
    _socket = new Net.createConnection({
      port: 10000,
      host: 'data.logentries.com',
    });
  };

  return _socket;
};
const sendEvent = (token, { event }) => getSocket().write(`${token} ${event}\n`);

// TCP server
const tcpServer = new Net.createServer();
tcpServer.listen(PORT, HOST,
  () => console.log(`TCP server listening on port ${PORT}`));
tcpServer.on('connection', socket => {
  console.log(`Connected: ${socket.remoteAddress}:${socket.remotePort}`);

  socket.on('data', data => {
    String(data).trim().split('\n').forEach(line => {
      try {
        console.log(`Received: ${line}`);

        const [_, token, timestamp, event] = /(\S+)\s+(\S+)\s+(.+)/.exec(line);
        console.log({token, timestamp, event});
        addToBuffer(token, timestamp, event);
      } catch (ex) {
        console.error(ex);
      }
    });
  });

  socket.on('close',
    () => console.log(`Connection closed: ${socket.remoteAddress}:${socket.remotePort}`));
});

setInterval(() => {
  const timestamp = getTime();

  Object.keys(buffer).forEach(token => {
    const logBuffer = buffer[token];

    const cleanup = [];
    logBuffer.forEach((entry, i) => {
      if (timestamp > entry.received + WINDOW_SECONDS) {
        console.log(`Sending: ${token} ${entry.event}`);
        sendEvent(token, entry);

        entry.sent = true;
        cleanup.push(i);
      }
    });

    cleanup.forEach(i => {
      delete logBuffer[i];
    });
  });
}, FLUSH_INTERVAL_SECONDS * 1000);

/*
a1183c0c-d274-484c-a594-cee89f268240 1578529571 C
a1183c0c-d274-484c-a594-cee89f268240 1578529570 B
a1183c0c-d274-484c-a594-cee89f268240 1578529573 D
a1183c0c-d274-484c-a594-cee89f268240 1578529560 A
 */