import net from 'node:net';
import { appendFile } from 'node:fs/promises';

const targetHost = process.env.TARGET_HOST ?? '127.0.0.1';
const targetPort = Number(process.env.TARGET_PORT ?? 1883);
const listenPort = Number(process.env.LISTEN_PORT ?? 2894);
const reportPath = process.env.REPORT_PATH;

function readVariableLength(buffer, offset) {
  let value = 0;
  let multiplier = 1;
  for (let index = 0; index < 4 && offset + index < buffer.length; index += 1) {
    const byte = buffer[offset + index];
    value += (byte & 0x7f) * multiplier;
    if ((byte & 0x80) === 0) return { value, next: offset + index + 1 };
    multiplier *= 128;
  }
  return null;
}

function readUtf8(buffer, offset) {
  if (offset + 2 > buffer.length) return null;
  const length = buffer.readUInt16BE(offset);
  const start = offset + 2;
  const end = start + length;
  if (end > buffer.length) return null;
  return { value: buffer.subarray(start, end).toString('utf8'), next: end };
}

function inspectConnect(packet) {
  if ((packet[0] >> 4) !== 1) return null;
  const remaining = readVariableLength(packet, 1);
  if (!remaining || remaining.next + remaining.value > packet.length) return null;
  let offset = remaining.next;
  const protocol = readUtf8(packet, offset);
  if (!protocol) return null;
  offset = protocol.next + 1 + 1 + 2;
  const flags = packet[protocol.next + 1];
  const properties = readVariableLength(packet, offset);
  if (!properties || properties.next + properties.value > packet.length) return null;
  offset = properties.next;
  const end = offset + properties.value;
  const result = {
    protocol,
    cleanStart: Boolean(flags & 0x02),
    will: Boolean(flags & 0x04),
    sessionExpiryIntervalSeconds: undefined,
    receiveMaximum: undefined,
    userProperties: [],
  };
  while (offset < end) {
    const id = packet[offset++];
    if (id === 0x11 && offset + 4 <= end) {
      result.sessionExpiryIntervalSeconds = packet.readUInt32BE(offset);
      offset += 4;
    } else if (id === 0x21 && offset + 2 <= end) {
      result.receiveMaximum = packet.readUInt16BE(offset);
      offset += 2;
    } else if (id === 0x26) {
      const key = readUtf8(packet, offset);
      if (!key) break;
      const value = readUtf8(packet, key.next);
      if (!value) break;
      result.userProperties.push({ key: key.value, value: value.value });
      offset = value.next;
    } else if (id === 0x01 || id === 0x15 || id === 0x1f || id === 0x22 || id === 0x27) {
      offset += id === 0x27 ? 4 : id === 0x22 ? 2 : 1;
    } else if (id === 0x02 || id === 0x18 || id === 0x28 || id === 0x29 || id === 0x2a) {
      offset += 4;
    } else if (id === 0x0b) {
      const value = readVariableLength(packet, offset);
      if (!value) break;
      offset = value.next;
    } else {
      break;
    }
  }
  return result;
}

function splitPackets(buffer) {
  const packets = [];
  let offset = 0;
  while (offset + 2 <= buffer.length) {
    const length = readVariableLength(buffer, offset + 1);
    if (!length) break;
    const total = length.next + length.value;
    if (offset + total > buffer.length) break;
    packets.push(buffer.subarray(offset, offset + total));
    offset += total;
  }
  return { packets, remainder: buffer.subarray(offset) };
}

const server = net.createServer((client) => {
  const upstream = net.createConnection({ host: targetHost, port: targetPort });
  let buffer = Buffer.alloc(0);
  client.on('data', async (chunk) => {
    buffer = Buffer.concat([buffer, chunk]);
    const parsed = splitPackets(buffer);
    buffer = parsed.remainder;
    for (const packet of parsed.packets) {
      const details = inspectConnect(packet);
      if (details && reportPath) await appendFile(reportPath, `${JSON.stringify(details)}\n`);
    }
  });
  client.pipe(upstream);
  upstream.pipe(client);
  const close = () => {
    client.destroy();
    upstream.destroy();
  };
  client.on('error', close);
  upstream.on('error', close);
  client.on('close', () => upstream.destroy());
  upstream.on('close', () => client.destroy());
});

server.listen(listenPort, '0.0.0.0', () => {
  console.log(`CONNECT inspection proxy listening on ${listenPort}, forwarding to ${targetHost}:${targetPort}`);
});
