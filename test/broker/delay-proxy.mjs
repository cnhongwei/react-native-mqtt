import net from 'node:net';

const targetHost = process.env.TARGET_HOST ?? 'mosquitto';
const targetPort = Number(process.env.TARGET_PORT ?? 1883);
const delayMs = Number(process.env.DELAY_MS ?? 250);
const listenPort = Number(process.env.LISTEN_PORT ?? 2883);
const dropPacketTypes = new Set(
  (process.env.DROP_PACKET_TYPES ?? '')
    .split(',')
    .map((value) => Number(value.trim()))
    .filter((value) => Number.isInteger(value) && value >= 1 && value <= 15),
);
const negativePacketTypes = new Set(
  (process.env.NEGATIVE_PACKET_TYPES ?? '')
    .split(',')
    .map((value) => Number(value.trim()))
    .filter((value) => Number.isInteger(value) && value >= 1 && value <= 15),
);
const negativeReasonCode = Number(process.env.NEGATIVE_REASON_CODE ?? 0x87);
const disconnectReasonCode = Number(process.env.DISCONNECT_REASON_CODE ?? -1);
const disconnectDelayMs = Number(process.env.DISCONNECT_DELAY_MS ?? 25);
const disconnectAfterPacketType = Number(process.env.DISCONNECT_AFTER_PACKET_TYPE ?? 2);
const closeClientAfterPacketType = Number(process.env.CLOSE_CLIENT_AFTER_PACKET_TYPE ?? -1);
const closeClientDelayMs = Number(process.env.CLOSE_CLIENT_DELAY_MS ?? 25);
const closeAfterUpstreamPacketType = Number(process.env.CLOSE_AFTER_UPSTREAM_PACKET_TYPE ?? -1);
const delayedPacketTypes = new Set(
  (process.env.DELAY_PACKET_TYPES ?? '')
    .split(',')
    .map((value) => Number(value.trim()))
    .filter((value) => Number.isInteger(value) && value >= 1 && value <= 15),
);
const packetDelayMs = Number(process.env.PACKET_DELAY_MS ?? delayMs);
const duplicatePacketTypes = new Set(
  (process.env.DUPLICATE_PACKET_TYPES ?? '')
    .split(',')
    .map((value) => Number(value.trim()))
    .filter((value) => Number.isInteger(value) && value >= 1 && value <= 15),
);

function replaceFirstReasonCode(packet, packetType) {
  // SUBACK and UNSUBACK carry a properties-length byte before reason codes;
  // PUBACK/PUBREC/PUBCOMP carry the reason code directly after packet id.
  // Mosquitto may use the MQTT 5 short form (remaining length 2), which
  // omits the success reason and properties. Expand that form before
  // injecting a negative reason code.
  if ((packetType === 4 || packetType === 5 || packetType === 7) && packet.length === 4) {
    return Buffer.from([packet[0], 4, packet[2], packet[3], negativeReasonCode & 0xff, 0]);
  }
  const reasonIndex = packetType === 9 || packetType === 11 ? 5 : 4;
  if (packet.length <= reasonIndex) return packet;
  const mutated = Buffer.from(packet);
  mutated[reasonIndex] = negativeReasonCode & 0xff;
  return mutated;
}

function splitMqttPackets(buffer) {
  const packets = [];
  let offset = 0;
  while (offset + 2 <= buffer.length) {
    let multiplier = 1;
    let remainingLength = 0;
    let lengthBytes = 0;
    let completeLength = false;
    for (; lengthBytes < 4 && offset + 1 + lengthBytes < buffer.length; lengthBytes += 1) {
      const byte = buffer[offset + 1 + lengthBytes];
      remainingLength += (byte & 0x7f) * multiplier;
      if ((byte & 0x80) === 0) {
        completeLength = true;
        break;
      }
      multiplier *= 128;
    }
    if (!completeLength) break;
    const headerLength = 1 + lengthBytes + 1;
    const packetLength = headerLength + remainingLength;
    if (offset + packetLength > buffer.length) break;
    packets.push(buffer.subarray(offset, offset + packetLength));
    offset += packetLength;
  }
  return { packets, remainder: buffer.subarray(offset) };
}

const server = net.createServer((client) => {
  const upstream = net.createConnection({ host: targetHost, port: targetPort });
  let disconnectSent = false;
  let closeClientScheduled = false;
  let clientBuffer = Buffer.alloc(0);
  let upstreamBuffer = Buffer.alloc(0);
  client.pipe(upstream);
  client.on('data', (chunk) => {
    clientBuffer = Buffer.concat([clientBuffer, chunk]);
    const parsed = splitMqttPackets(clientBuffer);
    clientBuffer = parsed.remainder;
    parsed.packets.forEach((packet) => {
      const packetType = packet[0] >> 4;
      if (packetType === closeClientAfterPacketType && !closeClientScheduled) {
        closeClientScheduled = true;
        setTimeout(() => {
          if (!client.destroyed) client.destroy();
          if (!upstream.destroyed) upstream.destroy();
          console.log(`closing client connection after packet type ${packetType}`);
        }, closeClientDelayMs);
      }
    });
  });
  upstream.on('data', (chunk) => {
    upstreamBuffer = Buffer.concat([upstreamBuffer, chunk]);
    const parsed = splitMqttPackets(upstreamBuffer);
    upstreamBuffer = parsed.remainder;
    parsed.packets.forEach((packet) => {
      const packetType = packet[0] >> 4;
      if (dropPacketTypes.has(packetType)) {
        console.log(`dropping broker packet type ${packetType}`);
        return;
      }
      const output = negativePacketTypes.has(packetType)
        ? replaceFirstReasonCode(packet, packetType)
        : packet;
      if (output !== packet) console.log(`injecting reason ${negativeReasonCode} for broker packet type ${packetType}`);
      const outputDelay = delayedPacketTypes.has(packetType) ? packetDelayMs : delayMs;
      setTimeout(() => {
        if (!client.destroyed) {
          client.write(output);
          if (duplicatePacketTypes.has(packetType)) client.write(output);
          if (packetType === closeAfterUpstreamPacketType) {
            client.destroy();
            upstream.destroy();
            console.log(`closing client after upstream packet type ${packetType}`);
          }
        }
      }, outputDelay);
      if (packetType === disconnectAfterPacketType && disconnectReasonCode >= 0 && !disconnectSent) {
        disconnectSent = true;
        setTimeout(() => {
          if (!client.destroyed) {
            client.write(Buffer.from([0xe0, 2, disconnectReasonCode & 0xff, 0]));
            console.log(`injecting server DISCONNECT reason ${disconnectReasonCode}`);
          }
        }, Math.max(delayMs, disconnectDelayMs));
      }
    });
  });
  client.on('error', () => upstream.destroy());
  upstream.on('error', () => client.destroy());
  client.on('close', () => upstream.destroy());
  upstream.on('close', () => client.destroy());
});

server.listen(listenPort, '0.0.0.0', () => {
  console.log(`delaying upstream MQTT data by ${delayMs}ms on ${listenPort}; dropping ${[...dropPacketTypes].join(',') || 'none'}; negative ${[...negativePacketTypes].join(',') || 'none'}; close-after-client-type ${closeClientAfterPacketType}`);
});
