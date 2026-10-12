import { MqttError } from './errors';
type TextEncoderLike = { encode(input: string): Uint8Array };
type TextDecoderLike = { decode(input: Uint8Array): string };
const textEncoder = (): TextEncoderLike | undefined => {
  const ctor = (
    globalThis as unknown as { TextEncoder?: new () => TextEncoderLike }
  ).TextEncoder;
  return ctor === undefined ? undefined : new ctor();
};
const textDecoder = (): TextDecoderLike | undefined => {
  const ctor = (
    globalThis as unknown as {
      TextDecoder?: new (
        label?: string,
        options?: { fatal?: boolean }
      ) => TextDecoderLike;
    }
  ).TextDecoder;
  return ctor === undefined ? undefined : new ctor('utf-8', { fatal: true });
};
function invalidPayload(message: string): MqttError {
  return new MqttError('ERR_INVALID_PAYLOAD', message, { outcome: 'not-sent' });
}
export function encodeUtf8(text: string): Uint8Array {
  const encoder = textEncoder();
  if (encoder === undefined) throw invalidPayload('TextEncoder is unavailable');
  return encoder.encode(text);
}
export function decodeUtf8(bytes: Uint8Array): string {
  const decoder = textDecoder();
  if (decoder !== undefined) {
    try {
      return decoder.decode(bytes);
    } catch {
      throw invalidPayload('Payload is not valid UTF-8');
    }
  }

  // Hermes/RN versions without TextDecoder still need a deterministic,
  // strict decoder for incoming MQTT payloads.
  let result = '';
  const codePoints: number[] = [];
  const flush = (): void => {
    if (codePoints.length === 0) return;
    result += String.fromCodePoint(...codePoints);
    codePoints.length = 0;
  };
  const continuation = (index: number): number => {
    const value = bytes[index];
    if (value === undefined || (value & 0xc0) !== 0x80)
      throw invalidPayload('Payload is not valid UTF-8');
    return value & 0x3f;
  };
  for (let index = 0; index < bytes.length; index += 1) {
    const first = bytes[index];
    if (first === undefined) throw invalidPayload('Payload is not valid UTF-8');
    if (first <= 0x7f) {
      codePoints.push(first);
      continue;
    }
    let codePoint: number;
    if (first >= 0xc2 && first <= 0xdf) {
      codePoint = ((first & 0x1f) << 6) | continuation(index + 1);
      index += 1;
    } else if (first >= 0xe0 && first <= 0xef) {
      const second = bytes[index + 1];
      if (
        second === undefined ||
        (second & 0xc0) !== 0x80 ||
        (first === 0xe0 && second < 0xa0) ||
        (first === 0xed && second > 0x9f)
      )
        throw invalidPayload('Payload is not valid UTF-8');
      codePoint =
        ((first & 0x0f) << 12) |
        ((second & 0x3f) << 6) |
        continuation(index + 2);
      index += 2;
    } else if (first >= 0xf0 && first <= 0xf4) {
      const second = bytes[index + 1];
      if (
        second === undefined ||
        (second & 0xc0) !== 0x80 ||
        (first === 0xf0 && second < 0x90) ||
        (first === 0xf4 && second > 0x8f)
      )
        throw invalidPayload('Payload is not valid UTF-8');
      codePoint =
        ((first & 0x07) << 18) |
        ((second & 0x3f) << 12) |
        (continuation(index + 2) << 6) |
        continuation(index + 3);
      index += 3;
    } else {
      throw invalidPayload('Payload is not valid UTF-8');
    }
    codePoints.push(codePoint);
    if (codePoints.length >= 0x4000) flush();
  }
  flush();
  return result;
}
const base64Alphabet =
  'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/';
export function encodeBase64(bytes: Uint8Array): string {
  let result = '';
  for (let index = 0; index < bytes.length; index += 3) {
    const a = bytes[index] ?? 0;
    const b = bytes[index + 1];
    const c = bytes[index + 2];
    result += base64Alphabet[a >> 2];
    result += base64Alphabet[((a & 3) << 4) | ((b ?? 0) >> 4)];
    result +=
      b === undefined ? '=' : base64Alphabet[((b & 15) << 2) | ((c ?? 0) >> 6)];
    result += c === undefined ? '=' : base64Alphabet[c & 63];
  }
  return result;
}
export function decodeBase64(text: string): Uint8Array {
  if (text.length % 4 !== 0 || /[^A-Za-z0-9+/=]/.test(text))
    throw invalidPayload('Base64 must use RFC 4648 alphabet and padding');
  const padding = text.endsWith('==') ? 2 : text.endsWith('=') ? 1 : 0;
  const firstPadding = text.indexOf('=');
  if (firstPadding >= 0 && firstPadding < text.length - padding)
    throw invalidPayload('Base64 padding is invalid');
  const output = new Uint8Array((text.length / 4) * 3 - padding);
  let outputIndex = 0;
  for (let index = 0; index < text.length; index += 4) {
    const chars = text.slice(index, index + 4);
    const values = chars
      .split('')
      .map((char) => (char === '=' ? 0 : base64Alphabet.indexOf(char)));
    const v0 = values[0] ?? -1;
    const v1 = values[1] ?? -1;
    const v2 = values[2] ?? -1;
    const v3 = values[3] ?? -1;
    if ([v0, v1, v2, v3].some((value) => value < 0))
      throw invalidPayload('Base64 contains an invalid character');
    if (chars[2] === '=' && (v1 & 15) !== 0)
      throw invalidPayload('Base64 has non-zero trailing bits');
    if (chars[3] === '=' && chars[2] !== '=' && (v2 & 3) !== 0)
      throw invalidPayload('Base64 has non-zero trailing bits');
    const combined = (v0 << 18) | (v1 << 12) | (v2 << 6) | v3;
    if (outputIndex < output.length)
      output[outputIndex++] = (combined >> 16) & 255;
    if (outputIndex < output.length)
      output[outputIndex++] = (combined >> 8) & 255;
    if (outputIndex < output.length) output[outputIndex++] = combined & 255;
  }
  return output;
}
