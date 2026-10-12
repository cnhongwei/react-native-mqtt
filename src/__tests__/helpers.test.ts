import { decodeBase64, decodeUtf8, encodeBase64, encodeUtf8 } from '../helpers';
import { MqttError } from '../errors';

describe('wire helpers', () => {
  test('round-trips UTF-8 and standard Base64 without Buffer', () => {
    const bytes = encodeUtf8('消息 🚀');
    expect(decodeUtf8(bytes)).toBe('消息 🚀');
    expect(Array.from(decodeBase64(encodeBase64(bytes)))).toEqual(
      Array.from(bytes)
    );
  });

  test('accepts empty payload and rejects non-RFC4648 input', () => {
    expect(decodeBase64('')).toEqual(new Uint8Array());
    expect(() => decodeBase64('YWJj ')).toThrow(MqttError);
    expect(() => decodeBase64('YR==')).toThrow(MqttError);
    expect(() => decodeUtf8(new Uint8Array([0xc3, 0x28]))).toThrow(MqttError);
  });

  test('decodes strictly without TextDecoder', () => {
    const runtime = globalThis as unknown as { TextDecoder?: unknown };
    const original = runtime.TextDecoder;
    Object.defineProperty(runtime, 'TextDecoder', {
      value: undefined,
      configurable: true,
    });
    try {
      expect(
        decodeUtf8(new Uint8Array([0xe6, 0xb6, 0x88, 0xf0, 0x9f, 0x9a, 0x80]))
      ).toBe('消🚀');
      expect(() => decodeUtf8(new Uint8Array([0xed, 0xa0, 0x80]))).toThrow(
        MqttError
      );
    } finally {
      Object.defineProperty(runtime, 'TextDecoder', {
        value: original,
        configurable: true,
      });
    }
  });
});
