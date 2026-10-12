import { appendBounded, clipUtf8Text, formatPublishText } from './demoHelpers';
import { decodeUtf8, encodeUtf8 } from '../../src/helpers';

describe('example demo helpers', () => {
  test('formats the publish payload with local, zero-padded time', () => {
    expect(formatPublishText(new Date(2026, 8, 12, 2, 17, 44))).toBe(
      'hello 2026-09-12 02:17:44'
    );
  });

  test('keeps only the newest bounded entries', () => {
    const entries = Array.from({ length: 501 }, (_, id) => ({ id }));
    expect(appendBounded(entries.slice(0, 500), entries[500])).toHaveLength(
      500
    );
    expect(appendBounded(entries.slice(0, 500), entries[500])[0]).toEqual({
      id: 1,
    });
  });

  test('clips payloads at the UTF-8 byte boundary', () => {
    const value = 'a'.repeat(15) + '🚀';
    const clipped = clipUtf8Text(value, 16, encodeUtf8, decodeUtf8);
    expect(clipped).toBe(`${'a'.repeat(15)}… [truncated]`);
    expect(encodeUtf8(clipped.replace('… [truncated]', '')).length).toBe(15);
  });
});
