const pad = (value) => String(value).padStart(2, '0');

export const formatPublishText = (date = new Date()) =>
  `hello ${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;

export const appendBounded = (items, item, limit = 500) =>
  [...items, item].slice(-limit);

export const clipUtf8Text = (value, limit, encode, decode) => {
  const text = String(value ?? '');
  const bytes = encode(text);
  if (bytes.length <= limit) return text;
  for (let end = limit; end >= Math.max(0, limit - 3); end -= 1) {
    try {
      return `${decode(bytes.slice(0, end))}… [truncated]`;
    } catch {
      // A UTF-8 code point may straddle the byte limit.
    }
  }
  return '… [truncated]';
};
