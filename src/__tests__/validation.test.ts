import { MqttError } from '../errors';
import {
  normalizeOptions,
  normalizePublishOptions,
  normalizeSubscribeOptions,
  toWirePublishRequest,
  toWireSessionOptions,
  validateTopicFilter,
  validateTopicName,
} from '../validation';

const base = { host: 'broker.example', port: 1883 } as const;

describe('option validation', () => {
  test('rejects MQTT 3.1 protocol level 3 with an explicit options error', () => {
    expect(() =>
      normalizeOptions({ ...base, protocolVersion: 3 } as never)
    ).toThrow(
      expect.objectContaining({
        code: 'ERR_INVALID_OPTIONS',
        message: 'protocolVersion must be 4 or 5',
      })
    );
  });
  test('keeps MQTT5 uint32 maximum and seconds defaults', () => {
    const options = normalizeOptions({
      ...base,
      protocolVersion: 5,
      sessionExpiryIntervalSeconds: 4294967295,
    });
    expect(options.sessionExpiryIntervalSeconds).toBe(4294967295);
    expect(options.keepAliveSeconds).toBe(60);
    expect(toWireSessionOptions(options).sessionExpiryIntervalSeconds).toBe(
      4294967295
    );
  });

  test('rejects invalid numbers, cross-version fields and unknown keys', () => {
    expect(() => normalizeOptions({ ...base, port: 1.5 })).toThrow(MqttError);
    expect(() =>
      normalizeOptions({ ...base, cleanStart: true } as never)
    ).toThrow(MqttError);
    expect(() =>
      normalizeOptions({ ...base, unsupported: true } as never)
    ).toThrow(MqttError);
    expect(() => normalizeOptions({ ...base, clientId: '' })).toThrow(
      MqttError
    );
    expect(() =>
      normalizeOptions({
        ...base,
        protocolVersion: 5,
        clientId: '',
        cleanStart: false,
      })
    ).toThrow(MqttError);
    expect(() => normalizeSubscribeOptions({ noLocal: true }, 4)).toThrow(
      MqttError
    );
    expect(() =>
      normalizePublishOptions({ contentType: 'text/plain' }, 4)
    ).toThrow(MqttError);
  });

  test('separates topic filters from publish names', () => {
    expect(() => validateTopicName('device/+/status')).toThrow(MqttError);
    expect(() => validateTopicFilter('device/+/status')).not.toThrow();
    expect(() => validateTopicFilter('device//status')).not.toThrow();
    expect(() => validateTopicFilter('$SYS/#')).not.toThrow();
    expect(() => validateTopicFilter('+/#')).not.toThrow();
    expect(() => validateTopicFilter('device/+/status+')).toThrow(MqttError);
    expect(() => validateTopicFilter('device/#/status')).toThrow(MqttError);
    expect(() => validateTopicFilter('device\0/status')).toThrow(MqttError);
    expect(() => validateTopicFilter(`device/${'x'.repeat(65536)}`)).toThrow(
      MqttError
    );
    expect(() => validateTopicFilter('device/\ud800/status')).toThrow(
      MqttError
    );
    expect(() => validateTopicName('device/\ud800/status')).toThrow(MqttError);
    expect(() =>
      toWirePublishRequest('device/+/status', '', undefined, 4)
    ).toThrow(MqttError);
    expect(() =>
      normalizePublishOptions({ responseTopic: 'reply/topic' })
    ).not.toThrow();
  });

  test('rejects malformed MQTT UTF-16 before encoding', () => {
    expect(() => normalizeOptions({ ...base, username: '\ud800' })).toThrow(
      MqttError
    );
    expect(() => normalizeOptions({ ...base, clientId: '\ud800' })).toThrow(
      MqttError
    );
  });

  test('rejects TLS configuration on a non-TLS transport', () => {
    expect(() =>
      normalizeOptions({ ...base, tls: { insecure: true } })
    ).toThrow(MqttError);
  });

  test('preserves duplicate properties and validates UTF-8 payload indicator', () => {
    const options = normalizeOptions({
      ...base,
      protocolVersion: 5,
      userProperties: [
        { key: 'x', value: '1' },
        { key: 'x', value: '2' },
      ],
    });
    expect(options.userProperties).toEqual([
      { key: 'x', value: '1' },
      { key: 'x', value: '2' },
    ]);
    expect(() =>
      toWireSessionOptions({
        ...options,
        transport: 'tls',
        tls: { insecure: true },
      })
    ).not.toThrow();
    expect(() =>
      // 0xff is not valid UTF-8 when payloadFormatIndicator is 1.
      toWirePublishRequest('topic', '/w==', { payloadFormatIndicator: 1 }, 5)
    ).toThrow(MqttError);
  });
});
