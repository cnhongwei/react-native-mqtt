import { demoConnectionOptions, runProtocolDemo } from './protocolDemo';
import { encodeUtf8 } from '../../src/helpers';

function fixture(transform = (message) => message) {
  let callback;
  const unsubscribe = jest.fn().mockResolvedValue(undefined);
  const remove = jest.fn();
  const client = {
    onMessage: jest.fn((handler) => {
      callback = handler;
      return { remove };
    }),
    subscribe: jest.fn().mockResolvedValue({ unsubscribe }),
    publishText: jest.fn(async (topic, text, options) => {
      const message = transform({
        topic,
        bytes: encodeUtf8(text),
        retained: false,
        ...options,
      });
      if (message) callback(message);
    }),
  };
  return { client, unsubscribe, remove };
}
const options = {
  topic: 'unique/check',
  encode: encodeUtf8,
  protocolVersion: 4,
};

test.each([4, 5])(
  'protocol %s validates QoS 0/1/2 receive and cleans up',
  async (protocolVersion) => {
    const { client, unsubscribe, remove } = fixture();
    const onProgress = jest.fn();
    await runProtocolDemo(client, { ...options, protocolVersion, onProgress });
    expect(client.publishText.mock.calls.map((call) => call[2].qos)).toEqual([
      0, 1, 2,
    ]);
    expect(onProgress).toHaveBeenCalledTimes(3);
    expect(unsubscribe).toHaveBeenCalledTimes(1);
    expect(remove).toHaveBeenCalledTimes(1);
  }
);

test('MQTT 5 validates properties and preserves duplicate keys', async () => {
  const { client } = fixture();
  await runProtocolDemo(client, {
    ...options,
    protocolVersion: 5,
    properties: true,
  });
  expect(client.publishText.mock.calls[0][2].userProperties).toEqual([
    { key: 'demo', value: 'first' },
    { key: 'demo', value: 'second' },
  ]);
});

test.each([
  (message) => ({ ...message, userProperties: undefined }),
  (message) => ({
    ...message,
    userProperties: [...message.userProperties].reverse(),
  }),
  (message) => ({ ...message, contentType: undefined }),
])(
  'property loss/change fails rather than accepting publish success',
  async (transform) => {
    const { client, remove, unsubscribe } = fixture(transform);
    await expect(
      runProtocolDemo(client, {
        ...options,
        protocolVersion: 5,
        properties: true,
      })
    ).rejects.toThrow('properties');
    expect(remove).toHaveBeenCalled();
    expect(unsubscribe).toHaveBeenCalled();
  }
);

test.each([
  (message) => ({ ...message, bytes: encodeUtf8('wrong') }),
  (message) => ({ ...message, qos: 2 }),
  (message) => ({ ...message, retained: true }),
])('bad round-trip data fails', async (transform) => {
  const { client } = fixture(transform);
  await expect(runProtocolDemo(client, options)).rejects.toThrow('mismatch');
});

test('wrong-topic messages and publish ACK alone cannot pass', async () => {
  jest.useFakeTimers();
  try {
    const { client, unsubscribe, remove } = fixture((message) => ({
      ...message,
      topic: 'other',
    }));
    const result = runProtocolDemo(client, options).catch((error) => error);
    await jest.advanceTimersByTimeAsync(15001);
    expect(await result).toEqual(
      expect.objectContaining({ message: expect.stringContaining('timed out') })
    );
    expect(remove).toHaveBeenCalled();
    expect(unsubscribe).toHaveBeenCalled();
    expect(jest.getTimerCount()).toBe(0);
  } finally {
    jest.useRealTimers();
  }
});

test('cancellation stops test and removes its listener/subscription', async () => {
  const controller = new AbortController();
  const { client, unsubscribe, remove } = fixture(() => {
    controller.abort();
    return null;
  });
  await expect(
    runProtocolDemo(client, { ...options, signal: controller.signal })
  ).rejects.toThrow('cancelled');
  expect(remove).toHaveBeenCalled();
  expect(unsubscribe).toHaveBeenCalled();
});

test('received echo without publish completion still times out', async () => {
  jest.useFakeTimers();
  try {
    const { client, unsubscribe } = fixture();
    const publish = client.publishText;
    client.publishText = async (...args) => {
      await publish(...args);
      return new Promise(() => {});
    };
    const result = runProtocolDemo(client, options).catch((error) => error);
    await jest.advanceTimersByTimeAsync(15001);
    expect((await result).message).toContain('timed out');
    expect(unsubscribe).toHaveBeenCalled();
    expect(jest.getTimerCount()).toBe(0);
  } finally {
    jest.useRealTimers();
  }
});

test('subscribe failure removes listener without publishing', async () => {
  const { client, remove } = fixture();
  client.subscribe.mockRejectedValue(new Error('subscribe refused'));
  await expect(runProtocolDemo(client, options)).rejects.toThrow(
    'subscribe refused'
  );
  expect(remove).toHaveBeenCalled();
  expect(client.publishText).not.toHaveBeenCalled();
});

test('publish failure unsubscribes and clears timers', async () => {
  jest.useFakeTimers();
  try {
    const { client, unsubscribe, remove } = fixture();
    client.publishText.mockRejectedValue(new Error('publish refused'));
    await expect(runProtocolDemo(client, options)).rejects.toThrow(
      'publish refused'
    );
    expect(remove).toHaveBeenCalled();
    expect(unsubscribe).toHaveBeenCalled();
    expect(jest.getTimerCount()).toBe(0);
  } finally {
    jest.useRealTimers();
  }
});

test('rejects v5-only demo on 4 and unsupported protocol 3 before native calls', async () => {
  const { client } = fixture();
  await expect(
    runProtocolDemo(client, { ...options, properties: true })
  ).rejects.toThrow('require');
  await expect(
    runProtocolDemo(client, { ...options, protocolVersion: 3 })
  ).rejects.toThrow('supported');
  expect(client.subscribe).not.toHaveBeenCalled();
});

test('switching version overrides runtime version and clears only incompatible session fields', () => {
  expect(
    demoConnectionOptions(
      {
        protocolVersion: 5,
        cleanStart: false,
        receiveMaximum: 7,
        userProperties: [],
        sessionExpiryIntervalSeconds: 10,
        host: 'local',
      },
      4
    )
  ).toEqual({ protocolVersion: 4, host: 'local' });
  expect(
    demoConnectionOptions({ cleanSession: false, host: 'local' }, 5)
  ).toEqual({ protocolVersion: 5, host: 'local' });
});
