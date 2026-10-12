const mockSnapshot = {
  instanceId: 'mqtt-test',
  revision: 0,
  generation: 1,
  state: 'connected',
  lastConnAck: null,
  subscriptionsRevision: 0,
  subscriptionsStatus: 'ready',
  deliveryStatus: 'active',
};

function mockCreateNative() {
  return {
    getCapabilities: jest.fn(),
    openSession: jest.fn(async () => mockSnapshot),
    connect: jest.fn(async () => ({
      snapshot: mockSnapshot,
      ack: { sessionPresent: false, reasonCode: 0 },
    })),
    disconnect: jest.fn(async () => ({
      ...mockSnapshot,
      state: 'disconnected',
    })),
    close: jest.fn(async () => ({ ...mockSnapshot, state: 'closed' })),
    getSnapshot: jest.fn(async () => mockSnapshot),
    setSubscription: jest.fn(async () => ({
      generation: 1,
      intentRevision: 1,
      grantedQos: 1,
      reasonCode: 0,
    })),
    removeSubscription: jest.fn(async () => ({ intentRevision: 2 })),
    awaitSubscriptions: jest.fn(async () => mockSnapshot),
    publish: jest.fn(async () => undefined),
    ackMessageBatch: jest.fn(async () => undefined),
    onSessionSnapshot: jest.fn(() => jest.fn()),
    onMessageBatch: jest.fn(() => jest.fn()),
    onDiagnostic: jest.fn(() => jest.fn()),
  };
}

jest.mock('../NativeReactNativeMqtt', () => ({
  __esModule: true,
  default: mockCreateNative(),
}));

import { createMqttClient } from '../client';

const mockNative = jest.requireMock('../NativeReactNativeMqtt')
  .default as ReturnType<typeof mockCreateNative>;

describe('native subscription coordination', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  test('deduplicates sequential connected connect and rejects connect during disconnect', async () => {
    const client = createMqttClient({ host: 'broker.example', port: 1883 });
    const [firstAck, concurrentAck] = await Promise.all([
      client.connect(),
      client.connect(),
    ]);
    expect(concurrentAck).toEqual(firstAck);
    const secondAck = await client.connect();
    expect(secondAck).toEqual(firstAck);
    expect(mockNative.connect).toHaveBeenCalledTimes(1);

    let release!: (value: typeof mockSnapshot) => void;
    mockNative.disconnect.mockImplementationOnce(
      () =>
        new Promise<typeof mockSnapshot>((resolve) => {
          release = resolve;
        })
    );
    const disconnect = client.disconnect();
    await expect(client.connect()).rejects.toMatchObject({
      code: 'ERR_OPERATION_IN_PROGRESS',
    });
    release({ ...mockSnapshot, state: 'disconnected' });
    await disconnect;
    await client.dispose();
  });

  test('maps native rejection metadata without losing code/message or zero reason codes', async () => {
    const cases: Array<{
      userInfo?: unknown;
      reasonCode?: number;
      details?: {
        operation?: string;
        generation?: number;
        reasonString?: string;
        outcome?: 'not-sent' | 'rejected' | 'unknown';
      };
    }> = [
      {},
      { userInfo: null },
      { userInfo: { reasonCode: 'not-a-number' } },
      {
        userInfo: {
          operation: 'connect',
          generation: 7,
          reasonCode: 0,
          reasonString: '',
          outcome: 'rejected',
        },
        reasonCode: 0,
        details: {
          operation: 'connect',
          generation: 7,
          reasonString: '',
          outcome: 'rejected',
        },
      },
    ];

    for (const value of cases) {
      const client = createMqttClient({ host: 'broker.example', port: 1883 });
      mockNative.connect.mockRejectedValueOnce({
        code: 'ERR_CONNECTION_REFUSED',
        message: 'broker refused connection',
        ...(value.userInfo === undefined ? {} : { userInfo: value.userInfo }),
      });
      await expect(client.connect()).rejects.toMatchObject({
        code: 'ERR_CONNECTION_REFUSED',
        message: 'broker refused connection',
        ...(value.reasonCode === undefined
          ? { reasonCode: undefined }
          : { reasonCode: value.reasonCode }),
        ...(value.details ?? {}),
      });
      await client.dispose();
    }
  });

  test('publishes bytes and canonical Base64 without altering binary payloads', async () => {
    const client = createMqttClient({ host: 'broker.example', port: 1883 });
    await client.connect();
    await client.publishBytes('binary/status', new Uint8Array([0, 255, 1]));
    await client.publishBase64('binary/status', 'AP8B');
    expect(mockNative.publish).toHaveBeenNthCalledWith(
      1,
      client.instanceId,
      expect.objectContaining({
        topic: 'binary/status',
        payloadBase64: 'AP8B',
      })
    );
    expect(mockNative.publish).toHaveBeenNthCalledWith(
      2,
      client.instanceId,
      expect.objectContaining({
        topic: 'binary/status',
        payloadBase64: 'AP8B',
      })
    );
    await client.dispose();
  });

  test('publishes immediately after connect, isolates instances, and drops late batches after dispose', async () => {
    const first = createMqttClient({ host: 'broker.example', port: 1883 });
    const second = createMqttClient({ host: 'broker.example', port: 1883 });
    await Promise.all([first.connect(), second.connect()]);
    await Promise.all([
      first.publishText('first/status', 'one'),
      second.publishText('second/status', 'two'),
    ]);
    expect(mockNative.publish).toHaveBeenCalledWith(
      first.instanceId,
      expect.objectContaining({ topic: 'first/status' })
    );
    expect(mockNative.publish).toHaveBeenCalledWith(
      second.instanceId,
      expect.objectContaining({ topic: 'second/status' })
    );

    const firstBatch = (mockNative.onMessageBatch as jest.Mock).mock
      .calls[0]?.[0] as (batch: unknown) => void;
    await first.dispose();
    mockNative.ackMessageBatch.mockClear();
    firstBatch({
      instanceId: first.instanceId,
      generation: 1,
      streamEpoch: 1,
      batchId: 99,
      messages: [],
    });
    expect(mockNative.ackMessageBatch).not.toHaveBeenCalled();
    await second.dispose();
  });

  test('shares one in-flight subscribe and releases only after the last handle', async () => {
    const client = createMqttClient({ host: 'broker.example', port: 1883 });
    await client.connect();

    const [first, second] = await Promise.all([
      client.subscribe('devices/+/status', { qos: 1 }),
      client.subscribe('devices/+/status', { qos: 1 }),
    ]);

    expect(mockNative.setSubscription).toHaveBeenCalledTimes(1);
    expect(first.qos).toBe(1);
    expect(second.qos).toBe(1);

    await first.unsubscribe();
    expect(mockNative.removeSubscription).not.toHaveBeenCalled();
    await second.unsubscribe();
    expect(mockNative.removeSubscription).toHaveBeenCalledTimes(1);
    await Promise.all([second.unsubscribe(), second.unsubscribe()]);
    expect(mockNative.removeSubscription).toHaveBeenCalledTimes(1);
  });

  test('dispatches overlapping filters once per client message and preserves broker copies', async () => {
    const client = createMqttClient({ host: 'broker.example', port: 1883 });
    const messages: string[] = [];
    client.onMessage((message) => messages.push(message.topic));
    await client.connect();
    const first = await client.subscribe('devices/+/status', { qos: 1 });
    const second = await client.subscribe('devices/a/status', { qos: 1 });

    const onBatch = (mockNative.onMessageBatch as jest.Mock).mock
      .calls[0]?.[0] as (batch: unknown) => void;
    const message = {
      instanceId: client.instanceId,
      generation: 1,
      topic: 'devices/a/status',
      payloadBase64: 'cA==',
      qos: 1,
      retained: false,
    };
    onBatch({
      instanceId: client.instanceId,
      generation: 1,
      streamEpoch: 1,
      batchId: 1,
      messages: [message],
    });
    onBatch({
      instanceId: client.instanceId,
      generation: 1,
      streamEpoch: 1,
      batchId: 2,
      messages: [message],
    });

    expect(messages).toEqual(['devices/a/status', 'devices/a/status']);
    expect(mockNative.setSubscription).toHaveBeenCalledTimes(2);
    await Promise.all([first.unsubscribe(), second.unsubscribe()]);
    await client.dispose();
  });

  test('restores the local handle when native removal fails so it can be retried', async () => {
    const client = createMqttClient({ host: 'broker.example', port: 1883 });
    await client.connect();
    const handle = await client.subscribe('retry/status', { qos: 1 });
    mockNative.removeSubscription.mockRejectedValueOnce(
      new Error('temporary unsubscribe failure')
    );

    await expect(handle.unsubscribe()).rejects.toMatchObject({
      code: 'ERR_UNKNOWN',
    });
    await handle.unsubscribe();
    expect(mockNative.removeSubscription).toHaveBeenCalledTimes(2);
    await client.dispose();
  });

  test('rejects an in-flight conflicting subscription without changing it', async () => {
    const client = createMqttClient({ host: 'broker.example', port: 1883 });
    await client.connect();
    const first = client.subscribe('devices/+/status', { qos: 1 });

    await expect(
      client.subscribe('devices/+/status', { qos: 2 })
    ).rejects.toMatchObject({ code: 'ERR_SUBSCRIPTION_CONFLICT' });
    await first;
    expect(mockNative.setSubscription).toHaveBeenCalledTimes(1);
  });

  test('accepts offline release and submits the tombstone before reconnect', async () => {
    const client = createMqttClient({ host: 'broker.example', port: 1883 });
    await client.connect();
    const handle = await client.subscribe('offline/status', { qos: 1 });
    await client.disconnect();

    await handle.unsubscribe();
    expect(mockNative.removeSubscription).toHaveBeenCalledTimes(1);

    await client.connect();
    await client.ready();
    expect(mockNative.removeSubscription).toHaveBeenCalledTimes(1);
    expect(mockNative.removeSubscription).toHaveBeenCalledWith(
      client.instanceId,
      'offline/status',
      expect.any(Number)
    );
    await client.dispose();
  });

  test('assigns monotonic intent revisions and ready waits for pending intent', async () => {
    const client = createMqttClient({ host: 'broker.example', port: 1883 });
    await client.connect();
    let release!: (value: {
      generation: number;
      intentRevision: number;
      grantedQos: number;
      reasonCode: number;
    }) => void;
    mockNative.setSubscription.mockImplementationOnce(
      () => new Promise((resolve) => (release = resolve))
    );
    const subscription = client.subscribe('pending/status', { qos: 1 });
    const ready = client.ready();
    await Promise.resolve();
    expect(mockNative.awaitSubscriptions).not.toHaveBeenCalled();
    release({ generation: 1, intentRevision: 1, grantedQos: 1, reasonCode: 0 });
    await subscription;
    await ready;
    expect(mockNative.awaitSubscriptions).toHaveBeenCalledWith(
      client.instanceId,
      1,
      1
    );

    await Promise.all([
      client.subscribe('first/status', { qos: 1 }),
      client.subscribe('second/status', { qos: 1 }),
    ]);
    expect(
      mockNative.setSubscription.mock.calls
        .slice(-2)
        .map((call) => (call as unknown[])[3])
    ).toEqual([2, 3]);
    await client.dispose();
  });

  test('dispose waits for pending subscription and rejects its late success', async () => {
    const client = createMqttClient({ host: 'broker.example', port: 1883 });
    await client.connect();
    let release!: (value: {
      generation: number;
      intentRevision: number;
      grantedQos: number;
      reasonCode: number;
    }) => void;
    mockNative.setSubscription.mockImplementationOnce(
      () => new Promise((resolve) => (release = resolve))
    );
    const subscription = client.subscribe('dispose/status', { qos: 1 });
    const dispose = client.dispose();
    await Promise.resolve();
    expect(mockNative.close).not.toHaveBeenCalled();
    release({ generation: 1, intentRevision: 1, grantedQos: 1, reasonCode: 0 });
    await expect(subscription).rejects.toMatchObject({ code: 'ERR_DISPOSED' });
    await dispose;
    expect(mockNative.close).toHaveBeenCalledTimes(1);
    expect(client.state).toBe('closed');
  });

  test('dispose waits for a publish already handed to Native', async () => {
    const client = createMqttClient({ host: 'broker.example', port: 1883 });
    await client.connect();
    let release!: () => void;
    mockNative.publish.mockImplementationOnce(
      () =>
        new Promise<undefined>((resolve) => {
          release = () => resolve(undefined);
        })
    );
    const publish = client.publishText('publish/status', 'payload');
    const dispose = client.dispose();
    await Promise.resolve();
    expect(mockNative.close).not.toHaveBeenCalled();
    release();
    await publish;
    await dispose;
    expect(mockNative.close).toHaveBeenCalledTimes(1);
    expect(client.state).toBe('closed');
  });

  test('dispose waits for an in-flight graceful disconnect', async () => {
    const client = createMqttClient({ host: 'broker.example', port: 1883 });
    await client.connect();
    let release!: (value: typeof mockSnapshot) => void;
    mockNative.disconnect.mockImplementationOnce(
      () =>
        new Promise<typeof mockSnapshot>((resolve) => {
          release = resolve;
        })
    );
    const disconnect = client.disconnect();
    const dispose = client.dispose();
    await Promise.resolve();
    expect(mockNative.close).not.toHaveBeenCalled();
    release({ ...mockSnapshot, state: 'disconnected' });
    await disconnect;
    await dispose;
    expect(mockNative.close).toHaveBeenCalledTimes(1);
    expect(client.state).toBe('closed');
  });

  test('isolates synchronous and thenable listener failures and always ACKs the batch', async () => {
    const client = createMqttClient({ host: 'broker.example', port: 1883 });
    const diagnostics: string[] = [];
    client.onDiagnostic((diagnostic) => diagnostics.push(diagnostic.message));
    client.onMessage(() => {
      throw new Error('sync listener failure');
    });
    client.onMessage(() => Promise.reject(new Error('async listener failure')));
    await client.connect();

    const onBatch = (mockNative.onMessageBatch as jest.Mock).mock
      .calls[0]?.[0] as unknown as (batch: unknown) => void;
    onBatch({
      instanceId: client.instanceId,
      generation: 1,
      streamEpoch: 1,
      batchId: 1,
      messages: [
        {
          instanceId: client.instanceId,
          generation: 1,
          topic: 'devices/a/status',
          payloadBase64: 'cA==',
          qos: 1,
          retained: false,
        },
      ],
    });
    await Promise.resolve();

    expect(mockNative.ackMessageBatch).toHaveBeenCalledWith(
      client.instanceId,
      1,
      1
    );
    expect(diagnostics).toEqual(
      expect.arrayContaining([
        'sync listener failure',
        'async listener failure',
      ])
    );
    await client.dispose();
  });

  test('isolates function-shaped thenable listener failures', async () => {
    const client = createMqttClient({ host: 'broker.example', port: 1883 });
    const diagnostics: string[] = [];
    client.onDiagnostic((diagnostic) => diagnostics.push(diagnostic.message));
    client.onMessage(() => {
      const thenable = (() => undefined) as (() => undefined) & {
        then: (_resolve: () => void, reject: (error: Error) => void) => void;
      };
      thenable.then = (_resolve, reject) =>
        reject(new Error('function thenable failure'));
      return thenable;
    });
    await client.connect();

    const onBatch = (mockNative.onMessageBatch as jest.Mock).mock
      .calls[0]?.[0] as unknown as (batch: unknown) => void;
    onBatch({
      instanceId: client.instanceId,
      generation: 1,
      streamEpoch: 1,
      batchId: 2,
      messages: [
        {
          instanceId: client.instanceId,
          generation: 1,
          topic: 'devices/a/status',
          payloadBase64: 'cA==',
          qos: 1,
          retained: false,
        },
      ],
    });
    await Promise.resolve();
    await Promise.resolve();

    expect(diagnostics).toContain('function thenable failure');
    expect(mockNative.ackMessageBatch).toHaveBeenCalledWith(
      client.instanceId,
      1,
      2
    );
    await client.dispose();
  });

  test('retries a failed batch ACK with the exact same token, including an empty batch', async () => {
    const client = createMqttClient({ host: 'broker.example', port: 1883 });
    mockNative.ackMessageBatch
      .mockRejectedValueOnce(new Error('temporary ACK failure'))
      .mockResolvedValueOnce(undefined);
    await client.connect();
    const onBatch = (mockNative.onMessageBatch as jest.Mock).mock
      .calls[0]?.[0] as unknown as (batch: unknown) => void;
    onBatch({
      instanceId: client.instanceId,
      generation: 1,
      streamEpoch: 7,
      batchId: 11,
      messages: [],
    });
    await new Promise<void>((resolve) => setTimeout(() => resolve(), 0));
    await new Promise<void>((resolve) => setTimeout(() => resolve(), 0));
    expect(mockNative.ackMessageBatch).toHaveBeenCalledTimes(2);
    expect(mockNative.ackMessageBatch).toHaveBeenNthCalledWith(
      1,
      client.instanceId,
      7,
      11
    );
    expect(mockNative.ackMessageBatch).toHaveBeenNthCalledWith(
      2,
      client.instanceId,
      7,
      11
    );
    await client.dispose();
  });

  test('stalls delivery once and recovers only when the same batch is ACKed', async () => {
    jest.useFakeTimers();
    let releaseAck!: () => void;
    mockNative.ackMessageBatch.mockImplementationOnce(
      () =>
        new Promise<undefined>((resolve) => {
          releaseAck = () => resolve(undefined);
        })
    );
    const client = createMqttClient({
      host: 'broker.example',
      port: 1883,
      delivery: { batchAckTimeoutMs: 10 },
    });
    const snapshots: string[] = [];
    const diagnostics: string[] = [];
    client.onStateChange((value) => snapshots.push(value.deliveryStatus));
    client.onDiagnostic((value) => diagnostics.push(value.code));
    await client.connect();
    const onBatch = (mockNative.onMessageBatch as jest.Mock).mock
      .calls[0]?.[0] as (batch: unknown) => void;
    onBatch({
      instanceId: client.instanceId,
      generation: 1,
      streamEpoch: 3,
      batchId: 4,
      messages: [],
    });

    jest.advanceTimersByTime(10);
    expect(snapshots).toContain('stalled');
    expect(diagnostics).toEqual(['DELIVERY_STALLED']);
    jest.advanceTimersByTime(100);
    expect(diagnostics).toEqual(['DELIVERY_STALLED']);

    releaseAck();
    await Promise.resolve();
    expect(snapshots.at(-1)).toBe('active');
    expect(mockNative.ackMessageBatch).toHaveBeenCalledWith(
      client.instanceId,
      3,
      4
    );
    await client.dispose();
  });

  test('does not deliver a later batch while the prior token is stalled', async () => {
    jest.useFakeTimers();
    let releaseAck!: () => void;
    mockNative.ackMessageBatch.mockImplementationOnce(
      () =>
        new Promise<undefined>(
          (resolve) => (releaseAck = () => resolve(undefined))
        )
    );
    const client = createMqttClient({
      host: 'broker.example',
      port: 1883,
      delivery: { batchAckTimeoutMs: 10 },
    });
    const listener = jest.fn();
    client.onMessage(listener);
    await client.connect();
    const onBatch = (mockNative.onMessageBatch as jest.Mock).mock
      .calls[0]?.[0] as (batch: unknown) => void;
    onBatch({
      instanceId: client.instanceId,
      generation: 1,
      streamEpoch: 4,
      batchId: 1,
      messages: [],
    });
    jest.advanceTimersByTime(10);
    onBatch({
      instanceId: client.instanceId,
      generation: 1,
      streamEpoch: 4,
      batchId: 2,
      messages: [
        {
          instanceId: client.instanceId,
          generation: 1,
          topic: 'stalled/topic',
          payloadBase64: '',
          qos: 0,
          retained: false,
        },
      ],
    });
    expect(listener).not.toHaveBeenCalled();
    expect(mockNative.ackMessageBatch).toHaveBeenCalledTimes(1);
    releaseAck();
    await Promise.resolve();
    await client.dispose();
  });

  test('cleans native event subscriptions and returns an inactive listener after dispose', async () => {
    const client = createMqttClient({ host: 'broker.example', port: 1883 });
    const stateListener = jest.fn();
    client.onStateChange(stateListener);
    await client.connect();
    await client.dispose();

    const inactive = client.onStateChange(stateListener);
    inactive.remove();
    const results = [
      mockNative.onSessionSnapshot.mock.results.at(-1)?.value,
      mockNative.onMessageBatch.mock.results.at(-1)?.value,
      mockNative.onDiagnostic.mock.results.at(-1)?.value,
    ] as jest.Mock[];
    results.forEach((remove) => expect(remove).toHaveBeenCalledTimes(1));
  });

  test('routes snapshots by instance, ignores older revisions, and recovers through getSnapshot', async () => {
    const client = createMqttClient({ host: 'broker.example', port: 1883 });
    const states: number[] = [];
    client.onStateChange((value) => states.push(value.revision));
    await client.connect();
    const onSnapshot = (mockNative.onSessionSnapshot as jest.Mock).mock
      .calls[0]?.[0] as (value: typeof mockSnapshot) => void;
    onSnapshot({
      ...mockSnapshot,
      instanceId: client.instanceId,
      revision: 10,
    });
    onSnapshot({ ...mockSnapshot, instanceId: client.instanceId, revision: 9 });
    onSnapshot({ ...mockSnapshot, instanceId: 'another-client', revision: 11 });
    expect(states).toContain(10);
    expect(states).not.toContain(9);
    expect(states).not.toContain(11);

    await client.getSnapshot();
    expect(mockNative.getSnapshot).toHaveBeenCalledWith(client.instanceId);
    await client.dispose();
  });
});
