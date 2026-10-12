import NativeReactNativeMqtt, {
  type WireBatch,
  type WireConnAck,
  type WireDiagnostic,
  type WireMessage,
  type WireSnapshot,
  type WireSubscriptionResult,
} from './NativeReactNativeMqtt';
import { MqttError, type MqttErrorCode } from './errors';
import { decodeBase64, encodeBase64, encodeUtf8 } from './helpers';
import type {
  ConnAck,
  ConnectionState,
  Diagnostic,
  Listener,
  Message,
  MqttClient,
  MqttOptions,
  SessionSnapshot,
  SubscribeOptions,
  Subscription,
} from './types';
import {
  normalizeOptions,
  normalizeSubscribeOptions,
  toWirePublishRequest,
  toWireSessionOptions,
  toWireSubscribeOptions,
  validateTopicFilter,
} from './validation';

type SnapshotListener = (snapshot: SessionSnapshot) => void;
type MessageListener = (message: Message) => void;
type DiagnosticListener = (diagnostic: Diagnostic) => void;
type NativeErrorShape = {
  code?: string;
  message?: string;
  userInfo?: {
    operation?: unknown;
    generation?: unknown;
    reasonCode?: unknown;
    reasonString?: unknown;
    outcome?: unknown;
  } | null;
};

const errorCodes = new Set<MqttErrorCode>([
  'ERR_INVALID_OPTIONS',
  'ERR_INVALID_PAYLOAD',
  'ERR_NATIVE_UNAVAILABLE',
  'ERR_CONTRACT_MISMATCH',
  'ERR_NOT_CONNECTED',
  'ERR_DISPOSED',
  'ERR_OPERATION_IN_PROGRESS',
  'ERR_OPERATION_SUPERSEDED',
  'ERR_CONNECT_TIMEOUT',
  'ERR_OPERATION_TIMEOUT',
  'ERR_CONNECTION_REFUSED',
  'ERR_CONNECTION_LOST',
  'ERR_SUBSCRIPTION_CONFLICT',
  'ERR_SUBSCRIBE_FAILED',
  'ERR_UNSUBSCRIBE_FAILED',
  'ERR_PUBLISH_FAILED',
  'ERR_TLS',
  'ERR_CLEANUP_FAILED',
  'ERR_UNKNOWN',
]);

function mapError(value: unknown, operation?: string): MqttError {
  if (value instanceof MqttError) return value;
  const shape = (
    value !== null && typeof value === 'object' ? value : {}
  ) as NativeErrorShape;
  const code =
    typeof shape.code === 'string' &&
    errorCodes.has(shape.code as MqttErrorCode)
      ? (shape.code as MqttErrorCode)
      : 'ERR_UNKNOWN';
  const message =
    typeof shape.message === 'string'
      ? shape.message
      : `${operation ?? 'operation'} failed`;
  const userInfo = shape.userInfo;
  const reasonCode =
    userInfo !== null &&
    userInfo !== undefined &&
    typeof userInfo.reasonCode === 'number'
      ? userInfo.reasonCode
      : undefined;
  const reasonString =
    userInfo !== null &&
    userInfo !== undefined &&
    typeof userInfo.reasonString === 'string'
      ? userInfo.reasonString
      : undefined;
  const nativeOperation =
    userInfo !== null &&
    userInfo !== undefined &&
    typeof userInfo.operation === 'string'
      ? userInfo.operation
      : undefined;
  const generation =
    userInfo !== null &&
    userInfo !== undefined &&
    typeof userInfo.generation === 'number' &&
    Number.isSafeInteger(userInfo.generation)
      ? userInfo.generation
      : undefined;
  const outcome =
    userInfo !== null &&
    userInfo !== undefined &&
    (userInfo.outcome === 'not-sent' ||
      userInfo.outcome === 'rejected' ||
      userInfo.outcome === 'unknown')
      ? userInfo.outcome
      : 'unknown';
  return new MqttError(code, message, {
    operation: nativeOperation ?? operation,
    generation,
    reasonCode,
    reasonString,
    outcome,
  });
}
function snapshotFromWire(value: WireSnapshot): SessionSnapshot {
  return {
    ...value,
    state: value.state as ConnectionState,
    subscriptionsStatus:
      value.subscriptionsStatus as SessionSnapshot['subscriptionsStatus'],
    deliveryStatus: value.deliveryStatus as SessionSnapshot['deliveryStatus'],
    lastConnAck:
      value.lastConnAck === null ? null : connAckFromWire(value.lastConnAck),
  };
}
function connAckFromWire(value: WireConnAck): ConnAck {
  return {
    sessionPresent: value.sessionPresent,
    assignedClientId: value.assignedClientId,
    reasonCode: value.reasonCode,
    reasonString: value.reasonString,
    userProperties: value.userProperties?.map((pair) => ({
      key: pair.key,
      value: pair.value,
    })),
  };
}
function messageFromWire(value: WireMessage): Message {
  return {
    instanceId: value.instanceId,
    generation: value.generation,
    topic: value.topic,
    bytes: decodeBase64(value.payloadBase64),
    qos: value.qos as Message['qos'],
    retained: value.retained,
    userProperties: value.userProperties?.map((pair) => ({
      key: pair.key,
      value: pair.value,
    })),
    contentType: value.contentType,
    responseTopic: value.responseTopic,
    correlationData:
      value.correlationDataBase64 === undefined
        ? undefined
        : decodeBase64(value.correlationDataBase64),
    messageExpiryIntervalSeconds: value.messageExpiryIntervalSeconds,
    payloadFormatIndicator:
      value.payloadFormatIndicator as Message['payloadFormatIndicator'],
  };
}
function diagnosticFromWire(value: WireDiagnostic): Diagnostic {
  return { ...value, code: value.code as Diagnostic['code'] };
}
function randomClientId(): string {
  const alphabet =
    'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
  let result = 'rn';
  while (result.length < 23)
    result += alphabet[Math.floor(Math.random() * alphabet.length)] ?? 'x';
  return result;
}

export function createMqttClient(input: MqttOptions): MqttClient {
  const options = normalizeOptions(input);
  const instanceId = `mqtt-${randomClientId()}`;
  const clientId =
    options.clientId === undefined ? randomClientId() : options.clientId;
  const sessionOptions = { ...options, clientId } as MqttOptions;
  let snapshot: SessionSnapshot = {
    instanceId,
    revision: 0,
    generation: 0,
    state: 'idle',
    lastConnAck: null,
    subscriptionsRevision: 0,
    subscriptionsStatus: 'ready',
    deliveryStatus: 'active',
  };
  let openPromise: Promise<void> | undefined;
  let opened = false;
  let connectPromise: Promise<ConnAck> | undefined;
  let lastConnAck: ConnAck | undefined;
  let disconnectPromise: Promise<void> | undefined;
  let disposePromise: Promise<void> | undefined;
  let nativeBound = false;
  let nextIntentRevision = snapshot.subscriptionsRevision;
  const stateListeners = new Set<SnapshotListener>();
  const messageListeners = new Set<MessageListener>();
  const diagnosticListeners = new Set<DiagnosticListener>();
  const nativeListeners: Array<Listener | (() => void)> = [];
  const ackTimers = new Map<string, ReturnType<typeof setTimeout>>();
  const ackRetryTimers = new Map<string, ReturnType<typeof setTimeout>>();
  const ackRetryAttempts = new Map<string, number>();
  const subscriptions = new Map<
    string,
    { options: SubscribeOptions; refs: number; qos: Subscription['qos'] }
  >();
  const pendingSubscriptions = new Map<
    string,
    { options: SubscribeOptions; promise: Promise<number> }
  >();
  const pendingOfflineRemovals = new Map<string, number>();
  const pendingIntentOperations = new Set<Promise<unknown>>();
  const pendingNativeOperations = new Set<Promise<unknown>>();

  const emitSnapshot = (next: SessionSnapshot): void => {
    if (next.revision < snapshot.revision) return;
    snapshot = next;
    nextIntentRevision = Math.max(
      nextIntentRevision,
      next.subscriptionsRevision
    );
    for (const listener of [...stateListeners]) {
      try {
        listener(snapshot);
      } catch (error) {
        emitDiagnostic({
          instanceId,
          generation: snapshot.generation,
          code: 'LISTENER_ERROR',
          message: error instanceof Error ? error.message : String(error),
        });
      }
    }
  };
  const emitNativeLifecycleSnapshot = (value: WireSnapshot): void => {
    const next = snapshotFromWire(value);
    emitSnapshot(
      next.revision <= snapshot.revision
        ? { ...next, revision: snapshot.revision + 1 }
        : next
    );
  };
  const emitDiagnostic = (diagnostic: Diagnostic): void => {
    for (const listener of [...diagnosticListeners]) {
      try {
        listener(diagnostic);
      } catch {
        /* diagnostic listeners are isolated */
      }
    }
  };
  const batchKey = (streamEpoch: number, batchId: number): string =>
    `${streamEpoch}:${batchId}`;
  const allocateIntentRevision = (): number => {
    nextIntentRevision =
      Math.max(nextIntentRevision, snapshot.subscriptionsRevision) + 1;
    return nextIntentRevision;
  };
  const trackIntentOperation = <T>(promise: Promise<T>): Promise<T> => {
    pendingIntentOperations.add(promise);
    promise.then(
      () => pendingIntentOperations.delete(promise),
      () => pendingIntentOperations.delete(promise)
    );
    return promise;
  };
  const trackNativeOperation = <T>(promise: Promise<T>): Promise<T> => {
    pendingNativeOperations.add(promise);
    promise.then(
      () => pendingNativeOperations.delete(promise),
      () => pendingNativeOperations.delete(promise)
    );
    return promise;
  };
  const flushOfflineRemovals = (): void => {
    if (snapshot.state !== 'connected' || pendingOfflineRemovals.size === 0)
      return;
    const native = bindNative();
    for (const [topicFilter, intentRevision] of [
      ...pendingOfflineRemovals.entries(),
    ]) {
      pendingOfflineRemovals.delete(topicFilter);
      const operation = trackIntentOperation(
        native
          .removeSubscription(instanceId, topicFilter, intentRevision)
          .catch((error: unknown) => {
            pendingOfflineRemovals.set(topicFilter, intentRevision);
            throw error;
          })
      );
      operation.catch(() => undefined);
    }
  };
  const acknowledgeBatch = (
    native: NonNullable<typeof NativeReactNativeMqtt>,
    streamEpoch: number,
    batchId: number
  ): void => {
    const key = batchKey(streamEpoch, batchId);
    const previous = ackTimers.get(key);
    if (previous !== undefined) clearTimeout(previous);
    const timeout = setTimeout(() => {
      if (!ackTimers.has(key) || snapshot.state === 'closed') return;
      emitSnapshot({
        ...snapshot,
        deliveryStatus: 'stalled',
        revision: snapshot.revision + 1,
      });
      emitDiagnostic({
        instanceId,
        generation: snapshot.generation,
        code: 'DELIVERY_STALLED',
        message: `Message batch ${key} acknowledgement timed out`,
      });
    }, options.delivery?.batchAckTimeoutMs ?? 15000);
    // Install the watchdog before submitting: a synchronously-resolved native
    // promise must still be able to clear the exact timer it owns.
    ackTimers.set(key, timeout);
    const submit = (attempt: number): void => {
      native
        .ackMessageBatch(instanceId, streamEpoch, batchId)
        .then(() => {
          ackTimers.delete(key);
          ackRetryAttempts.delete(key);
          ackRetryTimers.delete(key);
          if (snapshot.deliveryStatus === 'stalled')
            emitSnapshot({
              ...snapshot,
              deliveryStatus: 'active',
              revision: snapshot.revision + 1,
            });
        })
        .catch((error: unknown) => {
          if (attempt === 0 && snapshot.state !== 'closed') {
            ackRetryAttempts.set(key, 1);
            const retry = setTimeout(() => {
              ackRetryTimers.delete(key);
              if (snapshot.state !== 'closed') submit(1);
            }, 0);
            ackRetryTimers.set(key, retry);
            return;
          }
          emitDiagnostic({
            instanceId,
            generation: snapshot.generation,
            code: 'DELIVERY_STALLED',
            message: mapError(error, 'ackMessageBatch').message,
          });
        });
    };
    submit(ackRetryAttempts.get(key) ?? 0);
  };
  const nativeOrThrow = (): NonNullable<typeof NativeReactNativeMqtt> => {
    if (NativeReactNativeMqtt == null)
      throw new MqttError(
        'ERR_NATIVE_UNAVAILABLE',
        'ReactNativeMqtt TurboModule is unavailable; rebuild the app with the native module installed',
        { outcome: 'not-sent' }
      );
    return NativeReactNativeMqtt;
  };
  const bindNative = (): NonNullable<typeof NativeReactNativeMqtt> => {
    const native = nativeOrThrow();
    if (!nativeBound) {
      nativeBound = true;
      nativeListeners.push(
        native.onSessionSnapshot((value) => {
          if (snapshot.state === 'closed' || value.instanceId !== instanceId)
            return;
          emitSnapshot(snapshotFromWire(value));
        })
      );
      nativeListeners.push(
        native.onMessageBatch((batch: WireBatch) => {
          if (snapshot.state === 'closed' || batch.instanceId !== instanceId)
            return;
          // Keep the native in-flight token reserved while stalled. A late or
          // duplicate event must not deliver another batch or release capacity
          // until the original token is acknowledged.
          if (snapshot.deliveryStatus === 'stalled') return;
          try {
            for (const value of batch.messages) {
              if (value.instanceId !== instanceId) continue;
              let message: Message;
              try {
                message = messageFromWire(value);
              } catch (error) {
                emitDiagnostic({
                  instanceId,
                  generation: snapshot.generation,
                  code: 'LISTENER_ERROR',
                  message:
                    error instanceof Error ? error.message : String(error),
                });
                continue;
              }
              for (const listener of [...messageListeners]) {
                try {
                  const result = (listener as (value: Message) => unknown)(
                    message
                  );
                  if (
                    result !== null &&
                    (typeof result === 'object' || typeof result === 'function')
                  ) {
                    Promise.resolve(result as PromiseLike<unknown>).catch(
                      (error: unknown) =>
                        emitDiagnostic({
                          instanceId,
                          generation: snapshot.generation,
                          code: 'LISTENER_ERROR',
                          message:
                            error instanceof Error
                              ? error.message
                              : String(error),
                        })
                    );
                  }
                } catch (error) {
                  emitDiagnostic({
                    instanceId,
                    generation: snapshot.generation,
                    code: 'LISTENER_ERROR',
                    message:
                      error instanceof Error ? error.message : String(error),
                  });
                }
              }
            }
          } finally {
            acknowledgeBatch(native, batch.streamEpoch, batch.batchId);
          }
        })
      );
      nativeListeners.push(
        native.onDiagnostic((value) => {
          if (snapshot.state === 'closed' || value.instanceId !== instanceId)
            return;
          emitDiagnostic(diagnosticFromWire(value));
        })
      );
    }
    return native;
  };
  const ensureOpen = (): Promise<void> => {
    if (disposePromise !== undefined)
      return Promise.reject(
        new MqttError('ERR_DISPOSED', 'Client is disposed', {
          outcome: 'not-sent',
        })
      );
    if (openPromise !== undefined) return openPromise;
    openPromise = Promise.resolve()
      .then(() =>
        bindNative().openSession(
          instanceId,
          toWireSessionOptions(sessionOptions)
        )
      )
      .then((value) => {
        opened = true;
        emitNativeLifecycleSnapshot(value);
      })
      .catch((error: unknown) => {
        opened = false;
        openPromise = undefined;
        throw mapError(error, 'openSession');
      });
    return openPromise;
  };
  const listener = <T>(set: Set<T>, value: T): Listener => {
    if (disposePromise !== undefined || snapshot.state === 'closed')
      return { remove: () => undefined };
    set.add(value);
    return { remove: () => set.delete(value) };
  };
  const ensureUsable = (): void => {
    if (disposePromise !== undefined || snapshot.state === 'closed')
      throw new MqttError('ERR_DISPOSED', 'Client is disposed', {
        outcome: 'not-sent',
      });
  };

  const client: MqttClient = {
    get instanceId() {
      return instanceId;
    },
    get clientId() {
      return clientId;
    },
    get state() {
      return snapshot.state;
    },
    connect: () => {
      ensureUsable();
      if (snapshot.state === 'disconnecting')
        return Promise.reject(
          new MqttError(
            'ERR_OPERATION_IN_PROGRESS',
            'Disconnect is still in progress',
            { outcome: 'not-sent' }
          )
        );
      if (snapshot.state === 'connected' && lastConnAck !== undefined)
        return Promise.resolve(lastConnAck);
      if (connectPromise !== undefined) return connectPromise;
      connectPromise = ensureOpen()
        .then(() => bindNative().connect(instanceId))
        .then((value) => {
          emitNativeLifecycleSnapshot(value.snapshot);
          flushOfflineRemovals();
          lastConnAck = connAckFromWire(value.ack);
          return lastConnAck;
        })
        .catch((error: unknown) => {
          connectPromise = undefined;
          throw mapError(error, 'connect');
        });
      return connectPromise;
    },
    disconnect: () => {
      if (snapshot.state === 'closed') return Promise.resolve();
      ensureUsable();
      if (disconnectPromise !== undefined) return disconnectPromise;
      if (snapshot.state === 'idle' || snapshot.state === 'disconnected') {
        // A native disconnect callback may have settled the lifecycle before
        // this call reaches JS. Do not retain the resolved connect promise or
        // a later explicit connect would incorrectly return the old ACK.
        connectPromise = undefined;
        return Promise.resolve();
      }
      emitSnapshot({
        ...snapshot,
        state: 'disconnecting',
        revision: snapshot.revision + 1,
      });
      disconnectPromise = trackNativeOperation(
        ensureOpen()
          .then(() => bindNative().disconnect(instanceId))
          .then((value) => {
            emitNativeLifecycleSnapshot(value);
          })
          .catch((error: unknown) => {
            throw mapError(error, 'disconnect');
          })
          .finally(() => {
            disconnectPromise = undefined;
            connectPromise = undefined;
          })
      );
      return disconnectPromise;
    },
    dispose: () => {
      if (disposePromise !== undefined) return disposePromise;
      if (snapshot.state === 'closed') return Promise.resolve();
      const pendingOperations = [...pendingIntentOperations];
      const pendingNativeOperationsAtDispose = [...pendingNativeOperations];
      disposePromise = (openPromise ?? Promise.resolve())
        .catch(() => undefined)
        .then(() =>
          Promise.all(
            [...pendingOperations, ...pendingNativeOperationsAtDispose].map(
              (operation) => operation.catch(() => undefined)
            )
          )
        )
        .then(() =>
          NativeReactNativeMqtt == null
            ? undefined
            : NativeReactNativeMqtt.close(instanceId)
        )
        .then((value) => {
          if (value !== undefined) emitNativeLifecycleSnapshot(value);
          else
            emitSnapshot({
              ...snapshot,
              state: 'closed',
              deliveryStatus: 'closed',
              revision: snapshot.revision + 1,
            });
        })
        .catch((error: unknown) => {
          throw mapError(error, 'close');
        })
        .finally(() => {
          for (const timer of ackTimers.values()) clearTimeout(timer);
          ackTimers.clear();
          for (const timer of ackRetryTimers.values()) clearTimeout(timer);
          ackRetryTimers.clear();
          ackRetryAttempts.clear();
          for (const subscription of nativeListeners) {
            if (typeof subscription === 'function') subscription();
            else subscription.remove();
          }
          nativeListeners.length = 0;
          stateListeners.clear();
          messageListeners.clear();
          diagnosticListeners.clear();
        });
      return disposePromise;
    },
    ready: () => {
      ensureUsable();
      if (snapshot.state !== 'connected')
        return Promise.reject(
          new MqttError('ERR_NOT_CONNECTED', 'Client is not connected', {
            outcome: 'not-sent',
          })
        );
      const targetGeneration = snapshot.generation;
      const targetRevision = Math.max(
        nextIntentRevision,
        snapshot.subscriptionsRevision
      );
      const operations = [...pendingIntentOperations];
      return ensureOpen()
        .then(() => Promise.all(operations))
        .then(() =>
          bindNative().awaitSubscriptions(
            instanceId,
            targetGeneration,
            targetRevision
          )
        )
        .then((value) => {
          emitSnapshot(snapshotFromWire(value));
        });
    },
    getSnapshot: () => {
      if (snapshot.state === 'closed' || (!opened && openPromise === undefined))
        return Promise.resolve(snapshot);
      return ensureOpen()
        .then(() => bindNative().getSnapshot(instanceId))
        .then((value) => {
          const next = snapshotFromWire(value);
          emitSnapshot(next);
          return next;
        })
        .catch((error: unknown) => {
          throw mapError(error, 'getSnapshot');
        });
    },
    subscribe: (topicFilter, inputOptions) => {
      ensureUsable();
      validateTopicFilter(topicFilter);
      pendingOfflineRemovals.delete(topicFilter);
      const subscribeOptions = normalizeSubscribeOptions(
        inputOptions,
        options.protocolVersion ?? 4
      );
      const existing = subscriptions.get(topicFilter);
      const createHandle = (record: {
        options: SubscribeOptions;
        refs: number;
        qos: Subscription['qos'];
      }): Subscription => {
        record.refs += 1;
        let released = false;
        let releasePromise: Promise<void> | undefined;
        return {
          topicFilter,
          get qos() {
            return record.qos;
          },
          unsubscribe: async () => {
            if (released) return releasePromise;
            released = true;
            if (record.refs > 0) record.refs -= 1;
            if (record.refs !== 0) return;
            subscriptions.delete(topicFilter);
            const intentRevision = allocateIntentRevision();
            if (snapshot.state !== 'connected') {
              const removal = bindNative()
                .removeSubscription(instanceId, topicFilter, intentRevision)
                .then(() => undefined)
                .catch((error: unknown) => {
                  pendingOfflineRemovals.set(topicFilter, intentRevision);
                  released = false;
                  record.refs = 1;
                  subscriptions.set(topicFilter, record);
                  throw error;
                });
              releasePromise = trackIntentOperation(removal).catch(
                (error: unknown) => {
                  throw mapError(error, 'unsubscribe');
                }
              );
              return releasePromise;
            }
            const removal = bindNative()
              .removeSubscription(instanceId, topicFilter, intentRevision)
              .then(() => undefined)
              .catch((error: unknown) => {
                released = false;
                record.refs = 1;
                subscriptions.set(topicFilter, record);
                throw error;
              });
            releasePromise = trackIntentOperation(removal).catch(
              (error: unknown) => {
                throw mapError(error, 'unsubscribe');
              }
            );
            return releasePromise;
          },
        };
      };
      if (existing !== undefined) {
        if (
          JSON.stringify(existing.options) !== JSON.stringify(subscribeOptions)
        )
          return Promise.reject(
            new MqttError(
              'ERR_SUBSCRIPTION_CONFLICT',
              'Subscription options conflict for this filter',
              { outcome: 'not-sent' }
            )
          );
        return Promise.resolve(createHandle(existing));
      }
      const pending = pendingSubscriptions.get(topicFilter);
      if (pending !== undefined) {
        if (
          JSON.stringify(pending.options) !== JSON.stringify(subscribeOptions)
        )
          return Promise.reject(
            new MqttError(
              'ERR_SUBSCRIPTION_CONFLICT',
              'Subscription options conflict for this filter',
              { outcome: 'not-sent' }
            )
          );
        return pending.promise.then(() => {
          const record = subscriptions.get(topicFilter);
          if (record === undefined)
            throw new MqttError('ERR_UNKNOWN', 'Subscription request lost', {
              outcome: 'unknown',
            });
          return createHandle(record);
        });
      }
      const intentRevision = allocateIntentRevision();
      const promise = trackIntentOperation(
        ensureOpen()
          .then(() =>
            bindNative().setSubscription(
              instanceId,
              topicFilter,
              toWireSubscribeOptions(subscribeOptions),
              intentRevision
            )
          )
          .then((result: WireSubscriptionResult) => {
            if (disposePromise !== undefined)
              throw new MqttError('ERR_DISPOSED', 'Client is disposed', {
                outcome: 'not-sent',
              });
            nextIntentRevision = Math.max(
              nextIntentRevision,
              result.intentRevision
            );
            const record = {
              options: subscribeOptions,
              refs: 0,
              qos: result.grantedQos as Subscription['qos'],
            };
            subscriptions.set(topicFilter, record);
            return result.grantedQos;
          })
      );
      pendingSubscriptions.set(topicFilter, {
        options: subscribeOptions,
        promise,
      });
      return promise
        .then(() => {
          const record = subscriptions.get(topicFilter);
          if (record === undefined)
            throw new MqttError('ERR_UNKNOWN', 'Subscription request lost', {
              outcome: 'unknown',
            });
          return createHandle(record);
        })
        .catch((error: unknown) => {
          throw mapError(error, 'subscribe');
        })
        .finally(() => {
          if (pendingSubscriptions.get(topicFilter)?.promise === promise)
            pendingSubscriptions.delete(topicFilter);
        });
    },
    publishText: (topic, text, publishOptions) => {
      ensureUsable();
      const payload = encodeUtf8(text);
      return client.publishBytes(topic, payload, publishOptions);
    },
    publishBytes: (topic, bytes, publishOptions) => {
      ensureUsable();
      const copy = new Uint8Array(bytes);
      return trackNativeOperation(
        ensureOpen()
          .then(() =>
            bindNative().publish(
              instanceId,
              toWirePublishRequest(
                topic,
                encodeBase64(copy),
                publishOptions,
                options.protocolVersion ?? 4
              )
            )
          )
          .catch((error: unknown) => {
            throw mapError(error, 'publish');
          })
      );
    },
    publishBase64: (topic, base64, publishOptions) => {
      ensureUsable();
      return trackNativeOperation(
        ensureOpen()
          .then(() =>
            bindNative().publish(
              instanceId,
              toWirePublishRequest(
                topic,
                base64,
                publishOptions,
                options.protocolVersion ?? 4
              )
            )
          )
          .catch((error: unknown) => {
            throw mapError(error, 'publish');
          })
      );
    },
    onStateChange: (value) => listener(stateListeners, value),
    onMessage: (value) => listener(messageListeners, value),
    onDiagnostic: (value) => listener(diagnosticListeners, value),
  };
  if (Symbol.asyncDispose !== undefined)
    Object.defineProperty(client, Symbol.asyncDispose, {
      value: () => client.dispose(),
      enumerable: false,
    });
  return client;
}
