import { invalidOptions } from './errors';
import { decodeBase64, decodeUtf8, encodeBase64, encodeUtf8 } from './helpers';
import type {
  CommonMqttOptions,
  DeliveryOptions,
  MqttOptions,
  PublishOptions,
  ReconnectOptions,
  SubscribeOptions,
  TlsOptions,
  Transport,
  UserProperty,
  WillOptions,
} from './types';
import type {
  WirePublishRequest,
  WireSessionOptions,
  WireSubscribeOptions,
  WireUserProperty,
} from './NativeReactNativeMqtt';

const ownKeys = (value: object): string[] => Object.keys(value);
function object(value: unknown, name: string): Record<string, unknown> {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    throw invalidOptions(`${name} must be an object`);
  }
  return value as Record<string, unknown>;
}
function exactKeys(
  value: object,
  allowed: readonly string[],
  name: string
): void {
  const unknown = ownKeys(value).find((key) => !allowed.includes(key));
  if (unknown !== undefined)
    throw invalidOptions(`${name}.${unknown} is not supported`);
}
function stringValue(value: unknown, name: string, allowEmpty = true): string {
  if (typeof value !== 'string' || (!allowEmpty && value.length === 0))
    throw invalidOptions(`${name} must be a string`);
  return value;
}
function wellFormedMqttText(value: string): boolean {
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    if (code === 0 || (code >= 0xd800 && code <= 0xdfff)) {
      if (code < 0xdc00 && index + 1 < value.length) {
        const next = value.charCodeAt(index + 1);
        if (next >= 0xdc00 && next <= 0xdfff) {
          index += 1;
          continue;
        }
      }
      return false;
    }
  }
  return true;
}
function mqttString(value: unknown, name: string, allowEmpty = true): string {
  const result = stringValue(value, name, allowEmpty);
  if (!wellFormedMqttText(result))
    throw invalidOptions(`${name} is not a valid MQTT UTF-8 string`);
  if (encodeUtf8(result).length > 65535)
    throw invalidOptions(`${name} exceeds the MQTT UTF-8 string limit`);
  return result;
}
function boolValue(value: unknown, name: string): boolean {
  if (typeof value !== 'boolean')
    throw invalidOptions(`${name} must be a boolean`);
  return value;
}
function integer(
  value: unknown,
  name: string,
  min: number,
  max: number
): number {
  if (
    typeof value !== 'number' ||
    !Number.isSafeInteger(value) ||
    value < min ||
    value > max
  )
    throw invalidOptions(`${name} must be an integer in ${min}..${max}`);
  return value;
}
function ratio(value: unknown, name: string): number {
  if (
    typeof value !== 'number' ||
    !Number.isFinite(value) ||
    value < 0 ||
    value > 1
  )
    throw invalidOptions(`${name} must be a number in 0..1`);
  return value;
}
function propertyPairs(value: unknown, name: string): UserProperty[] {
  if (!Array.isArray(value)) throw invalidOptions(`${name} must be an array`);
  return value.map((item, index) => {
    const pair = object(item, `${name}[${index}]`);
    exactKeys(pair, ['key', 'value'], `${name}[${index}]`);
    return {
      key: mqttString(pair.key, `${name}[${index}].key`),
      value: mqttString(pair.value, `${name}[${index}].value`),
    };
  });
}
function base64(value: unknown, name: string): string {
  const text = stringValue(value, name);
  decodeBase64(text);
  return text;
}
function validateTls(value: unknown): TlsOptions | undefined {
  if (value === undefined) return undefined;
  const tls = object(value, 'tls');
  exactKeys(
    tls,
    [
      'caBase64',
      'p12Base64',
      'p12Password',
      'insecure',
      'allowInvalidHostnameForTesting',
    ],
    'tls'
  );
  const result: TlsOptions = {};
  if (tls.caBase64 !== undefined)
    result.caBase64 = base64(tls.caBase64, 'tls.caBase64');
  if (tls.p12Base64 !== undefined)
    result.p12Base64 = base64(tls.p12Base64, 'tls.p12Base64');
  if (tls.p12Password !== undefined)
    result.p12Password = stringValue(tls.p12Password, 'tls.p12Password');
  if (tls.insecure !== undefined)
    result.insecure = boolValue(tls.insecure, 'tls.insecure');
  if (tls.allowInvalidHostnameForTesting !== undefined)
    result.allowInvalidHostnameForTesting = boolValue(
      tls.allowInvalidHostnameForTesting,
      'tls.allowInvalidHostnameForTesting'
    );
  if (result.p12Password !== undefined && result.p12Base64 === undefined)
    throw invalidOptions('tls.p12Password requires tls.p12Base64');
  return result;
}
function validateReconnect(value: unknown): ReconnectOptions {
  if (value === undefined)
    return {
      enabled: true,
      initialDelayMs: 1000,
      maxDelayMs: 30000,
      jitterRatio: 0.2,
    };
  const reconnect = object(value, 'reconnect');
  exactKeys(
    reconnect,
    ['enabled', 'initialDelayMs', 'maxDelayMs', 'jitterRatio'],
    'reconnect'
  );
  const result: ReconnectOptions = {
    enabled:
      reconnect.enabled === undefined
        ? true
        : boolValue(reconnect.enabled, 'reconnect.enabled'),
    initialDelayMs:
      reconnect.initialDelayMs === undefined
        ? 1000
        : integer(
            reconnect.initialDelayMs,
            'reconnect.initialDelayMs',
            1,
            Number.MAX_SAFE_INTEGER
          ),
    maxDelayMs:
      reconnect.maxDelayMs === undefined
        ? 30000
        : integer(
            reconnect.maxDelayMs,
            'reconnect.maxDelayMs',
            1,
            Number.MAX_SAFE_INTEGER
          ),
    jitterRatio:
      reconnect.jitterRatio === undefined
        ? 0.2
        : ratio(reconnect.jitterRatio, 'reconnect.jitterRatio'),
  };
  if ((result.maxDelayMs ?? 0) < (result.initialDelayMs ?? 0))
    throw invalidOptions('reconnect.maxDelayMs must be >= initialDelayMs');
  return result;
}
function validateDelivery(value: unknown): DeliveryOptions {
  if (value === undefined)
    return {
      maxQueuedMessages: 1024,
      maxQueuedBytes: 8388608,
      maxMessageBytes: 1048576,
      batchMaxMessages: 64,
      batchMaxBytes: 262144,
      batchAckTimeoutMs: 15000,
      overflowPolicy: 'disconnect',
    };
  const delivery = object(value, 'delivery');
  exactKeys(
    delivery,
    [
      'maxQueuedMessages',
      'maxQueuedBytes',
      'maxMessageBytes',
      'batchMaxMessages',
      'batchMaxBytes',
      'batchAckTimeoutMs',
      'overflowPolicy',
    ],
    'delivery'
  );
  const result: DeliveryOptions = {
    maxQueuedMessages:
      delivery.maxQueuedMessages === undefined
        ? 1024
        : integer(
            delivery.maxQueuedMessages,
            'delivery.maxQueuedMessages',
            1,
            Number.MAX_SAFE_INTEGER
          ),
    maxQueuedBytes:
      delivery.maxQueuedBytes === undefined
        ? 8388608
        : integer(
            delivery.maxQueuedBytes,
            'delivery.maxQueuedBytes',
            1,
            Number.MAX_SAFE_INTEGER
          ),
    maxMessageBytes:
      delivery.maxMessageBytes === undefined
        ? 1048576
        : integer(
            delivery.maxMessageBytes,
            'delivery.maxMessageBytes',
            1,
            Number.MAX_SAFE_INTEGER
          ),
    batchMaxMessages:
      delivery.batchMaxMessages === undefined
        ? 64
        : integer(
            delivery.batchMaxMessages,
            'delivery.batchMaxMessages',
            1,
            Number.MAX_SAFE_INTEGER
          ),
    batchMaxBytes:
      delivery.batchMaxBytes === undefined
        ? 262144
        : integer(
            delivery.batchMaxBytes,
            'delivery.batchMaxBytes',
            1,
            Number.MAX_SAFE_INTEGER
          ),
    batchAckTimeoutMs:
      delivery.batchAckTimeoutMs === undefined
        ? 15000
        : integer(
            delivery.batchAckTimeoutMs,
            'delivery.batchAckTimeoutMs',
            1,
            Number.MAX_SAFE_INTEGER
          ),
    overflowPolicy:
      delivery.overflowPolicy === undefined
        ? 'disconnect'
        : delivery.overflowPolicy === 'drop-newest'
          ? 'drop-newest'
          : delivery.overflowPolicy === 'disconnect'
            ? 'disconnect'
            : undefined,
  };
  if (result.overflowPolicy === undefined)
    throw invalidOptions('delivery.overflowPolicy is invalid');
  if ((result.maxMessageBytes ?? 0) > (result.maxQueuedBytes ?? 0))
    throw invalidOptions('delivery.maxMessageBytes must be <= maxQueuedBytes');
  return result;
}
function validateWill(value: unknown): WillOptions | undefined {
  if (value === undefined) return undefined;
  const will = object(value, 'will');
  exactKeys(will, ['topic', 'payloadBase64', 'qos', 'retained'], 'will');
  const result: WillOptions = {
    topic: mqttString(will.topic, 'will.topic', false),
    payloadBase64: base64(will.payloadBase64, 'will.payloadBase64'),
    qos:
      will.qos === undefined
        ? 0
        : (integer(will.qos, 'will.qos', 0, 2) as WillOptions['qos']),
    retained:
      will.retained === undefined
        ? false
        : boolValue(will.retained, 'will.retained'),
  };
  validateTopicName(result.topic);
  return result;
}
function common(value: Record<string, unknown>): CommonMqttOptions {
  exactKeys(
    value,
    [
      'host',
      'port',
      'transport',
      'path',
      'clientId',
      'username',
      'password',
      'keepAliveSeconds',
      'connectTimeoutMs',
      'operationTimeoutMs',
      'disconnectTimeoutMs',
      'reconnect',
      'tls',
      'will',
      'delivery',
      'protocolVersion',
      'cleanSession',
      'cleanStart',
      'sessionExpiryIntervalSeconds',
      'receiveMaximum',
      'userProperties',
    ],
    'options'
  );
  const transport = value.transport === undefined ? 'tcp' : value.transport;
  if (
    transport !== 'tcp' &&
    transport !== 'tls' &&
    transport !== 'ws' &&
    transport !== 'wss'
  )
    throw invalidOptions('transport is invalid');
  const result: CommonMqttOptions = {
    host: stringValue(value.host, 'host', false),
    port: integer(value.port, 'port', 1, 65535),
    transport: transport as Transport,
    path:
      value.path === undefined
        ? undefined
        : stringValue(value.path, 'path', false),
    clientId:
      value.clientId === undefined
        ? undefined
        : mqttString(value.clientId, 'clientId'),
    username:
      value.username === undefined
        ? undefined
        : mqttString(value.username, 'username'),
    password:
      value.password === undefined
        ? undefined
        : mqttString(value.password, 'password'),
    keepAliveSeconds:
      value.keepAliveSeconds === undefined
        ? 60
        : integer(value.keepAliveSeconds, 'keepAliveSeconds', 0, 65535),
    connectTimeoutMs:
      value.connectTimeoutMs === undefined
        ? 30000
        : integer(
            value.connectTimeoutMs,
            'connectTimeoutMs',
            1,
            Number.MAX_SAFE_INTEGER
          ),
    operationTimeoutMs:
      value.operationTimeoutMs === undefined
        ? 30000
        : integer(
            value.operationTimeoutMs,
            'operationTimeoutMs',
            1,
            Number.MAX_SAFE_INTEGER
          ),
    disconnectTimeoutMs:
      value.disconnectTimeoutMs === undefined
        ? 5000
        : integer(
            value.disconnectTimeoutMs,
            'disconnectTimeoutMs',
            1,
            Number.MAX_SAFE_INTEGER
          ),
    reconnect: validateReconnect(value.reconnect),
    tls: validateTls(value.tls),
    will: validateWill(value.will),
    delivery: validateDelivery(value.delivery),
  };
  if (
    (result.path !== undefined) !==
      (transport === 'ws' || transport === 'wss') &&
    result.path !== undefined
  )
    throw invalidOptions('path is only valid for ws/wss');
  if ((transport === 'ws' || transport === 'wss') && result.path === undefined)
    result.path = '/mqtt';
  if (result.tls !== undefined && transport !== 'tls' && transport !== 'wss')
    throw invalidOptions('tls options are only valid for tls/wss');
  if (result.password !== undefined && result.username === undefined)
    throw invalidOptions('password requires username');
  return result;
}
export function normalizeOptions(input: MqttOptions): MqttOptions {
  const value = object(input, 'options');
  const result = common(value);
  if (
    result.clientId !== undefined &&
    encodeUtf8(result.clientId).length > 65535
  )
    throw invalidOptions('clientId exceeds the MQTT UTF-8 string limit');
  const protocolVersion =
    value.protocolVersion === undefined ? 4 : value.protocolVersion;
  if (protocolVersion !== 4 && protocolVersion !== 5)
    throw invalidOptions('protocolVersion must be 4 or 5');
  if (protocolVersion === 4) {
    if (result.clientId === '')
      throw invalidOptions('empty clientId is only valid for MQTT5 cleanStart');
    if (
      value.cleanStart !== undefined ||
      value.sessionExpiryIntervalSeconds !== undefined ||
      value.receiveMaximum !== undefined ||
      value.userProperties !== undefined
    )
      throw invalidOptions('MQTT5 options are not valid for protocolVersion 4');
    return {
      ...result,
      protocolVersion: 4,
      cleanSession:
        value.cleanSession === undefined
          ? true
          : boolValue(value.cleanSession, 'cleanSession'),
    };
  }
  if (value.cleanSession !== undefined)
    throw invalidOptions('cleanSession is not valid for protocolVersion 5');
  const sessionExpiry =
    value.sessionExpiryIntervalSeconds === undefined
      ? 0
      : integer(
          value.sessionExpiryIntervalSeconds,
          'sessionExpiryIntervalSeconds',
          0,
          4294967295
        );
  const receiveMaximum =
    value.receiveMaximum === undefined
      ? 65535
      : integer(value.receiveMaximum, 'receiveMaximum', 1, 65535);
  const cleanStart =
    value.cleanStart === undefined
      ? true
      : boolValue(value.cleanStart, 'cleanStart');
  if (result.clientId === '' && !cleanStart)
    throw invalidOptions('empty clientId requires MQTT5 cleanStart');
  return {
    ...result,
    protocolVersion: 5,
    cleanStart,
    sessionExpiryIntervalSeconds: sessionExpiry,
    receiveMaximum,
    userProperties:
      value.userProperties === undefined
        ? undefined
        : propertyPairs(value.userProperties, 'userProperties'),
  };
}
export function validateTopicName(topic: string): void {
  if (
    topic.length === 0 ||
    topic.includes('\0') ||
    topic.includes('+') ||
    topic.includes('#') ||
    !wellFormedMqttText(topic) ||
    encodeUtf8(topic).length > 65535
  )
    throw invalidOptions('topic name is invalid');
}
export function validateTopicFilter(filter: string): void {
  if (
    filter.length === 0 ||
    filter.includes('\0') ||
    !wellFormedMqttText(filter) ||
    encodeUtf8(filter).length > 65535 ||
    filter
      .split('/')
      .some(
        (part, index, parts) =>
          (part.includes('+') && part !== '+') ||
          (part.includes('#') && (part !== '#' || index !== parts.length - 1))
      )
  )
    throw invalidOptions('topic filter is invalid');
  if (filter.startsWith('$share/'))
    throw invalidOptions('shared subscriptions are not supported');
}
export function normalizeSubscribeOptions(
  input: SubscribeOptions | undefined,
  protocolVersion: 4 | 5 = 5
): SubscribeOptions {
  const value = input === undefined ? {} : object(input, 'subscribe options');
  exactKeys(
    value,
    ['qos', 'noLocal', 'retainAsPublished', 'retainHandling'],
    'subscribe options'
  );
  if (
    protocolVersion === 4 &&
    (value.noLocal !== undefined ||
      value.retainAsPublished !== undefined ||
      value.retainHandling !== undefined)
  )
    throw invalidOptions(
      'MQTT5 subscription options are not valid for protocolVersion 4'
    );
  return {
    qos:
      value.qos === undefined
        ? 1
        : (integer(
            value.qos,
            'subscribe options.qos',
            0,
            2
          ) as SubscribeOptions['qos']),
    noLocal:
      value.noLocal === undefined
        ? false
        : boolValue(value.noLocal, 'subscribe options.noLocal'),
    retainAsPublished:
      value.retainAsPublished === undefined
        ? false
        : boolValue(
            value.retainAsPublished,
            'subscribe options.retainAsPublished'
          ),
    retainHandling:
      value.retainHandling === undefined
        ? 0
        : (integer(
            value.retainHandling,
            'subscribe options.retainHandling',
            0,
            2
          ) as SubscribeOptions['retainHandling']),
  };
}
export function normalizePublishOptions(
  input: PublishOptions | undefined,
  protocolVersion: 4 | 5 = 5
): PublishOptions {
  const value = input === undefined ? {} : object(input, 'publish options');
  exactKeys(
    value,
    [
      'qos',
      'retained',
      'userProperties',
      'contentType',
      'responseTopic',
      'correlationDataBase64',
      'messageExpiryIntervalSeconds',
      'payloadFormatIndicator',
    ],
    'publish options'
  );
  if (
    protocolVersion === 4 &&
    (value.userProperties !== undefined ||
      value.contentType !== undefined ||
      value.responseTopic !== undefined ||
      value.correlationDataBase64 !== undefined ||
      value.messageExpiryIntervalSeconds !== undefined ||
      value.payloadFormatIndicator !== undefined)
  )
    throw invalidOptions(
      'MQTT5 publish options are not valid for protocolVersion 4'
    );
  return {
    qos:
      value.qos === undefined
        ? 1
        : (integer(
            value.qos,
            'publish options.qos',
            0,
            2
          ) as PublishOptions['qos']),
    retained:
      value.retained === undefined
        ? false
        : boolValue(value.retained, 'publish options.retained'),
    userProperties:
      value.userProperties === undefined
        ? undefined
        : propertyPairs(value.userProperties, 'publish options.userProperties'),
    contentType:
      value.contentType === undefined
        ? undefined
        : mqttString(value.contentType, 'publish options.contentType'),
    responseTopic:
      value.responseTopic === undefined
        ? undefined
        : mqttString(
            value.responseTopic,
            'publish options.responseTopic',
            false
          ),
    correlationDataBase64:
      value.correlationDataBase64 === undefined
        ? undefined
        : base64(
            value.correlationDataBase64,
            'publish options.correlationDataBase64'
          ),
    messageExpiryIntervalSeconds:
      value.messageExpiryIntervalSeconds === undefined
        ? undefined
        : integer(
            value.messageExpiryIntervalSeconds,
            'publish options.messageExpiryIntervalSeconds',
            0,
            4294967295
          ),
    payloadFormatIndicator:
      value.payloadFormatIndicator === undefined
        ? undefined
        : (integer(
            value.payloadFormatIndicator,
            'publish options.payloadFormatIndicator',
            0,
            1
          ) as PublishOptions['payloadFormatIndicator']),
  };
}
export function toWireSessionOptions(options: MqttOptions): WireSessionOptions {
  const normalized = normalizeOptions(options);
  const wire: WireSessionOptions = {
    host: normalized.host,
    port: normalized.port,
    protocolVersion: normalized.protocolVersion,
    transport: normalized.transport,
    path: normalized.path,
    clientId: normalized.clientId,
    username: normalized.username,
    password: normalized.password,
    keepAliveSeconds: normalized.keepAliveSeconds,
    connectTimeoutMs: normalized.connectTimeoutMs,
    operationTimeoutMs: normalized.operationTimeoutMs,
    disconnectTimeoutMs: normalized.disconnectTimeoutMs,
    reconnectEnabled: normalized.reconnect?.enabled,
    reconnectInitialDelayMs: normalized.reconnect?.initialDelayMs,
    reconnectMaxDelayMs: normalized.reconnect?.maxDelayMs,
    reconnectJitterRatio: normalized.reconnect?.jitterRatio,
    tlsCaBase64: normalized.tls?.caBase64,
    tlsP12Base64: normalized.tls?.p12Base64,
    tlsP12Password: normalized.tls?.p12Password,
    tlsInsecure: normalized.tls?.insecure,
    tlsAllowInvalidHostnameForTesting:
      normalized.tls?.allowInvalidHostnameForTesting,
    willTopic: normalized.will?.topic,
    willPayloadBase64: normalized.will?.payloadBase64,
    willQos: normalized.will?.qos,
    willRetained: normalized.will?.retained,
    cleanSession:
      normalized.protocolVersion === 4 ? normalized.cleanSession : undefined,
    cleanStart:
      normalized.protocolVersion === 5 ? normalized.cleanStart : undefined,
    sessionExpiryIntervalSeconds:
      normalized.protocolVersion === 5
        ? normalized.sessionExpiryIntervalSeconds
        : undefined,
    receiveMaximum:
      normalized.protocolVersion === 5 ? normalized.receiveMaximum : undefined,
    userProperties:
      normalized.protocolVersion === 5
        ? normalized.userProperties?.map(toWireProperty)
        : undefined,
    maxQueuedMessages: normalized.delivery?.maxQueuedMessages,
    maxQueuedBytes: normalized.delivery?.maxQueuedBytes,
    maxMessageBytes: normalized.delivery?.maxMessageBytes,
    batchMaxMessages: normalized.delivery?.batchMaxMessages,
    batchMaxBytes: normalized.delivery?.batchMaxBytes,
    batchAckTimeoutMs: normalized.delivery?.batchAckTimeoutMs,
    overflowPolicy: normalized.delivery?.overflowPolicy,
  };
  return wire;
}
function toWireProperty(property: UserProperty): WireUserProperty {
  return { key: property.key, value: property.value };
}
export function toWireSubscribeOptions(
  options: SubscribeOptions | undefined
): WireSubscribeOptions {
  const normalized = normalizeSubscribeOptions(options);
  return {
    qos: normalized.qos,
    noLocal: normalized.noLocal,
    retainAsPublished: normalized.retainAsPublished,
    retainHandling: normalized.retainHandling,
  };
}
export function toWirePublishRequest(
  topic: string,
  payloadBase64: string,
  options: PublishOptions | undefined,
  protocolVersion: 4 | 5 = 5
): WirePublishRequest {
  validateTopicName(topic);
  const normalized = normalizePublishOptions(options, protocolVersion);
  if (normalized.responseTopic !== undefined)
    validateTopicName(normalized.responseTopic);
  const payload = decodeBase64(payloadBase64);
  if (normalized.payloadFormatIndicator === 1) decodeUtf8(payload);
  return {
    topic,
    payloadBase64: encodeBase64(payload),
    qos: normalized.qos,
    retained: normalized.retained,
    userProperties: normalized.userProperties?.map(toWireProperty),
    contentType: normalized.contentType,
    responseTopic: normalized.responseTopic,
    correlationDataBase64: normalized.correlationDataBase64,
    messageExpiryIntervalSeconds: normalized.messageExpiryIntervalSeconds,
    payloadFormatIndicator: normalized.payloadFormatIndicator,
  };
}
export { encodeBase64 };
