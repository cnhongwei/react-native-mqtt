export type QoS = 0 | 1 | 2;
export type Transport = 'tcp' | 'tls' | 'ws' | 'wss';
export type ConnectionState =
  | 'idle'
  | 'connecting'
  | 'connected'
  | 'reconnecting'
  | 'disconnecting'
  | 'disconnected'
  | 'closed';
export type UserProperty = { key: string; value: string };
export type TlsOptions = {
  caBase64?: string;
  p12Base64?: string;
  p12Password?: string;
  insecure?: boolean;
  allowInvalidHostnameForTesting?: boolean;
};
export type ReconnectOptions = {
  enabled?: boolean;
  initialDelayMs?: number;
  maxDelayMs?: number;
  jitterRatio?: number;
};
export type DeliveryOptions = {
  maxQueuedMessages?: number;
  maxQueuedBytes?: number;
  maxMessageBytes?: number;
  batchMaxMessages?: number;
  batchMaxBytes?: number;
  batchAckTimeoutMs?: number;
  overflowPolicy?: 'disconnect' | 'drop-newest';
};
export type WillOptions = {
  topic: string;
  payloadBase64: string;
  qos?: QoS;
  retained?: boolean;
};

export type CommonMqttOptions = {
  host: string;
  port: number;
  transport?: Transport;
  path?: string;
  clientId?: string;
  username?: string;
  password?: string;
  keepAliveSeconds?: number;
  connectTimeoutMs?: number;
  operationTimeoutMs?: number;
  disconnectTimeoutMs?: number;
  reconnect?: ReconnectOptions;
  tls?: TlsOptions;
  will?: WillOptions;
  delivery?: DeliveryOptions;
};
export type Mqtt4Options = CommonMqttOptions & {
  /** MQTT 3.1.1 (default). MQTT 3.1 / protocol level 3 is unsupported. */
  protocolVersion?: 4;
  cleanSession?: boolean;
  cleanStart?: never;
  sessionExpiryIntervalSeconds?: never;
  receiveMaximum?: never;
  userProperties?: never;
};
export type Mqtt5Options = CommonMqttOptions & {
  /** MQTT 5.0. Only protocol levels 4 and 5 are supported on Android/iOS. */
  protocolVersion: 5;
  cleanSession?: never;
  cleanStart?: boolean;
  sessionExpiryIntervalSeconds?: number;
  receiveMaximum?: number;
  userProperties?: UserProperty[];
};
export type MqttOptions = Mqtt4Options | Mqtt5Options;

export type SubscribeOptions = {
  qos?: QoS;
  noLocal?: boolean;
  retainAsPublished?: boolean;
  retainHandling?: 0 | 1 | 2;
};
export type PublishOptions = {
  qos?: QoS;
  retained?: boolean;
  userProperties?: UserProperty[];
  contentType?: string;
  responseTopic?: string;
  correlationDataBase64?: string;
  messageExpiryIntervalSeconds?: number;
  payloadFormatIndicator?: 0 | 1;
};
export type ConnAck = {
  sessionPresent: boolean;
  assignedClientId?: string;
  reasonCode?: number;
  reasonString?: string;
  userProperties?: UserProperty[];
};
export type SessionSnapshot = {
  instanceId: string;
  revision: number;
  generation: number;
  state: ConnectionState;
  lastConnAck: ConnAck | null;
  subscriptionsRevision: number;
  subscriptionsStatus: 'pending' | 'ready' | 'failed';
  deliveryStatus: 'active' | 'stalled' | 'closed';
};
export type Message = {
  instanceId: string;
  generation: number;
  topic: string;
  bytes: Uint8Array;
  qos: QoS;
  retained: boolean;
  userProperties?: UserProperty[];
  contentType?: string;
  responseTopic?: string;
  correlationData?: Uint8Array;
  messageExpiryIntervalSeconds?: number;
  payloadFormatIndicator?: 0 | 1;
};
export type Diagnostic = {
  instanceId: string;
  generation: number;
  code:
    | 'SUBSCRIPTION_SYNC_FAILED'
    | 'DELIVERY_STALLED'
    | 'QUEUE_OVERFLOW'
    | 'LISTENER_ERROR'
    | 'INSECURE_TLS';
  message: string;
  operation?: string;
  reasonCode?: number;
  reasonString?: string;
  metrics?: {
    queuedMessages: number;
    queuedBytes: number;
    pendingOperations: number;
    droppedMessages: number;
    reconnectAttempts: number;
  };
};
export type Listener = { remove(): void };
export type Subscription = {
  readonly topicFilter: string;
  readonly qos: QoS;
  unsubscribe(): Promise<void>;
};
export type MqttClient = {
  readonly instanceId: string;
  readonly clientId: string;
  readonly state: ConnectionState;
  connect(): Promise<ConnAck>;
  disconnect(): Promise<void>;
  dispose(): Promise<void>;
  ready(): Promise<void>;
  getSnapshot(): Promise<SessionSnapshot>;
  subscribe(
    topicFilter: string,
    options?: SubscribeOptions
  ): Promise<Subscription>;
  publishText(
    topic: string,
    text: string,
    options?: PublishOptions
  ): Promise<void>;
  publishBytes(
    topic: string,
    bytes: Uint8Array,
    options?: PublishOptions
  ): Promise<void>;
  publishBase64(
    topic: string,
    base64: string,
    options?: PublishOptions
  ): Promise<void>;
  onStateChange(listener: (snapshot: SessionSnapshot) => void): Listener;
  onMessage(listener: (message: Message) => void): Listener;
  onDiagnostic(listener: (diagnostic: Diagnostic) => void): Listener;
  readonly [Symbol.asyncDispose]?: () => Promise<void>;
};
