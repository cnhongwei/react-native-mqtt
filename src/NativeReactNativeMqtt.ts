import {
  TurboModuleRegistry,
  type CodegenTypes,
  type TurboModule,
} from 'react-native';

export type WireUserProperty = { key: string; value: string };
export type WireCapabilities = {
  wireVersion: number;
  platform: string;
  protocolVersions: number[];
  transports: string[];
  supportsSessionPresent: boolean;
  supportsDuplicateUserProperties: boolean;
};
export type WireSessionOptions = {
  host: string;
  port: number;
  protocolVersion?: number;
  transport?: string;
  path?: string;
  clientId?: string;
  username?: string;
  password?: string;
  keepAliveSeconds?: number;
  connectTimeoutMs?: number;
  operationTimeoutMs?: number;
  disconnectTimeoutMs?: number;
  reconnectEnabled?: boolean;
  reconnectInitialDelayMs?: number;
  reconnectMaxDelayMs?: number;
  reconnectJitterRatio?: number;
  tlsCaBase64?: string;
  tlsP12Base64?: string;
  tlsP12Password?: string;
  tlsInsecure?: boolean;
  tlsAllowInvalidHostnameForTesting?: boolean;
  willTopic?: string;
  willPayloadBase64?: string;
  willQos?: number;
  willRetained?: boolean;
  cleanSession?: boolean;
  cleanStart?: boolean;
  sessionExpiryIntervalSeconds?: number;
  receiveMaximum?: number;
  userProperties?: WireUserProperty[];
  maxQueuedMessages?: number;
  maxQueuedBytes?: number;
  maxMessageBytes?: number;
  batchMaxMessages?: number;
  batchMaxBytes?: number;
  batchAckTimeoutMs?: number;
  overflowPolicy?: string;
};
export type WireSubscribeOptions = {
  qos?: number;
  noLocal?: boolean;
  retainAsPublished?: boolean;
  retainHandling?: number;
};
export type WirePublishRequest = {
  topic: string;
  payloadBase64: string;
  qos?: number;
  retained?: boolean;
  userProperties?: WireUserProperty[];
  contentType?: string;
  responseTopic?: string;
  correlationDataBase64?: string;
  messageExpiryIntervalSeconds?: number;
  payloadFormatIndicator?: number;
};
export type WireConnAck = {
  sessionPresent: boolean;
  assignedClientId?: string;
  reasonCode?: number;
  reasonString?: string;
  userProperties?: WireUserProperty[];
};
export type WireSnapshot = {
  instanceId: string;
  revision: number;
  generation: number;
  state: string;
  lastConnAck: WireConnAck | null;
  subscriptionsRevision: number;
  subscriptionsStatus: string;
  deliveryStatus: string;
};
export type WireMessage = {
  instanceId: string;
  generation: number;
  topic: string;
  payloadBase64: string;
  qos: number;
  retained: boolean;
  userProperties?: WireUserProperty[];
  contentType?: string;
  responseTopic?: string;
  correlationDataBase64?: string;
  messageExpiryIntervalSeconds?: number;
  payloadFormatIndicator?: number;
};
export type WireBatch = {
  instanceId: string;
  streamEpoch: number;
  batchId: number;
  messages: WireMessage[];
};
export type WireDiagnostic = {
  instanceId: string;
  generation: number;
  code: string;
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
export type WireSubscriptionResult = {
  generation: number;
  intentRevision: number;
  grantedQos: number;
  reasonCode?: number;
};
export type WireIntentResult = { intentRevision: number };

export interface Spec extends TurboModule {
  getCapabilities(): Promise<WireCapabilities>;
  openSession(
    instanceId: string,
    options: WireSessionOptions
  ): Promise<WireSnapshot>;
  connect(
    instanceId: string
  ): Promise<{ snapshot: WireSnapshot; ack: WireConnAck }>;
  disconnect(instanceId: string): Promise<WireSnapshot>;
  close(instanceId: string): Promise<WireSnapshot>;
  getSnapshot(instanceId: string): Promise<WireSnapshot>;
  setSubscription(
    instanceId: string,
    filter: string,
    options: WireSubscribeOptions,
    intentRevision: number
  ): Promise<WireSubscriptionResult>;
  removeSubscription(
    instanceId: string,
    filter: string,
    intentRevision: number
  ): Promise<WireIntentResult>;
  awaitSubscriptions(
    instanceId: string,
    generation: number,
    desiredRevision: number
  ): Promise<WireSnapshot>;
  publish(instanceId: string, request: WirePublishRequest): Promise<void>;
  ackMessageBatch(
    instanceId: string,
    streamEpoch: number,
    batchId: number
  ): Promise<void>;
  readonly onSessionSnapshot: CodegenTypes.EventEmitter<WireSnapshot>;
  readonly onMessageBatch: CodegenTypes.EventEmitter<WireBatch>;
  readonly onDiagnostic: CodegenTypes.EventEmitter<WireDiagnostic>;
}

export default TurboModuleRegistry.get<Spec>('ReactNativeMqtt');
