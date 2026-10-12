export type { Spec as NativeReactNativeMqttSpec } from './NativeReactNativeMqtt';
export { MqttError } from './errors';
export type { MqttErrorCode } from './errors';
export { createMqttClient } from './client';
export { decodeBase64, decodeUtf8, encodeBase64, encodeUtf8 } from './helpers';
export type {
  ConnAck,
  ConnectionState,
  DeliveryOptions,
  Diagnostic,
  Listener,
  Message,
  Mqtt4Options,
  Mqtt5Options,
  MqttClient,
  MqttOptions,
  PublishOptions,
  QoS,
  ReconnectOptions,
  SessionSnapshot,
  SubscribeOptions,
  Subscription,
  TlsOptions,
  Transport,
  UserProperty,
  WillOptions,
} from './types';

export const packageName = '@cnhongwei/react-native-mqtt' as const;
export const packageVersion = '0.1.0' as const;
