export type MqttErrorCode =
  | 'ERR_INVALID_OPTIONS'
  | 'ERR_INVALID_PAYLOAD'
  | 'ERR_NATIVE_UNAVAILABLE'
  | 'ERR_CONTRACT_MISMATCH'
  | 'ERR_NOT_CONNECTED'
  | 'ERR_DISPOSED'
  | 'ERR_OPERATION_IN_PROGRESS'
  | 'ERR_OPERATION_SUPERSEDED'
  | 'ERR_CONNECT_TIMEOUT'
  | 'ERR_OPERATION_TIMEOUT'
  | 'ERR_CONNECTION_REFUSED'
  | 'ERR_CONNECTION_LOST'
  | 'ERR_SUBSCRIPTION_CONFLICT'
  | 'ERR_SUBSCRIBE_FAILED'
  | 'ERR_UNSUBSCRIBE_FAILED'
  | 'ERR_PUBLISH_FAILED'
  | 'ERR_TLS'
  | 'ERR_CLEANUP_FAILED'
  | 'ERR_UNKNOWN';
export class MqttError extends Error {
  readonly code: MqttErrorCode;
  readonly operation?: string;
  readonly generation?: number;
  readonly reasonCode?: number;
  readonly reasonString?: string;
  readonly outcome?: 'not-sent' | 'rejected' | 'unknown';
  constructor(
    code: MqttErrorCode,
    message: string,
    details: {
      operation?: string;
      generation?: number;
      reasonCode?: number;
      reasonString?: string;
      outcome?: 'not-sent' | 'rejected' | 'unknown';
    } = {}
  ) {
    super(message);
    this.name = 'MqttError';
    this.code = code;
    this.operation = details.operation;
    this.generation = details.generation;
    this.reasonCode = details.reasonCode;
    this.reasonString = details.reasonString;
    this.outcome = details.outcome;
  }
}
export function invalidOptions(message: string): MqttError {
  return new MqttError('ERR_INVALID_OPTIONS', message, { outcome: 'not-sent' });
}
