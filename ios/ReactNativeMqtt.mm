#import "ReactNativeMqtt.h"
#import "ReactNativeMqtt-Swift.h"
#include <cmath>

static BOOL RNMqttValidMqttString(NSString *value, BOOL allowEmpty) {
  if (value == nil || (!allowEmpty && value.length == 0) || [value rangeOfString:@"\0"].location != NSNotFound) return NO;
  NSData *encoded = [value dataUsingEncoding:NSUTF8StringEncoding allowLossyConversion:NO];
  return encoded != nil && encoded.length <= 65535;
}

static BOOL RNMqttValidTopicName(NSString *value) {
  return RNMqttValidMqttString(value, NO) && [value rangeOfString:@"+"].location == NSNotFound && [value rangeOfString:@"#"].location == NSNotFound;
}

static BOOL RNMqttValidBase64(NSString *value) {
  NSData *decoded = [[NSData alloc] initWithBase64EncodedString:value options:0];
  return decoded != nil && [[decoded base64EncodedStringWithOptions:0] isEqualToString:value];
}

static BOOL RNMqttValidTopicFilter(NSString *value) {
  if (!RNMqttValidMqttString(value, NO) || [value hasPrefix:@"$share/"]) return NO;
  NSArray<NSString *> *levels = [value componentsSeparatedByString:@"/"];
  for (NSUInteger index = 0; index < levels.count; index += 1) {
    NSString *level = levels[index];
    BOOL hasSingle = [level rangeOfString:@"+"].location != NSNotFound;
    BOOL hasMulti = [level rangeOfString:@"#"].location != NSNotFound;
    if ((hasSingle && ![level isEqualToString:@"+"]) || (hasMulti && (! [level isEqualToString:@"#"] || index + 1 != levels.count))) return NO;
  }
  return YES;
}

static NSError *RNMqttAnnotatedError(NSError *error, NSString *operation, NSNumber *generation, NSString *outcome) {
  if (error == nil) return nil;
  NSMutableDictionary *userInfo = [error.userInfo mutableCopy] ?: [NSMutableDictionary dictionary];
  if (operation != nil && userInfo[@"operation"] == nil) userInfo[@"operation"] = operation;
  if (generation != nil && userInfo[@"generation"] == nil) userInfo[@"generation"] = generation;
  if (outcome != nil && userInfo[@"outcome"] == nil) userInfo[@"outcome"] = outcome;
  return [NSError errorWithDomain:error.domain code:error.code userInfo:userInfo];
}

static NSArray<NSDictionary *> *RNMqttUserProperties(JS::NativeReactNativeMqtt::WireSessionOptions &options) {
  NSMutableArray<NSDictionary *> *properties = [NSMutableArray array];
  auto userProperties = options.userProperties();
  if (userProperties.has_value()) {
    for (const auto &property : *userProperties) {
      [properties addObject:@{ @"key": property.key(), @"value": property.value() }];
    }
  }
  return properties;
}

typedef void (^RNMqttSubscriptionOperation)(dispatch_block_t finish);

@implementation ReactNativeMqtt

- (instancetype)init {
  self = [super init];
  if (self) {
    _sessionQueue = dispatch_queue_create("com.cnhongwei.reactnativemqtt.sessions", DISPATCH_QUEUE_SERIAL);
    _sessions = [NSMutableDictionary dictionary];
    _iosBatchLedger = [NSMutableDictionary dictionary];
    _iosStreamEpochs = [NSMutableDictionary dictionary];
    _iosSubscriptionQueues = [NSMutableDictionary dictionary];
    _iosBatchId = 0;
    _nextOperationId = 0;
  }
  return self;
}

- (NSMutableDictionary *)operationWaiterWithReject:(RCTPromiseRejectBlock)reject
                                          operation:(NSString *)operation
                                               topic:(NSString *)topic
                                          generation:(NSNumber *)generation {
  return [@{
    @"reject": [reject copy],
    @"settled": @NO,
    @"operationId": @(++_nextOperationId),
    @"operation": operation,
    @"topic": topic ?: [NSNull null],
    @"generation": generation ?: @0,
  } mutableCopy];
}

- (void)startNextSubscriptionOperationForInstance:(NSString *)instanceId filter:(NSString *)filter {
  NSMutableDictionary *byFilter = self->_iosSubscriptionQueues[instanceId];
  NSMutableArray *queue = byFilter[filter];
  if (queue.count == 0) return;
  RNMqttSubscriptionOperation operation = [queue.firstObject copy];
  __block BOOL finished = NO;
  dispatch_block_t finish = ^{
    if (finished) return;
    finished = YES;
    dispatch_async(self->_sessionQueue, ^{
      NSMutableDictionary *currentByFilter = self->_iosSubscriptionQueues[instanceId];
      NSMutableArray *currentQueue = currentByFilter[filter];
      if (currentQueue.count > 0) [currentQueue removeObjectAtIndex:0];
      if (currentQueue.count == 0) {
        [currentByFilter removeObjectForKey:filter];
      } else {
        [self startNextSubscriptionOperationForInstance:instanceId filter:filter];
      }
      if (currentByFilter.count == 0) [self->_iosSubscriptionQueues removeObjectForKey:instanceId];
    });
  };
  operation(finish);
}

- (void)enqueueSubscriptionOperation:(RNMqttSubscriptionOperation)operation instanceId:(NSString *)instanceId filter:(NSString *)filter {
  NSMutableDictionary *byFilter = [self->_iosSubscriptionQueues[instanceId] mutableCopy] ?: [NSMutableDictionary dictionary];
  NSMutableArray *queue = [byFilter[filter] mutableCopy] ?: [NSMutableArray array];
  BOOL shouldStart = queue.count == 0;
  [queue addObject:[operation copy]];
  byFilter[filter] = queue;
  self->_iosSubscriptionQueues[instanceId] = byFilter;
  if (shouldStart) [self startNextSubscriptionOperationForInstance:instanceId filter:filter];
}
- (void)getCapabilities:(RCTPromiseResolveBlock)resolve
                 reject:(RCTPromiseRejectBlock)reject {
  (void)reject;
  resolve(@{
    @"wireVersion": @1,
    @"platform": @"ios",
    @"protocolVersions": @[@4, @5],
    @"transports": @[@"tcp", @"tls", @"ws", @"wss"],
    // MQTT 5 exposes sessionPresent through the CocoaMQTT CONNACK probe;
    // MQTT 3.1.1 keeps the protocol-defined unavailable/false value.
    @"supportsSessionPresent": @YES,
    @"supportsDuplicateUserProperties": @YES,
  });
}

- (NSString *)invalidSessionOptions:(JS::NativeReactNativeMqtt::WireSessionOptions &)options {
  NSString *host = options.host();
  if (host == nil || host.length == 0) return @"host must be a non-empty string";
  const double port = options.port();
  if (!std::isfinite(port) || std::floor(port) != port || port < 1 || port > 65535) return @"port must be an integer in 1..65535";
  const NSInteger protocol = options.protocolVersion().value_or(4);
  if (protocol != 4 && protocol != 5) return @"protocolVersion must be 4 or 5";
  NSString *transport = options.transport();
  if (transport == nil || (![transport isEqualToString:@"tcp"] && ![transport isEqualToString:@"tls"] && ![transport isEqualToString:@"ws"] && ![transport isEqualToString:@"wss"])) return @"transport is invalid";
  NSString *path = options.path();
  if (path != nil && ![transport isEqualToString:@"ws"] && ![transport isEqualToString:@"wss"]) return @"path is only valid for ws/wss";
  if (path != nil && (path.length == 0 || ![path hasPrefix:@"/"])) return @"path must start with /";
  if ((options.tlsCaBase64() != nil || options.tlsP12Base64() != nil || options.tlsP12Password() != nil || options.tlsInsecure().has_value()) && ![transport isEqualToString:@"tls"] && ![transport isEqualToString:@"wss"]) return @"tls options are only valid for tls/wss";
  if (options.password() != nil && options.username() == nil) return @"password requires username";
  if (options.username() != nil && !RNMqttValidMqttString(options.username(), YES)) return @"username is not valid MQTT UTF-8";
  if (options.password() != nil && !RNMqttValidMqttString(options.password(), YES)) return @"password is not valid MQTT UTF-8";
  if (options.clientId() != nil) {
    if (!RNMqttValidMqttString(options.clientId(), YES)) return @"clientId is not valid MQTT UTF-8";
    NSData *clientIdData = [options.clientId() dataUsingEncoding:NSUTF8StringEncoding];
    if (clientIdData == nil || clientIdData.length > 65535) return @"clientId exceeds the MQTT UTF-8 string limit";
    if (options.clientId().length == 0 && !(protocol == 5 && options.cleanStart().value_or(true))) return @"empty clientId requires MQTT5 cleanStart";
  }
  if (protocol == 4 && (options.cleanStart().has_value() || options.sessionExpiryIntervalSeconds().has_value() || options.receiveMaximum().has_value() || options.userProperties().has_value())) return @"MQTT5 options are not valid for protocolVersion 4";
  if (protocol == 5 && options.cleanSession().has_value()) return @"cleanSession is not valid for protocolVersion 5";
  NSString *willTopic = options.willTopic();
  NSString *willPayload = options.willPayloadBase64();
  if ((willTopic == nil) != (willPayload == nil)) return @"willTopic and willPayloadBase64 must be provided together";
  if (willTopic != nil && !RNMqttValidTopicName(willTopic)) return @"willTopic is invalid";
  if (willPayload != nil && !RNMqttValidBase64(willPayload)) return @"willPayloadBase64 is invalid";
  if (options.willQos().has_value() && (*options.willQos() < 0 || *options.willQos() > 2 || std::floor(*options.willQos()) != *options.willQos())) return @"willQos must be an integer in 0..2";
  auto connectionUserProperties = options.userProperties();
  if (connectionUserProperties.has_value()) {
    for (const auto &property : *connectionUserProperties) {
      NSString *key = property.key();
      NSString *value = property.value();
      if (key == nil || value == nil || !RNMqttValidMqttString(key, YES) || !RNMqttValidMqttString(value, YES)) return @"userProperties entries must contain valid strings";
    }
  }
  if (options.keepAliveSeconds().has_value() && (*options.keepAliveSeconds() < 0 || *options.keepAliveSeconds() > 65535)) return @"keepAliveSeconds is out of range";
  if (options.sessionExpiryIntervalSeconds().has_value() && (*options.sessionExpiryIntervalSeconds() < 0 || *options.sessionExpiryIntervalSeconds() > 4294967295)) return @"sessionExpiryIntervalSeconds is out of range";
  if (options.receiveMaximum().has_value() && (*options.receiveMaximum() < 1 || *options.receiveMaximum() > 65535)) return @"receiveMaximum is out of range";
  if (options.connectTimeoutMs().has_value() && *options.connectTimeoutMs() < 1) return @"connectTimeoutMs must be positive";
  if (options.operationTimeoutMs().has_value() && *options.operationTimeoutMs() < 1) return @"operationTimeoutMs must be positive";
  if (options.disconnectTimeoutMs().has_value() && *options.disconnectTimeoutMs() < 1) return @"disconnectTimeoutMs must be positive";
  if (options.reconnectInitialDelayMs().has_value() && *options.reconnectInitialDelayMs() < 1) return @"reconnectInitialDelayMs must be positive";
  if (options.reconnectMaxDelayMs().has_value() && *options.reconnectMaxDelayMs() < 1) return @"reconnectMaxDelayMs must be positive";
  if (options.reconnectInitialDelayMs().has_value() && options.reconnectMaxDelayMs().has_value() && *options.reconnectMaxDelayMs() < *options.reconnectInitialDelayMs()) return @"reconnectMaxDelayMs must be >= reconnectInitialDelayMs";
  if (options.reconnectJitterRatio().has_value() && (*options.reconnectJitterRatio() < 0 || *options.reconnectJitterRatio() > 1)) return @"reconnectJitterRatio must be in 0..1";
  if (options.maxQueuedMessages().has_value() && *options.maxQueuedMessages() < 1) return @"maxQueuedMessages must be positive";
  if (options.maxQueuedBytes().has_value() && *options.maxQueuedBytes() < 1) return @"maxQueuedBytes must be positive";
  if (options.maxMessageBytes().has_value() && *options.maxMessageBytes() < 1) return @"maxMessageBytes must be positive";
  if (options.maxMessageBytes().has_value() && options.maxQueuedBytes().has_value() && *options.maxMessageBytes() > *options.maxQueuedBytes()) return @"maxMessageBytes must be <= maxQueuedBytes";
  if (options.batchMaxMessages().has_value() && *options.batchMaxMessages() < 1) return @"batchMaxMessages must be positive";
  if (options.batchMaxBytes().has_value() && *options.batchMaxBytes() < 1) return @"batchMaxBytes must be positive";
  if (options.batchAckTimeoutMs().has_value() && *options.batchAckTimeoutMs() < 1) return @"batchAckTimeoutMs must be positive";
  if (options.overflowPolicy() != nil && ![options.overflowPolicy() isEqualToString:@"disconnect"] && ![options.overflowPolicy() isEqualToString:@"drop-newest"]) return @"overflowPolicy is invalid";
  return nil;
}

- (ReactNativeMqttCocoaBridge *)bridgeForSessionOptions:(NSDictionary *)options clientId:(NSString *)clientId {
  return [[ReactNativeMqttCocoaBridge alloc]
    initWithClientId:clientId
    host:options[@"host"]
    port:options[@"port"]
    protocolVersion:options[@"protocolVersion"]
    transport:options[@"transport"]
    path:options[@"path"]
    username:options[@"username"] == [NSNull null] ? nil : options[@"username"]
    password:options[@"password"] == [NSNull null] ? nil : options[@"password"]
    caBase64:options[@"tlsCaBase64"] == [NSNull null] ? nil : options[@"tlsCaBase64"]
    p12Base64:options[@"tlsP12Base64"] == [NSNull null] ? nil : options[@"tlsP12Base64"]
    p12Password:options[@"tlsP12Password"] == [NSNull null] ? nil : options[@"tlsP12Password"]
    insecure:[options[@"tlsInsecure"] boolValue]
    allowInvalidHostnameForTesting:[options[@"tlsAllowInvalidHostnameForTesting"] boolValue]
    keepAlive:options[@"keepAliveSeconds"]
    cleanStart:[options[@"cleanStart"] boolValue]
    cleanSession:[options[@"cleanSession"] boolValue]
    sessionExpiryInterval:options[@"sessionExpiryIntervalSeconds"]
    receiveMaximum:options[@"receiveMaximum"]
    userProperties:options[@"userProperties"]
    willTopic:options[@"willTopic"] == [NSNull null] ? nil : options[@"willTopic"]
    willPayloadBase64:options[@"willPayloadBase64"] == [NSNull null] ? nil : options[@"willPayloadBase64"]
    willQos:options[@"willQos"]
    willRetained:[options[@"willRetained"] boolValue]
    reconnectEnabled:[options[@"reconnectEnabled"] boolValue]
    reconnectInitialDelayMs:options[@"reconnectInitialDelayMs"]
    reconnectMaxDelayMs:options[@"reconnectMaxDelayMs"]
    reconnectJitterRatio:options[@"reconnectJitterRatio"]
    connectTimeoutMs:options[@"connectTimeoutMs"]
    disconnectTimeoutMs:options[@"disconnectTimeoutMs"]];
}

- (NSDictionary *)snapshotForInstance:(NSString *)instanceId state:(NSString *)state revision:(NSNumber *)revision generation:(NSNumber *)generation lastConnAck:(id)lastConnAck deliveryStatus:(NSString *)deliveryStatus {
  NSDictionary *record = self->_sessions[instanceId];
  return @{
    @"instanceId": instanceId,
    @"revision": revision,
    @"generation": generation,
    @"state": state,
    @"lastConnAck": lastConnAck ?: [NSNull null],
    @"subscriptionsRevision": record[@"subscriptionsRevision"] ?: @0,
    @"subscriptionsStatus": record[@"subscriptionsStatus"] ?: @"ready",
    @"deliveryStatus": deliveryStatus,
  };
}

- (void)openSession:(NSString *)instanceId options:(JS::NativeReactNativeMqtt::WireSessionOptions &)options resolve:(RCTPromiseResolveBlock)resolve reject:(RCTPromiseRejectBlock)reject {
  NSString *invalid = [self invalidSessionOptions:options];
  if (invalid != nil) {
    reject(@"ERR_INVALID_OPTIONS", invalid, nil);
    return;
  }
  NSArray<NSDictionary *> *userProperties = RNMqttUserProperties(options);
  NSDictionary *sessionOptions = @{
    @"host": options.host(),
    @"port": @(options.port()),
    @"protocolVersion": options.protocolVersion().has_value() ? @(*options.protocolVersion()) : @4,
    @"transport": options.transport(),
    @"path": options.path() ?: @"/mqtt",
    @"clientId": options.clientId() ?: [NSNull null],
    @"username": options.username() ?: [NSNull null],
    @"password": options.password() ?: [NSNull null],
    @"tlsCaBase64": options.tlsCaBase64() ?: [NSNull null],
    @"tlsP12Base64": options.tlsP12Base64() ?: [NSNull null],
    @"tlsP12Password": options.tlsP12Password() ?: [NSNull null],
    @"tlsInsecure": @(options.tlsInsecure().value_or(false)),
    @"tlsAllowInvalidHostnameForTesting": @(options.tlsAllowInvalidHostnameForTesting().value_or(false)),
    @"keepAliveSeconds": @(options.keepAliveSeconds().value_or(60)),
    @"cleanStart": @(options.cleanStart().value_or(true)),
    @"cleanSession": @(options.cleanSession().value_or(true)),
    @"sessionExpiryIntervalSeconds": @(options.sessionExpiryIntervalSeconds().value_or(0)),
    @"receiveMaximum": @(options.receiveMaximum().value_or(65535)),
    @"userProperties": userProperties,
    @"willTopic": options.willTopic() ?: [NSNull null],
    @"willPayloadBase64": options.willPayloadBase64() ?: [NSNull null],
    @"willQos": @(options.willQos().value_or(0)),
    @"willRetained": @(options.willRetained().value_or(false)),
    @"maxQueuedMessages": @(options.maxQueuedMessages().value_or(1024)),
    @"maxQueuedBytes": @(options.maxQueuedBytes().value_or(8 * 1024 * 1024)),
    @"maxMessageBytes": @(options.maxMessageBytes().value_or(1024 * 1024)),
    @"batchMaxMessages": @(options.batchMaxMessages().value_or(64)),
    @"batchMaxBytes": @(options.batchMaxBytes().value_or(256 * 1024)),
    @"connectTimeoutMs": @(options.connectTimeoutMs().value_or(30000)),
    @"operationTimeoutMs": @(options.operationTimeoutMs().value_or(30000)),
    @"disconnectTimeoutMs": @(options.disconnectTimeoutMs().value_or(5000)),
    @"reconnectEnabled": @(options.reconnectEnabled().value_or(true)),
    @"reconnectInitialDelayMs": @(options.reconnectInitialDelayMs().value_or(1000)),
    @"reconnectMaxDelayMs": @(options.reconnectMaxDelayMs().value_or(30000)),
    @"reconnectJitterRatio": @(options.reconnectJitterRatio().value_or(0.2)),
    @"overflowPolicy": options.overflowPolicy() ?: @"disconnect",
  };
  NSString *clientId = options.clientId() ?: instanceId;
  dispatch_async(_sessionQueue, ^{
    if (self->_invalidated) {
      reject(@"ERR_DISPOSED", @"Native module is invalidated", nil);
      return;
    }
    NSMutableDictionary *existing = [self->_sessions[instanceId] mutableCopy];
    if (existing != nil && ![existing[@"options"] isEqual:sessionOptions]) {
      reject(@"ERR_OPERATION_IN_PROGRESS", @"Session options cannot change", nil);
      return;
    }
    if (existing == nil) {
      NSNumber *streamEpoch = @([self->_iosStreamEpochs[instanceId] unsignedLongLongValue] + 1);
      self->_iosStreamEpochs[instanceId] = streamEpoch;
      existing = [@{
        @"options": sessionOptions,
        @"clientId": clientId,
        @"messageQueue": [NSMutableArray array],
        @"overflowDiagnosticPending": @NO,
        @"revision": @0,
        @"generation": @0,
        @"state": @"idle",
        @"lastConnAck": [NSNull null],
        @"subscriptionsRevision": @0,
        @"subscriptionsStatus": @"ready",
        @"pendingSubscriptionOperations": @0,
        @"droppedMessages": @0,
        @"desiredSubscriptions": [NSMutableDictionary dictionary],
        @"confirmedSubscriptions": [NSMutableDictionary dictionary],
        @"tombstones": [NSMutableDictionary dictionary],
        @"subscriptionWaiters": [NSMutableArray array],
        @"connectWaiters": [NSMutableArray array],
        @"operationWaiters": [NSMutableArray array],
        @"reconnectAttempts": @0,
        @"reconnectToken": @0,
        @"streamEpoch": streamEpoch,
      } mutableCopy];
      existing[@"bridge"] = [self bridgeForSessionOptions:sessionOptions clientId:clientId];
      self->_sessions[instanceId] = existing;
    }
    resolve([self snapshotForInstance:instanceId state:existing[@"state"] revision:existing[@"revision"] generation:existing[@"generation"] lastConnAck:existing[@"lastConnAck"] deliveryStatus:@"active"]);
  });
}

- (void)emitNextMessageBatchForInstance:(NSString *)instanceId {
  NSMutableDictionary *record = [self->_sessions[instanceId] mutableCopy];
  if (record == nil || self->_iosBatchLedger[instanceId] != nil) return;
  NSMutableArray *queue = [record[@"messageQueue"] mutableCopy];
  if (queue == nil || queue.count == 0) return;
    NSDictionary *options = record[@"options"];
  NSUInteger maxMessages = [options[@"batchMaxMessages"] unsignedIntegerValue] ?: 64;
  NSUInteger maxBytes = [options[@"batchMaxBytes"] unsignedIntegerValue] ?: (256 * 1024);
  NSMutableArray *messages = [NSMutableArray array];
  NSUInteger bytes = 0;
  while (queue.count > 0 && messages.count < maxMessages) {
    NSDictionary *candidate = queue.firstObject;
    NSUInteger candidateBytes = [candidate[@"payloadBytes"] unsignedIntegerValue];
    if (messages.count > 0 && bytes + candidateBytes > maxBytes) break;
    [messages addObject:candidate];
    [queue removeObjectAtIndex:0];
    bytes += candidateBytes;
  }
  if (messages.count == 0) return;
  self->_iosBatchId += 1;
  NSNumber *batchId = @(self->_iosBatchId);
  NSNumber *streamEpoch = record[@"streamEpoch"] ?: @1;
  self->_iosBatchLedger[instanceId] = @{ @"streamEpoch": streamEpoch, @"batchId": batchId, @"messageCount": @(messages.count), @"bytes": @(bytes) };
  record[@"messageQueue"] = queue;
  self->_sessions[instanceId] = record;
  NSMutableArray *wireMessages = [NSMutableArray arrayWithCapacity:messages.count];
  for (NSDictionary *message in messages) {
    NSMutableDictionary *wire = [message mutableCopy];
    [wire removeObjectForKey:@"payloadBytes"];
    [wireMessages addObject:wire];
  }
  [self emitOnMessageBatch:@{ @"instanceId": instanceId, @"streamEpoch": streamEpoch, @"batchId": batchId, @"messages": wireMessages }];
}

- (void)restoreSubscriptionsForInstance:(NSString *)instanceId bridge:(ReactNativeMqttCocoaBridge *)bridge generation:(NSNumber *)generation {
  NSMutableDictionary *record = [self->_sessions[instanceId] mutableCopy];
  NSDictionary *desired = [record[@"desiredSubscriptions"] copy];
  if (record == nil || desired.count == 0) return;
  record[@"restorePending"] = @(desired.count);
  record[@"restoreFailed"] = @NO;
  self->_sessions[instanceId] = record;
  NSMutableArray<NSArray<NSDictionary *> *> *batches = [NSMutableArray array];
  NSMutableArray<NSDictionary *> *batch = [NSMutableArray array];
  NSUInteger batchBytes = 0;
  for (NSString *filter in desired) {
    NSDictionary *intent = desired[filter];
    NSDictionary *request = @{
      @"filter": filter,
      @"qos": intent[@"qos"] ?: @1,
      @"noLocal": intent[@"noLocal"] ?: @NO,
      @"retainAsPublished": intent[@"retainAsPublished"] ?: @NO,
      @"retainHandling": intent[@"retainHandling"] ?: @0,
      @"intentRevision": intent[@"intentRevision"] ?: @0,
    };
    NSUInteger estimatedBytes = [filter lengthOfBytesUsingEncoding:NSUTF8StringEncoding] + 8;
    if (estimatedBytes > 16 * 1024) {
      record[@"restoreFailed"] = @YES;
      NSUInteger pending = [record[@"restorePending"] unsignedIntegerValue];
      record[@"restorePending"] = @(pending > 0 ? pending - 1 : 0);
      continue;
    }
    if (batch.count > 0 && (batch.count >= 32 || batchBytes + estimatedBytes > 16 * 1024)) {
      [batches addObject:[batch copy]];
      batch = [NSMutableArray array];
      batchBytes = 0;
    }
    [batch addObject:request];
    batchBytes += estimatedBytes;
  }
  if (batch.count > 0) [batches addObject:[batch copy]];
  if (batches.count == 0) {
    record[@"subscriptionsStatus"] = @"failed";
    record[@"revision"] = @([record[@"revision"] integerValue] + 1);
    self->_sessions[instanceId] = record;
    [self settleSubscriptionWaitersForInstance:instanceId];
    return;
  }
  self->_sessions[instanceId] = record;
  [self restoreSubscriptionBatches:batches instanceId:instanceId bridge:bridge generation:generation index:0];
}

- (void)restoreSubscriptionBatches:(NSArray<NSArray<NSDictionary *> *> *)batches instanceId:(NSString *)instanceId bridge:(ReactNativeMqttCocoaBridge *)bridge generation:(NSNumber *)generation index:(NSUInteger)index {
  if (index >= batches.count) return;
  NSUInteger waveCount = MIN((NSUInteger)4, batches.count - index);
  NSMutableDictionary *waveRecord = [self->_sessions[instanceId] mutableCopy];
  if (waveRecord == nil) return;
  waveRecord[@"restoreWavePending"] = @(waveCount);
  self->_sessions[instanceId] = waveRecord;
  for (NSUInteger waveOffset = 0; waveOffset < waveCount; waveOffset += 1) {
    NSUInteger batchIndex = index + waveOffset;
  NSArray<NSDictionary *> *batch = batches[batchIndex];
  NSMutableArray<NSDictionary *> *wire = [NSMutableArray arrayWithCapacity:batch.count];
  for (NSDictionary *request in batch) {
    [wire addObject:@{
      @"filter": request[@"filter"],
      @"qos": request[@"qos"],
      @"noLocal": request[@"noLocal"],
      @"retainAsPublished": request[@"retainAsPublished"],
      @"retainHandling": request[@"retainHandling"],
    }];
  }
  [bridge subscribeBatch:wire completion:^(NSArray<NSNumber *> *granted, NSError *error) {
    dispatch_async(self->_sessionQueue, ^{
      NSMutableDictionary *current = [self->_sessions[instanceId] mutableCopy];
      if (self->_invalidated || current == nil || current[@"bridge"] != bridge || ![current[@"state"] isEqual:@"connected"] || ![current[@"generation"] isEqual:generation]) return;
      if (error != nil || granted.count != batch.count) current[@"restoreFailed"] = @YES;
      NSUInteger count = MIN(granted.count, batch.count);
      for (NSUInteger itemIndex = 0; itemIndex < count; itemIndex += 1) {
        NSDictionary *request = batch[itemIndex];
        NSString *filter = request[@"filter"];
        NSDictionary *intent = current[@"desiredSubscriptions"][filter];
        if (intent == nil || ![intent[@"intentRevision"] isEqual:request[@"intentRevision"]]) continue;
        NSNumber *value = granted[itemIndex];
        if (value.integerValue > 2) current[@"restoreFailed"] = @YES;
        else {
          NSMutableDictionary *confirmed = [current[@"confirmedSubscriptions"] mutableCopy] ?: [NSMutableDictionary dictionary];
          confirmed[filter] = @{ @"intentRevision": request[@"intentRevision"], @"grantedQos": value };
          current[@"confirmedSubscriptions"] = confirmed;
        }
        NSUInteger pending = [current[@"restorePending"] unsignedIntegerValue];
        current[@"restorePending"] = @(pending > 0 ? pending - 1 : 0);
      }
      if (error != nil || granted.count != batch.count) {
        NSUInteger unaccounted = batch.count > count ? batch.count - count : batch.count;
        NSUInteger pending = [current[@"restorePending"] unsignedIntegerValue];
        current[@"restorePending"] = @(pending > unaccounted ? pending - unaccounted : 0);
      }
      self->_sessions[instanceId] = current;
      NSUInteger wavePending = [current[@"restoreWavePending"] unsignedIntegerValue];
      current[@"restoreWavePending"] = @(wavePending > 0 ? wavePending - 1 : 0);
      self->_sessions[instanceId] = current;
      if (wavePending <= 1) {
        [self restoreSubscriptionBatches:batches instanceId:instanceId bridge:bridge generation:generation index:index + waveCount];
        NSMutableDictionary *updated = [self->_sessions[instanceId] mutableCopy];
        if ([updated[@"restorePending"] unsignedIntegerValue] == 0) {
          updated[@"subscriptionsStatus"] = [updated[@"restoreFailed"] boolValue] ? @"failed" : @"ready";
          updated[@"revision"] = @([updated[@"revision"] integerValue] + 1);
          self->_sessions[instanceId] = updated;
          [self settleSubscriptionWaitersForInstance:instanceId];
        }
      }
    });
    }];
  }
}

- (void)restoreTombstonesForInstance:(NSString *)instanceId bridge:(ReactNativeMqttCocoaBridge *)bridge generation:(NSNumber *)generation {
  NSMutableDictionary *record = [self->_sessions[instanceId] mutableCopy];
  NSDictionary *tombstones = [record[@"tombstones"] copy];
  if (record == nil || tombstones.count == 0) return;
  record[@"restorePending"] = @(tombstones.count);
  record[@"restoreFailed"] = @NO;
  record[@"subscriptionsStatus"] = @"pending";
  self->_sessions[instanceId] = record;
  NSMutableArray<NSDictionary *> *requests = [NSMutableArray arrayWithCapacity:tombstones.count];
  for (NSString *filter in tombstones) [requests addObject:@{ @"filter": filter, @"intentRevision": tombstones[filter] }];
  [self restoreTombstoneRequests:requests instanceId:instanceId bridge:bridge generation:generation index:0];
}

- (void)restoreTombstoneRequests:(NSArray<NSDictionary *> *)requests instanceId:(NSString *)instanceId bridge:(ReactNativeMqttCocoaBridge *)bridge generation:(NSNumber *)generation index:(NSUInteger)index {
  if (index >= requests.count) return;
  NSString *filter = requests[index][@"filter"];
  NSNumber *intentRevision = requests[index][@"intentRevision"];
  [self enqueueSubscriptionOperation:^(dispatch_block_t finish) {
    [bridge unsubscribeFilter:filter completion:^(NSError *error) {
      dispatch_async(self->_sessionQueue, ^{
        NSMutableDictionary *current = [self->_sessions[instanceId] mutableCopy];
        if (self->_invalidated || current == nil || current[@"bridge"] != bridge || ![current[@"state"] isEqual:@"connected"] || ![current[@"generation"] isEqual:generation]) { finish(); return; }
        if (error != nil) {
          current[@"restoreFailed"] = @YES;
        } else if ([current[@"tombstones"][filter] isEqual:intentRevision]) {
          NSMutableDictionary *currentTombstones = [current[@"tombstones"] mutableCopy] ?: [NSMutableDictionary dictionary];
          [currentTombstones removeObjectForKey:filter];
          current[@"tombstones"] = currentTombstones;
          NSMutableDictionary *confirmed = [current[@"confirmedSubscriptions"] mutableCopy] ?: [NSMutableDictionary dictionary];
          [confirmed removeObjectForKey:filter];
          current[@"confirmedSubscriptions"] = confirmed;
        }
        NSUInteger pending = [current[@"restorePending"] unsignedIntegerValue];
        current[@"restorePending"] = @(pending > 0 ? pending - 1 : 0);
        self->_sessions[instanceId] = current;
        [self restoreTombstoneRequests:requests instanceId:instanceId bridge:bridge generation:generation index:index + 1];
        NSMutableDictionary *updated = [self->_sessions[instanceId] mutableCopy];
        if ([updated[@"restorePending"] unsignedIntegerValue] == 0) {
          updated[@"subscriptionsStatus"] = [updated[@"restoreFailed"] boolValue] ? @"failed" : @"ready";
          updated[@"revision"] = @([updated[@"revision"] integerValue] + 1);
          self->_sessions[instanceId] = updated;
          [self settleSubscriptionWaitersForInstance:instanceId];
        }
        finish();
      });
    }];
  } instanceId:instanceId filter:filter];
}

- (void)settleSubscriptionWaitersForInstance:(NSString *)instanceId {
  NSMutableDictionary *record = [self->_sessions[instanceId] mutableCopy];
  NSMutableArray *waiters = [record[@"subscriptionWaiters"] mutableCopy];
  if (record == nil || waiters.count == 0) return;
  NSMutableArray *remaining = [NSMutableArray array];
  for (NSDictionary *waiter in waiters) {
    NSNumber *waiterGeneration = waiter[@"generation"];
    NSNumber *desiredRevision = waiter[@"desiredRevision"];
    RCTPromiseResolveBlock resolve = waiter[@"resolve"];
    RCTPromiseRejectBlock reject = waiter[@"reject"];
    BOOL generationMatches = [record[@"generation"] isEqual:waiterGeneration];
    if (![record[@"state"] isEqual:@"connected"] || !generationMatches) {
      reject(@"ERR_CONNECTION_LOST", @"Subscription generation changed", nil);
    } else if ([record[@"subscriptionsStatus"] isEqual:@"failed"]) {
      reject(@"ERR_SUBSCRIBE_FAILED", @"Subscription synchronization failed", nil);
    } else if ([record[@"subscriptionsStatus"] isEqual:@"ready"] && [record[@"subscriptionsRevision"] doubleValue] >= [desiredRevision doubleValue]) {
      resolve([self snapshotForInstance:instanceId state:record[@"state"] revision:record[@"revision"] generation:record[@"generation"] lastConnAck:record[@"lastConnAck"] deliveryStatus:@"active"]);
    } else {
      [remaining addObject:waiter];
    }
  }
  record[@"subscriptionWaiters"] = remaining;
  self->_sessions[instanceId] = record;
}

- (void)bindMessageHandler:(ReactNativeMqttCocoaBridge *)bridge instanceId:(NSString *)instanceId token:(NSNumber *)handlerToken {
  __weak ReactNativeMqtt *weakSelf = self;
  __weak ReactNativeMqttCocoaBridge *weakBridge = bridge;
  [bridge setSessionEventHandler:^(NSString *state, NSNumber *revision, NSNumber *generation, NSDictionary *ack) {
    ReactNativeMqtt *strongSelf = weakSelf;
    ReactNativeMqttCocoaBridge *strongBridge = weakBridge;
    if (strongSelf == nil || strongBridge == nil) return;
    dispatch_async(strongSelf->_sessionQueue, ^{
      NSMutableDictionary *record = [strongSelf->_sessions[instanceId] mutableCopy];
      if (strongSelf->_invalidated || record == nil || record[@"bridge"] != strongBridge) return;
      record[@"state"] = state;
      record[@"revision"] = revision;
      record[@"generation"] = generation;
      if ([state isEqualToString:@"reconnecting"]) {
        record[@"reconnectAttempts"] = @([record[@"reconnectAttempts"] unsignedIntegerValue] + 1);
        [strongSelf->_iosSubscriptionQueues removeObjectForKey:instanceId];
        [strongSelf settleSubscriptionWaitersForInstance:instanceId];
      } else if ([state isEqualToString:@"connected"] && ack != nil) {
        record[@"lastConnAck"] = ack;
        record[@"reconnectAttempts"] = @0;
        BOOL sessionPresent = [ack[@"sessionPresent"] boolValue];
        BOOL hasTombstones = [record[@"tombstones"] count] != 0;
        if (!sessionPresent) {
          [record[@"tombstones"] removeAllObjects];
          hasTombstones = NO;
        }
        record[@"subscriptionsStatus"] = (([record[@"desiredSubscriptions"] count] == 0 && !hasTombstones) || (sessionPresent && !hasTombstones)) ? @"ready" : @"pending";
      }
      strongSelf->_sessions[instanceId] = record;
      [strongSelf emitOnSessionSnapshot:[strongSelf snapshotForInstance:instanceId state:state revision:revision generation:generation lastConnAck:record[@"lastConnAck"] deliveryStatus:@"active"]];
      if ([state isEqualToString:@"connected"] && ack != nil) {
        BOOL sessionPresent = [ack[@"sessionPresent"] boolValue];
        BOOL hasTombstones = [record[@"tombstones"] count] != 0;
        if ([record[@"desiredSubscriptions"] count] != 0 && !sessionPresent) {
          [strongSelf restoreSubscriptionsForInstance:instanceId bridge:strongBridge generation:generation];
        } else if (sessionPresent && hasTombstones) {
          [strongSelf restoreTombstonesForInstance:instanceId bridge:strongBridge generation:generation];
        }
        NSArray *pending = [record[@"connectWaiters"] copy] ?: @[];
        record[@"connectWaiters"] = [NSMutableArray array];
        strongSelf->_sessions[instanceId] = record;
        NSDictionary *snapshot = [strongSelf snapshotForInstance:instanceId state:@"connected" revision:revision generation:generation lastConnAck:ack deliveryStatus:@"active"];
        for (NSDictionary *waiter in pending) ((RCTPromiseResolveBlock)waiter[@"resolve"])(@{ @"snapshot": snapshot, @"ack": ack });
      }
    });
  }];
  [bridge setDisconnectedHandler:^(NSNumber *reasonCode, NSString *reasonString) {
    ReactNativeMqtt *strongSelf = weakSelf;
    ReactNativeMqttCocoaBridge *strongBridge = weakBridge;
    if (strongSelf == nil || strongBridge == nil) return;
    dispatch_async(strongSelf->_sessionQueue, ^{
      NSMutableDictionary *record = [strongSelf->_sessions[instanceId] mutableCopy];
      if (strongSelf->_invalidated || record == nil || record[@"bridge"] != strongBridge || ![record[@"reconnectToken"] isEqual:handlerToken]) return;
      if (reasonCode != nil) {
        [strongSelf emitOnDiagnostic:@{
          @"instanceId": instanceId,
          @"generation": record[@"generation"] ?: @0,
          @"code": @"SERVER_DISCONNECT",
          @"message": @"Broker disconnected the MQTT session",
          @"operation": @"disconnect",
          @"reasonCode": reasonCode,
          @"reasonString": reasonString ?: @"",
        }];
      }
    });
  }];
  [bridge setMessageHandler:^(NSString *topic, NSData *payload, NSNumber *qos, BOOL retained, NSDictionary *metadata) {
    ReactNativeMqtt *strongSelf = weakSelf;
    if (strongSelf == nil) return;
    dispatch_async(strongSelf->_sessionQueue, ^{
      NSMutableDictionary *record = [strongSelf->_sessions[instanceId] mutableCopy];
      if (strongSelf->_invalidated || record == nil || ![record[@"state"] isEqual:@"connected"]) return;
      NSDictionary *options = record[@"options"];
      NSUInteger payloadBytes = payload.length;
      NSUInteger maxMessageBytes = [options[@"maxMessageBytes"] unsignedIntegerValue] ?: (1024 * 1024);
      NSMutableArray *queue = [record[@"messageQueue"] mutableCopy] ?: [NSMutableArray array];
      NSDictionary *inFlight = strongSelf->_iosBatchLedger[instanceId];
      NSUInteger inFlightMessages = [inFlight[@"messageCount"] unsignedIntegerValue];
      NSUInteger inFlightBytes = [inFlight[@"bytes"] unsignedIntegerValue];
      NSUInteger maxQueuedMessages = [options[@"maxQueuedMessages"] unsignedIntegerValue] ?: 1024;
      NSUInteger maxQueuedBytes = [options[@"maxQueuedBytes"] unsignedIntegerValue] ?: (8 * 1024 * 1024);
      NSNumber *queuedBytesNumber = [queue valueForKeyPath:@"@sum.payloadBytes"];
      NSUInteger queuedBytes = queuedBytesNumber.unsignedIntegerValue;
      BOOL overLimit = payloadBytes > maxMessageBytes || queue.count + inFlightMessages >= maxQueuedMessages || inFlightBytes + queuedBytes + payloadBytes > maxQueuedBytes;
      if (overLimit) {
        NSString *policy = options[@"overflowPolicy"] ?: @"disconnect";
        record[@"droppedMessages"] = @([record[@"droppedMessages"] unsignedIntegerValue] + 1);
        if (![record[@"overflowDiagnosticPending"] boolValue]) {
          record[@"overflowDiagnosticPending"] = @YES;
          [strongSelf emitOnDiagnostic:@{ @"instanceId": instanceId, @"generation": record[@"generation"] ?: @0, @"code": @"QUEUE_OVERFLOW", @"message": @"Message delivery queue limit reached", @"operation": @"onMessageBatch", @"metrics": @{ @"queuedMessages": @(queue.count + inFlightMessages), @"queuedBytes": @(inFlightBytes + queuedBytes), @"pendingOperations": @([record[@"operationWaiters"] count] + [record[@"subscriptionWaiters"] count]), @"droppedMessages": record[@"droppedMessages"], @"reconnectAttempts": record[@"reconnectAttempts"] ?: @0 } }];
        }
        // Preserve the drop counter and coalescing marker for drop-newest;
        // this branch must not lose the mutable session copy before return.
        strongSelf->_sessions[instanceId] = record;
        if ([policy isEqualToString:@"disconnect"]) {
          record[@"reconnectToken"] = @([record[@"reconnectToken"] unsignedIntegerValue] + 1);
          for (NSMutableDictionary *waiter in [record[@"operationWaiters"] copy] ?: @[]) {
            if (![waiter[@"settled"] boolValue]) {
              waiter[@"settled"] = @YES;
              ((RCTPromiseRejectBlock)waiter[@"reject"])(@"ERR_CONNECTION_LOST", @"Operation was cancelled by queue overflow", nil);
            }
          }
          record[@"operationWaiters"] = [NSMutableArray array];
          record[@"state"] = @"disconnected";
          record[@"revision"] = @([record[@"revision"] integerValue] + 1);
          strongSelf->_sessions[instanceId] = record;
          [strongSelf->_iosSubscriptionQueues removeObjectForKey:instanceId];
          [strongSelf settleSubscriptionWaitersForInstance:instanceId];
          [weakBridge disconnect];
          [strongSelf emitOnSessionSnapshot:[strongSelf snapshotForInstance:instanceId
                                                                    state:record[@"state"]
                                                                revision:record[@"revision"]
                                                              generation:record[@"generation"]
                                                             lastConnAck:record[@"lastConnAck"]
                                                            deliveryStatus:@"active"]];
        }
        return;
      }
      NSMutableDictionary *wireMessage = [@{ @"instanceId": instanceId, @"generation": record[@"generation"] ?: @0, @"topic": topic, @"payloadBase64": [payload base64EncodedStringWithOptions:0] ?: @"", @"qos": qos ?: @0, @"retained": @(retained), @"payloadBytes": @(payloadBytes) } mutableCopy];
      [metadata enumerateKeysAndObjectsUsingBlock:^(NSString *key, id value, BOOL *stop) {
        (void)stop;
        if (value != [NSNull null]) wireMessage[key] = value;
      }];
      [queue addObject:wireMessage];
      record[@"messageQueue"] = queue;
      strongSelf->_sessions[instanceId] = record;
      [strongSelf emitNextMessageBatchForInstance:instanceId];
    });
  }];
}
- (void)connect:(NSString *)instanceId resolve:(RCTPromiseResolveBlock)resolve reject:(RCTPromiseRejectBlock)reject {
  dispatch_async(_sessionQueue, ^{
    NSMutableDictionary *record = [self->_sessions[instanceId] mutableCopy];
    ReactNativeMqttCocoaBridge *bridge = record[@"bridge"];
    if (record == nil || bridge == nil) { reject(@"ERR_NATIVE_UNAVAILABLE", @"Session is not open", nil); return; }
    if ([record[@"state"] isEqual:@"closing"]) { reject(@"ERR_DISPOSED", @"Session is closing", nil); return; }
    if ([record[@"state"] isEqual:@"connected"]) {
      NSDictionary *snapshot = [self snapshotForInstance:instanceId state:record[@"state"] revision:record[@"revision"] generation:record[@"generation"] lastConnAck:record[@"lastConnAck"] deliveryStatus:@"active"];
      resolve(@{ @"snapshot": snapshot, @"ack": record[@"lastConnAck"] });
      return;
    }
    NSMutableArray *waiters = [record[@"connectWaiters"] mutableCopy] ?: [NSMutableArray array];
    [waiters addObject:@{ @"resolve": [resolve copy], @"reject": [reject copy] }];
    if ([record[@"state"] isEqual:@"connecting"]) {
      record[@"connectWaiters"] = waiters;
      self->_sessions[instanceId] = record;
      return;
    }
    if ([record[@"state"] isEqual:@"reconnecting"]) {
      NSDictionary *waiter = waiters.lastObject;
      record[@"connectWaiters"] = waiters;
      self->_sessions[instanceId] = record;
      NSUInteger timeoutMs = [record[@"options"][@"connectTimeoutMs"] unsignedIntegerValue] ?: 30000;
      dispatch_after(dispatch_time(DISPATCH_TIME_NOW, (int64_t)timeoutMs * NSEC_PER_MSEC), self->_sessionQueue, ^{
        NSMutableDictionary *current = [self->_sessions[instanceId] mutableCopy];
        NSMutableArray *currentWaiters = [current[@"connectWaiters"] mutableCopy];
        if (current == nil || currentWaiters == nil || ![currentWaiters containsObject:waiter]) return;
        [currentWaiters removeObject:waiter];
        current[@"connectWaiters"] = currentWaiters;
        self->_sessions[instanceId] = current;
        NSError *timeoutError = [NSError errorWithDomain:@"ReactNativeMqtt" code:408 userInfo:@{ NSLocalizedDescriptionKey: @"Connection timed out while reconnecting" }];
        ((RCTPromiseRejectBlock)waiter[@"reject"])(@"ERR_CONNECT_TIMEOUT", timeoutError.localizedDescription, timeoutError);
      });
      return;
    }
    if (record[@"lastConnAck"] != [NSNull null]) {
      bridge = [self bridgeForSessionOptions:record[@"options"] clientId:record[@"clientId"] ?: instanceId];
      record[@"bridge"] = bridge;
    }
    record[@"state"] = @"connecting";
    record[@"revision"] = @([record[@"revision"] integerValue] + 1);
    record[@"reconnectToken"] = @([record[@"reconnectToken"] unsignedIntegerValue] + 1);
    record[@"connectWaiters"] = waiters;
    self->_sessions[instanceId] = record;
    [self bindMessageHandler:bridge instanceId:instanceId token:record[@"reconnectToken"]];
    __block BOOL settled = NO;
    [bridge connectWithCompletion:^(NSDictionary *ack, NSError *error) {
      dispatch_async(self->_sessionQueue, ^{
        if (settled) return;
        settled = YES;
        NSMutableDictionary *current = [self->_sessions[instanceId] mutableCopy];
        NSArray *pending = [current[@"connectWaiters"] copy] ?: @[];
        if (self->_invalidated || current == nil || current[@"bridge"] != bridge) {
          for (NSDictionary *waiter in pending) ((RCTPromiseRejectBlock)waiter[@"reject"])(@"ERR_DISPOSED", @"Native module is invalidated", nil);
          return;
        }
        if (error != nil) {
          if (![current[@"state"] isEqual:@"connecting"] && ![current[@"state"] isEqual:@"disconnected"]) return;
          current[@"connectWaiters"] = [NSMutableArray array];
          current[@"state"] = @"disconnected";
          current[@"revision"] = @([current[@"revision"] integerValue] + 1);
          self->_sessions[instanceId] = current;
          NSNumber *callbackGeneration = @([current[@"generation"] integerValue] + 1);
          NSError *annotatedError = RNMqttAnnotatedError(error, @"connect", callbackGeneration, @"unknown");
          for (NSDictionary *waiter in pending) ((RCTPromiseRejectBlock)waiter[@"reject"])(@"ERR_CONNECTION_REFUSED", annotatedError.localizedDescription, annotatedError);
          return;
        }
        // The Swift Session's connect event owns the successful transition,
        // ACK cache, generation, and connect waiter settlement.
      });
    }];
    NSUInteger timeoutMs = [record[@"options"][@"connectTimeoutMs"] unsignedIntegerValue] ?: 30000;
    dispatch_after(dispatch_time(DISPATCH_TIME_NOW, (int64_t)timeoutMs * NSEC_PER_MSEC), self->_sessionQueue, ^{
      if (settled) return;
      settled = YES;
      NSMutableDictionary *current = [self->_sessions[instanceId] mutableCopy];
      if (self->_invalidated || current == nil || current[@"bridge"] != bridge || ![current[@"state"] isEqual:@"connecting"]) return;
      NSArray *pending = [current[@"connectWaiters"] copy] ?: @[];
      current[@"connectWaiters"] = [NSMutableArray array];
      current[@"state"] = @"disconnected";
      current[@"revision"] = @([current[@"revision"] integerValue] + 1);
      current[@"reconnectToken"] = @([current[@"reconnectToken"] unsignedIntegerValue] + 1);
      self->_sessions[instanceId] = current;
      [bridge disconnect];
      NSError *timeoutError = [NSError errorWithDomain:@"ReactNativeMqtt" code:408 userInfo:@{ NSLocalizedDescriptionKey: @"Connection timed out" }];
      for (NSDictionary *waiter in pending) ((RCTPromiseRejectBlock)waiter[@"reject"])(@"ERR_CONNECT_TIMEOUT", timeoutError.localizedDescription, timeoutError);
    });
  });
}
- (void)disconnect:(NSString *)instanceId resolve:(RCTPromiseResolveBlock)resolve reject:(RCTPromiseRejectBlock)reject {
  dispatch_async(_sessionQueue, ^{
    NSMutableDictionary *record = [self->_sessions[instanceId] mutableCopy];
    if (record == nil || [record[@"state"] isEqual:@"closed"]) { resolve([self snapshotForInstance:instanceId state:@"closed" revision:@0 generation:@0 lastConnAck:[NSNull null] deliveryStatus:@"closed"]); return; }
    ReactNativeMqttCocoaBridge *bridge = record[@"bridge"];
    if (bridge == nil) { resolve([self snapshotForInstance:instanceId state:record[@"state"] revision:record[@"revision"] generation:record[@"generation"] lastConnAck:record[@"lastConnAck"] deliveryStatus:@"active"]); return; }
    if ([record[@"state"] isEqual:@"connecting"] || [record[@"state"] isEqual:@"reconnecting"]) {
      NSArray *pending = [record[@"connectWaiters"] copy] ?: @[];
      record[@"connectWaiters"] = [NSMutableArray array];
      for (NSMutableDictionary *waiter in [record[@"operationWaiters"] copy] ?: @[]) {
        if (![waiter[@"settled"] boolValue]) {
          waiter[@"settled"] = @YES;
          ((RCTPromiseRejectBlock)waiter[@"reject"])(@"ERR_CONNECTION_LOST", @"Operation was cancelled by disconnect", nil);
        }
      }
      record[@"operationWaiters"] = [NSMutableArray array];
      record[@"state"] = @"disconnected";
      record[@"revision"] = @([record[@"revision"] integerValue] + 1);
      record[@"reconnectToken"] = @([record[@"reconnectToken"] unsignedIntegerValue] + 1);
      self->_sessions[instanceId] = record;
      [self->_iosSubscriptionQueues removeObjectForKey:instanceId];
      [bridge disconnect];
      for (NSDictionary *waiter in pending) ((RCTPromiseRejectBlock)waiter[@"reject"])(@"ERR_CONNECTION_LOST", @"Connection was stopped", nil);
      [self emitOnSessionSnapshot:[self snapshotForInstance:instanceId state:record[@"state"] revision:record[@"revision"] generation:record[@"generation"] lastConnAck:record[@"lastConnAck"] deliveryStatus:@"active"]];
      resolve([self snapshotForInstance:instanceId state:record[@"state"] revision:record[@"revision"] generation:record[@"generation"] lastConnAck:record[@"lastConnAck"] deliveryStatus:@"active"]);
      return;
    }
    if (![record[@"state"] isEqual:@"connected"]) { resolve([self snapshotForInstance:instanceId state:record[@"state"] revision:record[@"revision"] generation:record[@"generation"] lastConnAck:record[@"lastConnAck"] deliveryStatus:@"active"]); return; }
    record[@"state"] = @"disconnecting";
    record[@"revision"] = @([record[@"revision"] integerValue] + 1);
    record[@"reconnectToken"] = @([record[@"reconnectToken"] unsignedIntegerValue] + 1);
    for (NSMutableDictionary *waiter in [record[@"operationWaiters"] copy] ?: @[]) {
      if (![waiter[@"settled"] boolValue]) {
        waiter[@"settled"] = @YES;
        ((RCTPromiseRejectBlock)waiter[@"reject"])(@"ERR_CONNECTION_LOST", @"Operation was cancelled by disconnect", nil);
      }
    }
    record[@"operationWaiters"] = [NSMutableArray array];
    self->_sessions[instanceId] = record;
    [self->_iosSubscriptionQueues removeObjectForKey:instanceId];
    [self settleSubscriptionWaitersForInstance:instanceId];
    __block BOOL settled = NO;
    void (^finish)(NSError *) = ^(NSError *error) {
      if (settled) return;
      settled = YES;
      NSMutableDictionary *current = [self->_sessions[instanceId] mutableCopy];
      if (current == nil || current[@"bridge"] != bridge) return;
      current[@"state"] = @"disconnected";
      current[@"revision"] = @([current[@"revision"] integerValue] + 1);
      self->_sessions[instanceId] = current;
      if (error != nil) { reject(@"ERR_CLEANUP_FAILED", error.localizedDescription, error); return; }
      resolve([self snapshotForInstance:instanceId state:current[@"state"] revision:current[@"revision"] generation:current[@"generation"] lastConnAck:current[@"lastConnAck"] deliveryStatus:@"active"]);
    };
    [bridge disconnectWithCompletion:^(NSError *error) { dispatch_async(self->_sessionQueue, ^{ finish(error); }); }];
    NSUInteger timeoutMs = [record[@"options"][@"disconnectTimeoutMs"] unsignedIntegerValue] ?: 5000;
    dispatch_after(dispatch_time(DISPATCH_TIME_NOW, (int64_t)timeoutMs * NSEC_PER_MSEC), self->_sessionQueue, ^{
      if (settled) return;
      [bridge disconnect];
      finish([NSError errorWithDomain:@"ReactNativeMqtt" code:408 userInfo:@{ NSLocalizedDescriptionKey: @"Disconnect timed out" }]);
    });
  });
}
- (void)close:(NSString *)instanceId resolve:(RCTPromiseResolveBlock)resolve reject:(RCTPromiseRejectBlock)reject {
  dispatch_async(_sessionQueue, ^{
    NSMutableDictionary *existing = [self->_sessions[instanceId] mutableCopy];
    if (existing == nil) { resolve([self snapshotForInstance:instanceId state:@"closed" revision:@0 generation:@0 lastConnAck:[NSNull null] deliveryStatus:@"closed"]); return; }
    NSNumber *revision = existing == nil ? @0 : @([existing[@"revision"] integerValue] + 1);
    for (NSDictionary *waiter in [existing[@"connectWaiters"] copy] ?: @[]) {
      ((RCTPromiseRejectBlock)waiter[@"reject"])(@"ERR_DISPOSED", @"Session was closed", nil);
    }
    for (NSDictionary *waiter in [existing[@"subscriptionWaiters"] copy] ?: @[]) {
      ((RCTPromiseRejectBlock)waiter[@"reject"])(@"ERR_DISPOSED", @"Session was closed", nil);
    }
    for (NSMutableDictionary *waiter in [existing[@"operationWaiters"] copy] ?: @[]) {
      if (![waiter[@"settled"] boolValue]) {
        waiter[@"settled"] = @YES;
        ((RCTPromiseRejectBlock)waiter[@"reject"])(@"ERR_DISPOSED", @"Session was closed", nil);
      }
    }
    ReactNativeMqttCocoaBridge *bridge = existing[@"bridge"];
    if (bridge == nil || [existing[@"state"] isEqual:@"idle"] || [existing[@"state"] isEqual:@"disconnected"] || [existing[@"state"] isEqual:@"closed"]) {
      [bridge disconnect];
      [self->_sessions removeObjectForKey:instanceId];
      [self->_iosBatchLedger removeObjectForKey:instanceId];
      [self->_iosSubscriptionQueues removeObjectForKey:instanceId];
      self->_iosStreamEpochs[instanceId] = @([self->_iosStreamEpochs[instanceId] unsignedLongLongValue] + 1);
      resolve([self snapshotForInstance:instanceId state:@"closed" revision:revision generation:existing[@"generation"] ?: @0 lastConnAck:existing[@"lastConnAck"] deliveryStatus:@"closed"]);
      return;
    }
    existing[@"state"] = @"closing";
    existing[@"revision"] = revision;
    existing[@"reconnectToken"] = @([existing[@"reconnectToken"] unsignedIntegerValue] + 1);
    existing[@"connectWaiters"] = [NSMutableArray array];
    existing[@"subscriptionWaiters"] = [NSMutableArray array];
    self->_sessions[instanceId] = existing;
    __block BOOL settled = NO;
    void (^finish)(NSError *) = ^(NSError *error) {
      if (settled) return;
      settled = YES;
      NSMutableDictionary *current = [self->_sessions[instanceId] mutableCopy];
      if (current == nil || current[@"bridge"] != bridge) return;
      NSNumber *closedRevision = @([current[@"revision"] integerValue] + 1);
      [self->_sessions removeObjectForKey:instanceId];
      [self->_iosBatchLedger removeObjectForKey:instanceId];
      [self->_iosSubscriptionQueues removeObjectForKey:instanceId];
      self->_iosStreamEpochs[instanceId] = @([self->_iosStreamEpochs[instanceId] unsignedLongLongValue] + 1);
      if (error != nil) { reject(@"ERR_CLEANUP_FAILED", error.localizedDescription, error); return; }
      resolve([self snapshotForInstance:instanceId state:@"closed" revision:closedRevision generation:current[@"generation"] ?: @0 lastConnAck:current[@"lastConnAck"] deliveryStatus:@"closed"]);
    };
    [bridge disposeWithCompletion:^(NSError *error) { dispatch_async(self->_sessionQueue, ^{ finish(error); }); }];
    NSUInteger timeoutMs = [existing[@"options"][@"disconnectTimeoutMs"] unsignedIntegerValue] ?: 5000;
    dispatch_after(dispatch_time(DISPATCH_TIME_NOW, (int64_t)timeoutMs * NSEC_PER_MSEC), self->_sessionQueue, ^{
      if (settled) return;
      [bridge disconnect];
      finish([NSError errorWithDomain:@"ReactNativeMqtt" code:408 userInfo:@{ NSLocalizedDescriptionKey: @"Close timed out while disconnecting" }]);
    });
  });
}
- (void)getSnapshot:(NSString *)instanceId resolve:(RCTPromiseResolveBlock)resolve reject:(RCTPromiseRejectBlock)reject { (void)reject; dispatch_async(_sessionQueue, ^{ NSDictionary *existing = self->_sessions[instanceId]; if (existing == nil) { resolve([self snapshotForInstance:instanceId state:@"closed" revision:@0 generation:@0 lastConnAck:[NSNull null] deliveryStatus:@"closed"]); } else { resolve([self snapshotForInstance:instanceId state:existing[@"state"] revision:existing[@"revision"] generation:existing[@"generation"] lastConnAck:existing[@"lastConnAck"] deliveryStatus:@"active"]); } }); }
- (void)setSubscription:(NSString *)instanceId filter:(NSString *)filter options:(JS::NativeReactNativeMqtt::WireSubscribeOptions &)options intentRevision:(double)intentRevision resolve:(RCTPromiseResolveBlock)resolve reject:(RCTPromiseRejectBlock)reject {
  if (!RNMqttValidTopicFilter(filter)) { reject(@"ERR_INVALID_OPTIONS", @"topic filter is invalid", nil); return; }
  NSNumber *requestedQos = @(options.qos().value_or(1));
  BOOL requestedNoLocal = options.noLocal().value_or(false);
  BOOL requestedRetainAsPublished = options.retainAsPublished().value_or(false);
  NSNumber *requestedRetainHandling = @(options.retainHandling().value_or(0));
  dispatch_async(_sessionQueue, ^{
    NSMutableDictionary *record = [self->_sessions[instanceId] mutableCopy];
    ReactNativeMqttCocoaBridge *bridge = record[@"bridge"];
    if (record == nil || bridge == nil || ![record[@"state"] isEqual:@"connected"]) { reject(@"ERR_NOT_CONNECTED", @"Session is not connected", nil); return; }
    NSMutableDictionary *desired = [record[@"desiredSubscriptions"] mutableCopy] ?: [NSMutableDictionary dictionary];
    desired[filter] = @{ @"qos": requestedQos, @"noLocal": @(requestedNoLocal), @"retainAsPublished": @(requestedRetainAsPublished), @"retainHandling": requestedRetainHandling, @"intentRevision": @(intentRevision) };
    record[@"desiredSubscriptions"] = desired;
    NSMutableDictionary *tombstones = [record[@"tombstones"] mutableCopy] ?: [NSMutableDictionary dictionary];
    [tombstones removeObjectForKey:filter];
    record[@"tombstones"] = tombstones;
    record[@"subscriptionsRevision"] = @(MAX([record[@"subscriptionsRevision"] doubleValue], intentRevision));
    record[@"subscriptionsStatus"] = @"pending";
    record[@"pendingSubscriptionOperations"] = @([record[@"pendingSubscriptionOperations"] unsignedIntegerValue] + 1);
    NSMutableDictionary *operationWaiter = [self operationWaiterWithReject:reject operation:@"subscribe" topic:filter generation:record[@"generation"]];
    NSMutableArray *operationWaiters = [record[@"operationWaiters"] mutableCopy] ?: [NSMutableArray array];
    [operationWaiters addObject:operationWaiter];
    record[@"operationWaiters"] = operationWaiters;
    self->_sessions[instanceId] = record;
    __block BOOL settled = NO;
    __block dispatch_block_t finishOperation;
    [self enqueueSubscriptionOperation:^(dispatch_block_t finish) {
      finishOperation = [finish copy];
      [bridge subscribeFilter:filter qos:requestedQos noLocal:requestedNoLocal retainAsPublished:requestedRetainAsPublished retainHandling:requestedRetainHandling completion:^(NSNumber *granted, NSError *error) {
      dispatch_async(self->_sessionQueue, ^{
        if (settled || [operationWaiter[@"settled"] boolValue]) { finish(); return; }
        settled = YES;
        operationWaiter[@"settled"] = @YES;
        NSMutableDictionary *current = [self->_sessions[instanceId] mutableCopy];
        NSMutableArray *remainingOperations = [current[@"operationWaiters"] mutableCopy] ?: [NSMutableArray array];
        [remainingOperations removeObject:operationWaiter];
        current[@"operationWaiters"] = remainingOperations;
        NSDictionary *intent = current[@"desiredSubscriptions"][filter];
        if (error != nil || granted == nil || granted.integerValue >= 0x80) {
          NSError *subscribeError = error;
          if (subscribeError == nil) {
            NSNumber *reasonCode = granted ?: @0x80;
            subscribeError = [NSError errorWithDomain:@"ReactNativeMqtt" code:reasonCode.integerValue userInfo:@{
              NSLocalizedDescriptionKey: [NSString stringWithFormat:@"Subscription rejected (reason 0x%02lX)", (long)reasonCode.integerValue],
              @"reasonCode": reasonCode
            }];
          }
          subscribeError = RNMqttAnnotatedError(subscribeError, @"subscribe", current[@"generation"], @"rejected");
          current[@"pendingSubscriptionOperations"] = @([current[@"pendingSubscriptionOperations"] unsignedIntegerValue] > 0 ? [current[@"pendingSubscriptionOperations"] unsignedIntegerValue] - 1 : 0);
          if ([intent[@"intentRevision"] doubleValue] == intentRevision) current[@"subscriptionsStatus"] = @"failed";
          self->_sessions[instanceId] = current;
          [self settleSubscriptionWaitersForInstance:instanceId];
          reject(@"ERR_SUBSCRIBE_FAILED", subscribeError.localizedDescription, subscribeError);
        } else if ([intent[@"intentRevision"] doubleValue] != intentRevision) {
          [self settleSubscriptionWaitersForInstance:instanceId];
          reject(@"ERR_OPERATION_SUPERSEDED", @"Subscription intent was superseded", nil);
        } else {
          current[@"pendingSubscriptionOperations"] = @([current[@"pendingSubscriptionOperations"] unsignedIntegerValue] > 0 ? [current[@"pendingSubscriptionOperations"] unsignedIntegerValue] - 1 : 0);
          current[@"subscriptionsStatus"] = [current[@"pendingSubscriptionOperations"] unsignedIntegerValue] == 0 ? @"ready" : @"pending";
          NSMutableDictionary *confirmed = [current[@"confirmedSubscriptions"] mutableCopy] ?: [NSMutableDictionary dictionary];
          confirmed[filter] = @{ @"intentRevision": @(intentRevision), @"grantedQos": granted ?: @0 };
          current[@"confirmedSubscriptions"] = confirmed;
          current[@"revision"] = @([current[@"revision"] integerValue] + 1);
          self->_sessions[instanceId] = current;
          [self settleSubscriptionWaitersForInstance:instanceId];
          resolve(@{ @"generation": current[@"generation"] ?: @0, @"intentRevision": @(intentRevision), @"grantedQos": granted ?: @0, @"reasonCode": granted ?: @0 });
        }
        finish();
      });
      }];
    } instanceId:instanceId filter:filter];
    NSUInteger timeoutMs = [record[@"options"][@"operationTimeoutMs"] unsignedIntegerValue] ?: 30000;
    dispatch_after(dispatch_time(DISPATCH_TIME_NOW, (int64_t)timeoutMs * NSEC_PER_MSEC), self->_sessionQueue, ^{
      if (settled) return;
      settled = YES;
      operationWaiter[@"settled"] = @YES;
      NSMutableDictionary *current = [self->_sessions[instanceId] mutableCopy];
      if (current != nil) {
        NSMutableArray *remainingOperations = [current[@"operationWaiters"] mutableCopy] ?: [NSMutableArray array];
        [remainingOperations removeObject:operationWaiter];
        current[@"operationWaiters"] = remainingOperations;
        current[@"pendingSubscriptionOperations"] = @([current[@"pendingSubscriptionOperations"] unsignedIntegerValue] > 0 ? [current[@"pendingSubscriptionOperations"] unsignedIntegerValue] - 1 : 0);
        current[@"subscriptionsStatus"] = @"failed";
        self->_sessions[instanceId] = current;
        [self settleSubscriptionWaitersForInstance:instanceId];
      }
      reject(@"ERR_OPERATION_TIMEOUT", @"Subscription operation timed out", nil);
      if (finishOperation != nil) finishOperation();
    });
  });
}
- (void)removeSubscription:(NSString *)instanceId filter:(NSString *)filter intentRevision:(double)intentRevision resolve:(RCTPromiseResolveBlock)resolve reject:(RCTPromiseRejectBlock)reject {
  if (!RNMqttValidTopicFilter(filter)) { reject(@"ERR_INVALID_OPTIONS", @"topic filter is invalid", nil); return; }
  dispatch_async(_sessionQueue, ^{
    NSMutableDictionary *record = [self->_sessions[instanceId] mutableCopy];
    ReactNativeMqttCocoaBridge *bridge = record[@"bridge"];
    if (record == nil || bridge == nil) { reject(@"ERR_NOT_CONNECTED", @"Session is not open", nil); return; }
    NSMutableDictionary *desired = [record[@"desiredSubscriptions"] mutableCopy] ?: [NSMutableDictionary dictionary];
    NSDictionary *previous = desired[filter];
    [desired removeObjectForKey:filter];
    record[@"desiredSubscriptions"] = desired;
    NSMutableDictionary *tombstones = [record[@"tombstones"] mutableCopy] ?: [NSMutableDictionary dictionary];
    tombstones[filter] = @(intentRevision);
    record[@"tombstones"] = tombstones;
    record[@"subscriptionsRevision"] = @(MAX([record[@"subscriptionsRevision"] doubleValue], intentRevision));
    if (![record[@"state"] isEqual:@"connected"]) {
      record[@"subscriptionsStatus"] = @"pending";
      self->_sessions[instanceId] = record;
      resolve(@{ @"intentRevision": @(intentRevision) });
      return;
    }
    record[@"subscriptionsStatus"] = @"pending";
    record[@"pendingSubscriptionOperations"] = @([record[@"pendingSubscriptionOperations"] unsignedIntegerValue] + 1);
    NSMutableDictionary *operationWaiter = [self operationWaiterWithReject:reject operation:@"unsubscribe" topic:filter generation:record[@"generation"]];
    NSMutableArray *operationWaiters = [record[@"operationWaiters"] mutableCopy] ?: [NSMutableArray array];
    [operationWaiters addObject:operationWaiter];
    record[@"operationWaiters"] = operationWaiters;
    self->_sessions[instanceId] = record;
    __block BOOL settled = NO;
    __block dispatch_block_t finishOperation;
    [self enqueueSubscriptionOperation:^(dispatch_block_t finish) {
      finishOperation = [finish copy];
      [bridge unsubscribeFilter:filter completion:^(NSError *error) {
      dispatch_async(self->_sessionQueue, ^{
        if (settled || [operationWaiter[@"settled"] boolValue]) { finish(); return; }
        settled = YES;
        operationWaiter[@"settled"] = @YES;
        NSMutableDictionary *current = [self->_sessions[instanceId] mutableCopy];
        NSMutableArray *remainingOperations = [current[@"operationWaiters"] mutableCopy] ?: [NSMutableArray array];
        [remainingOperations removeObject:operationWaiter];
        current[@"operationWaiters"] = remainingOperations;
        if (error != nil) {
          if (previous != nil) current[@"desiredSubscriptions"][filter] = previous;
          NSError *annotatedError = RNMqttAnnotatedError(error, @"unsubscribe", current[@"generation"], @"rejected");
          current[@"pendingSubscriptionOperations"] = @([current[@"pendingSubscriptionOperations"] unsignedIntegerValue] > 0 ? [current[@"pendingSubscriptionOperations"] unsignedIntegerValue] - 1 : 0);
          current[@"subscriptionsStatus"] = @"failed";
          self->_sessions[instanceId] = current;
          [self settleSubscriptionWaitersForInstance:instanceId];
          reject(@"ERR_UNSUBSCRIBE_FAILED", annotatedError.localizedDescription, annotatedError);
        } else {
          current[@"pendingSubscriptionOperations"] = @([current[@"pendingSubscriptionOperations"] unsignedIntegerValue] > 0 ? [current[@"pendingSubscriptionOperations"] unsignedIntegerValue] - 1 : 0);
          current[@"subscriptionsStatus"] = [current[@"pendingSubscriptionOperations"] unsignedIntegerValue] == 0 ? @"ready" : @"pending";
          NSMutableDictionary *confirmed = [current[@"confirmedSubscriptions"] mutableCopy] ?: [NSMutableDictionary dictionary];
          [confirmed removeObjectForKey:filter];
          current[@"confirmedSubscriptions"] = confirmed;
          NSMutableDictionary *currentTombstones = [current[@"tombstones"] mutableCopy] ?: [NSMutableDictionary dictionary];
          [currentTombstones removeObjectForKey:filter];
          current[@"tombstones"] = currentTombstones;
          current[@"revision"] = @([current[@"revision"] integerValue] + 1);
          self->_sessions[instanceId] = current;
          [self settleSubscriptionWaitersForInstance:instanceId];
          resolve(@{ @"intentRevision": @(intentRevision) });
        }
        finish();
      });
      }];
    } instanceId:instanceId filter:filter];
    NSUInteger timeoutMs = [record[@"options"][@"operationTimeoutMs"] unsignedIntegerValue] ?: 30000;
    dispatch_after(dispatch_time(DISPATCH_TIME_NOW, (int64_t)timeoutMs * NSEC_PER_MSEC), self->_sessionQueue, ^{
      if (settled) return;
      settled = YES;
      operationWaiter[@"settled"] = @YES;
      NSMutableDictionary *current = [self->_sessions[instanceId] mutableCopy];
      if (current != nil) {
        NSMutableArray *remainingOperations = [current[@"operationWaiters"] mutableCopy] ?: [NSMutableArray array];
        [remainingOperations removeObject:operationWaiter];
        current[@"operationWaiters"] = remainingOperations;
        if (previous != nil) {
          NSMutableDictionary *desiredAfterTimeout = [current[@"desiredSubscriptions"] mutableCopy] ?: [NSMutableDictionary dictionary];
          desiredAfterTimeout[filter] = previous;
          current[@"desiredSubscriptions"] = desiredAfterTimeout;
        }
        current[@"pendingSubscriptionOperations"] = @([current[@"pendingSubscriptionOperations"] unsignedIntegerValue] > 0 ? [current[@"pendingSubscriptionOperations"] unsignedIntegerValue] - 1 : 0);
        current[@"subscriptionsStatus"] = @"failed";
        self->_sessions[instanceId] = current;
        [self settleSubscriptionWaitersForInstance:instanceId];
      }
      reject(@"ERR_OPERATION_TIMEOUT", @"Unsubscription operation timed out", nil);
      if (finishOperation != nil) finishOperation();
    });
  });
}
- (void)awaitSubscriptions:(NSString *)instanceId generation:(double)generation desiredRevision:(double)desiredRevision resolve:(RCTPromiseResolveBlock)resolve reject:(RCTPromiseRejectBlock)reject { dispatch_async(_sessionQueue, ^{ NSMutableDictionary *record = [self->_sessions[instanceId] mutableCopy]; if (record == nil || ![record[@"state"] isEqual:@"connected"]) { reject(@"ERR_NOT_CONNECTED", @"Session is not connected", nil); return; } if ([record[@"generation"] doubleValue] != generation) { reject(@"ERR_CONNECTION_LOST", @"Connection generation changed", nil); return; } if ([record[@"subscriptionsStatus"] isEqual:@"failed"]) { reject(@"ERR_SUBSCRIBE_FAILED", @"Subscription synchronization failed", nil); return; } if ([record[@"subscriptionsRevision"] doubleValue] >= desiredRevision && [record[@"subscriptionsStatus"] isEqual:@"ready"]) { resolve([self snapshotForInstance:instanceId state:record[@"state"] revision:record[@"revision"] generation:record[@"generation"] lastConnAck:record[@"lastConnAck"] deliveryStatus:@"active"]); return; } NSMutableArray *waiters = [record[@"subscriptionWaiters"] mutableCopy] ?: [NSMutableArray array]; NSDictionary *waiter = @{ @"generation": @(generation), @"desiredRevision": @(desiredRevision), @"resolve": [resolve copy], @"reject": [reject copy] }; [waiters addObject:waiter]; record[@"subscriptionWaiters"] = waiters; self->_sessions[instanceId] = record; NSUInteger timeoutMs = [record[@"options"][@"operationTimeoutMs"] unsignedIntegerValue] ?: 30000; dispatch_after(dispatch_time(DISPATCH_TIME_NOW, (int64_t)timeoutMs * NSEC_PER_MSEC), self->_sessionQueue, ^{ NSMutableDictionary *current = [self->_sessions[instanceId] mutableCopy]; NSMutableArray *currentWaiters = [current[@"subscriptionWaiters"] mutableCopy]; if (current == nil || currentWaiters == nil || ![currentWaiters containsObject:waiter]) return; [currentWaiters removeObject:waiter]; current[@"subscriptionWaiters"] = currentWaiters; self->_sessions[instanceId] = current; reject(@"ERR_OPERATION_TIMEOUT", @"Subscription synchronization is not ready", nil); }); }); }
- (void)publish:(NSString *)instanceId request:(JS::NativeReactNativeMqtt::WirePublishRequest &)request resolve:(RCTPromiseResolveBlock)resolve reject:(RCTPromiseRejectBlock)reject {
  NSString *topic = [request.topic() copy];
  NSString *responseTopic = [request.responseTopic() copy];
  NSString *payloadBase64 = [request.payloadBase64() copy];
  NSString *contentType = [request.contentType() copy];
  NSString *correlationDataBase64 = [request.correlationDataBase64() copy];
  NSNumber *messageExpiry = request.messageExpiryIntervalSeconds().has_value() ? @(*request.messageExpiryIntervalSeconds()) : nil;
  NSNumber *payloadFormatIndicator = request.payloadFormatIndicator().has_value() ? @(*request.payloadFormatIndicator()) : nil;
  NSNumber *qos = @(request.qos().value_or(1));
  BOOL retained = request.retained().value_or(false);
  NSArray<NSDictionary *> *userProperties = nil;
  if (auto values = request.userProperties(); values.has_value()) {
    NSMutableArray<NSDictionary *> *pairs = [NSMutableArray arrayWithCapacity:values->size()];
    for (const auto &property : *values) [pairs addObject:@{ @"key": property.key(), @"value": property.value() }];
    userProperties = pairs;
  }
  if (!RNMqttValidTopicName(topic)) { reject(@"ERR_INVALID_OPTIONS", @"topic name is invalid", nil); return; }
  if (responseTopic != nil && !RNMqttValidTopicName(responseTopic)) { reject(@"ERR_INVALID_OPTIONS", @"responseTopic is invalid", nil); return; }
  dispatch_async(_sessionQueue, ^{
    NSMutableDictionary *record = [self->_sessions[instanceId] mutableCopy];
    ReactNativeMqttCocoaBridge *bridge = record[@"bridge"];
    if (record == nil || bridge == nil || ![record[@"state"] isEqual:@"connected"]) { reject(@"ERR_NOT_CONNECTED", @"Session is not connected", nil); return; }
    NSData *payload = [[NSData alloc] initWithBase64EncodedString:payloadBase64 options:0];
    if (payload == nil) { reject(@"ERR_INVALID_OPTIONS", @"payloadBase64 is invalid", nil); return; }
    NSMutableDictionary *properties = [NSMutableDictionary dictionary];
    if (contentType != nil) properties[@"contentType"] = contentType;
    if (responseTopic != nil) properties[@"responseTopic"] = responseTopic;
    if (correlationDataBase64 != nil) {
      NSData *correlation = [[NSData alloc] initWithBase64EncodedString:correlationDataBase64 options:0];
      if (correlation == nil) { reject(@"ERR_INVALID_PAYLOAD", @"correlationDataBase64 is invalid", nil); return; }
      properties[@"correlationData"] = correlation;
    }
    if (messageExpiry != nil) properties[@"messageExpiryIntervalSeconds"] = messageExpiry;
    if (payloadFormatIndicator != nil) properties[@"payloadFormatIndicator"] = payloadFormatIndicator;
    if (userProperties != nil) properties[@"userProperties"] = userProperties;
    NSMutableDictionary *operationWaiter = [self operationWaiterWithReject:reject operation:@"publish" topic:topic generation:record[@"generation"]];
    NSMutableArray *operationWaiters = [record[@"operationWaiters"] mutableCopy] ?: [NSMutableArray array];
    [operationWaiters addObject:operationWaiter];
    record[@"operationWaiters"] = operationWaiters;
    self->_sessions[instanceId] = record;
    __block BOOL settled = NO;
    [bridge publishTopic:topic payload:payload qos:qos retained:retained properties:properties completion:^(NSError *error) {
      dispatch_async(self->_sessionQueue, ^{
        if (settled || [operationWaiter[@"settled"] boolValue]) return;
        settled = YES;
        operationWaiter[@"settled"] = @YES;
        NSMutableDictionary *current = [self->_sessions[instanceId] mutableCopy];
        NSMutableArray *remainingOperations = [current[@"operationWaiters"] mutableCopy] ?: [NSMutableArray array];
        [remainingOperations removeObject:operationWaiter];
        current[@"operationWaiters"] = remainingOperations;
        self->_sessions[instanceId] = current;
        if (error != nil) {
          NSError *annotatedError = RNMqttAnnotatedError(error, @"publish", current[@"generation"], @"unknown");
          reject(@"ERR_PUBLISH_FAILED", annotatedError.localizedDescription, annotatedError);
        } else {
          resolve(nil);
        }
      });
    }];
    NSUInteger timeoutMs = [record[@"options"][@"operationTimeoutMs"] unsignedIntegerValue] ?: 30000;
    dispatch_after(dispatch_time(DISPATCH_TIME_NOW, (int64_t)timeoutMs * NSEC_PER_MSEC), self->_sessionQueue, ^{
      if (settled) return;
      settled = YES;
      operationWaiter[@"settled"] = @YES;
      NSMutableDictionary *current = [self->_sessions[instanceId] mutableCopy];
      if (current != nil) {
        NSMutableArray *remainingOperations = [current[@"operationWaiters"] mutableCopy] ?: [NSMutableArray array];
        [remainingOperations removeObject:operationWaiter];
        current[@"operationWaiters"] = remainingOperations;
        self->_sessions[instanceId] = current;
      }
      NSError *timeoutError = [NSError errorWithDomain:@"ReactNativeMqtt" code:408 userInfo:@{ NSLocalizedDescriptionKey: @"Publish operation timed out", @"operation": @"publish", @"outcome": @"unknown" }];
      reject(@"ERR_OPERATION_TIMEOUT", timeoutError.localizedDescription, timeoutError);
    });
  });
}
- (void)ackMessageBatch:(NSString *)instanceId streamEpoch:(double)streamEpoch batchId:(double)batchId resolve:(RCTPromiseResolveBlock)resolve reject:(RCTPromiseRejectBlock)reject { if (!std::isfinite(streamEpoch) || !std::isfinite(batchId) || streamEpoch < 0 || batchId < 0 || std::floor(streamEpoch) != streamEpoch || std::floor(batchId) != batchId) { reject(@"ERR_INVALID_OPTIONS", @"ACK token must contain non-negative integers", nil); return; } dispatch_async(_sessionQueue, ^{ NSDictionary *token = self->_iosBatchLedger[instanceId]; if (token != nil && [token[@"streamEpoch"] doubleValue] == streamEpoch && [token[@"batchId"] doubleValue] == batchId) { [self->_iosBatchLedger removeObjectForKey:instanceId]; NSMutableDictionary *record = [self->_sessions[instanceId] mutableCopy]; if (record != nil) { record[@"overflowDiagnosticPending"] = @NO; self->_sessions[instanceId] = record; } [self emitNextMessageBatchForInstance:instanceId]; } resolve(nil); }); }

- (void)invalidate {
  dispatch_sync(_sessionQueue, ^{
    self->_invalidated = YES;
    for (NSString *instanceId in [self->_sessions allKeys]) {
      NSDictionary *record = self->_sessions[instanceId];
      ReactNativeMqttCocoaBridge *bridge = record[@"bridge"];
      for (NSDictionary *waiter in [record[@"connectWaiters"] copy] ?: @[]) {
        ((RCTPromiseRejectBlock)waiter[@"reject"])(@"ERR_DISPOSED", @"Native module was invalidated", nil);
      }
      for (NSDictionary *waiter in [record[@"subscriptionWaiters"] copy] ?: @[]) {
        ((RCTPromiseRejectBlock)waiter[@"reject"])(@"ERR_DISPOSED", @"Native module was invalidated", nil);
      }
      for (NSMutableDictionary *waiter in [record[@"operationWaiters"] copy] ?: @[]) {
        if (![waiter[@"settled"] boolValue]) {
          waiter[@"settled"] = @YES;
          ((RCTPromiseRejectBlock)waiter[@"reject"])(@"ERR_DISPOSED", @"Native module was invalidated", nil);
        }
      }
      [bridge disconnect];
    }
    [self->_sessions removeAllObjects];
    [self->_iosBatchLedger removeAllObjects];
    [self->_iosStreamEpochs removeAllObjects];
    [self->_iosSubscriptionQueues removeAllObjects];
  });
}

- (std::shared_ptr<facebook::react::TurboModule>)getTurboModule:
    (const facebook::react::ObjCTurboModule::InitParams &)params
{
    return std::make_shared<facebook::react::NativeReactNativeMqttSpecJSI>(params);
}

+ (NSString *)moduleName
{
  return @"ReactNativeMqtt";
}

@end
