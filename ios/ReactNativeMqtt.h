#import <ReactNativeMqttSpec/ReactNativeMqttSpec.h>

@interface ReactNativeMqtt : NativeReactNativeMqttSpecBase <NativeReactNativeMqttSpec>
{
  dispatch_queue_t _sessionQueue;
  NSMutableDictionary<NSString *, NSDictionary *> *_sessions;
  NSMutableDictionary<NSString *, NSDictionary *> *_iosBatchLedger;
  NSMutableDictionary<NSString *, NSNumber *> *_iosStreamEpochs;
  NSMutableDictionary<NSString *, NSMutableDictionary<NSString *, NSMutableArray *> *> *_iosSubscriptionQueues;
  NSUInteger _iosBatchId;
  NSUInteger _nextOperationId;
  BOOL _invalidated;
}

@end
