import Foundation

private final class SmokeTransport: ReactNativeMqttSessionTransport {
  var connectAttempts = 0
  var pendingConnect: [(Result<Any, Error>) -> Void] = []
  var pendingDisconnect: [(Error?) -> Void] = []
  var disconnected: ((Error?) -> Void)?

  func connect(completion: @escaping (Result<Any, Error>) -> Void) {
    connectAttempts += 1
    pendingConnect.append(completion)
  }

  func disconnect(completion: @escaping (Error?) -> Void) {
    pendingDisconnect.append(completion)
  }

  func setDisconnectedHandler(_ handler: @escaping (Error?) -> Void) {
    disconnected = handler
  }

  func succeedConnect(_ value: Any = "ack") {
    precondition(!pendingConnect.isEmpty)
    pendingConnect.removeFirst()(.success(value))
  }

  func failConnect(_ error: Error) {
    precondition(!pendingConnect.isEmpty)
    pendingConnect.removeFirst()(.failure(error))
  }

  func completeDisconnect(_ error: Error? = nil) {
    precondition(!pendingDisconnect.isEmpty)
    pendingDisconnect.removeFirst()(error)
  }

  func emitDisconnect(_ error: Error? = nil) {
    disconnected?(error)
  }
}

private func wait(_ semaphore: DispatchSemaphore, _ message: String) {
  precondition(semaphore.wait(timeout: .now() + 2) == .success, message)
}

private func runSmoke() {
  let transport = SmokeTransport()
  var snapshots: [ReactNativeMqttSessionSnapshot] = []
  let session = ReactNativeMqttSession(
    transport: transport,
    queueLabel: "com.cnhongwei.reactnativemqtt.swift-session-smoke",
    reconnectInitialDelayMs: 1,
    reconnectMaxDelayMs: 4,
    reconnectJitterRatio: 0,
    connectTimeoutMs: 100,
    disconnectTimeoutMs: 100,
    snapshotHandler: { snapshots.append($0) }
  )

  let connected = DispatchSemaphore(value: 0)
  var connectResults = 0
  session.connect { result in
    precondition(result.isSuccess)
    connectResults += 1
    connected.signal()
  }
  session.connect { result in
    precondition(result.isSuccess)
    connectResults += 1
    connected.signal()
  }
  usleep(20_000)
  precondition(transport.connectAttempts == 1, "connect calls were not deduplicated")
  transport.succeedConnect()
  wait(connected, "deduplicated connect did not settle first waiter")
  wait(connected, "deduplicated connect did not settle second waiter")
  precondition(connectResults == 2)

  let cachedConnect = DispatchSemaphore(value: 0)
  session.connect { result in
    if case .success(let value) = result {
      precondition((value as? String) == "ack", "connected session did not return cached CONNACK")
    } else {
      preconditionFailure("connected session did not return cached CONNACK")
    }
    cachedConnect.signal()
  }
  wait(cachedConnect, "cached connect did not settle")
  precondition(transport.connectAttempts == 1, "cached connect started another transport attempt")

  transport.emitDisconnect()
  usleep(40_000)
  precondition(transport.connectAttempts == 2, "disconnect did not schedule one reconnect")
  let reconnected = DispatchSemaphore(value: 0)
  session.connect { result in
    precondition(result.isSuccess)
    reconnected.signal()
  }
  transport.succeedConnect()
  wait(reconnected, "reconnect waiter did not settle")
  let states = snapshots.map(\.state)
  precondition(states.contains(.reconnecting) && states.last == .connected, "session state sequence lost reconnect states")
  precondition(snapshots.map(\.revision) == Array(1...snapshots.count).map(UInt64.init), "session revisions are not monotonic")
  precondition(snapshots[snapshots.count - 1].generation == 2, "reconnect did not advance generation")

  let failedTransport = SmokeTransport()
  let failedSession = ReactNativeMqttSession(
    transport: failedTransport,
    queueLabel: "com.cnhongwei.reactnativemqtt.swift-session-failure",
    reconnectInitialDelayMs: 1,
    reconnectMaxDelayMs: 2,
    reconnectJitterRatio: 0,
    connectTimeoutMs: 100
  )
  let failed = DispatchSemaphore(value: 0)
  failedSession.connect { result in
    precondition(result.isFailure)
    failed.signal()
  }
  usleep(20_000)
  failedTransport.failConnect(NSError(domain: "smoke", code: 1))
  wait(failed, "initial connect failure did not settle")
  usleep(20_000)
  precondition(failedTransport.connectAttempts == 1, "initial failure unexpectedly retried")

  let disposeTransport = SmokeTransport()
  let disposeSession = ReactNativeMqttSession(
    transport: disposeTransport,
    queueLabel: "com.cnhongwei.reactnativemqtt.swift-session-dispose",
    connectTimeoutMs: 100,
    disconnectTimeoutMs: 100
  )
  let disposed = DispatchSemaphore(value: 0)
  var disposeConnectResults = 0
  disposeSession.connect { result in
    precondition(result.isFailure)
    disposeConnectResults += 1
  }
  usleep(20_000)
  disposeSession.dispose { error in
    precondition(error == nil)
    disposed.signal()
  }
  usleep(20_000)
  precondition(disposeTransport.pendingDisconnect.count == 1, "dispose did not close pending transport")
  disposeTransport.completeDisconnect()
  wait(disposed, "dispose barrier did not settle")
  disposeTransport.succeedConnect("late-success")
  usleep(20_000)
  precondition(disposeConnectResults == 1, "late connect ACK resurrected a disposed session")
  precondition(disposeSession.snapshot.state == .closed, "disposed session was resurrected by a late ACK")

  let registry = ReactNativeMqttSessionRegistry()
  let opened = DispatchSemaphore(value: 0)
  registry.open("one", make: {
    ReactNativeMqttSession(transport: SmokeTransport(), queueLabel: "com.cnhongwei.reactnativemqtt.registry")
  }) { result in
    precondition(result.isSuccess)
    opened.signal()
  }
  wait(opened, "registry open did not settle")
  let invalidated = DispatchSemaphore(value: 0)
  registry.invalidate {
    invalidated.signal()
  }
  wait(invalidated, "registry invalidate did not settle")
  precondition(registry.epoch == 1, "registry invalidate did not advance epoch")
  let rejected = DispatchSemaphore(value: 0)
  registry.open("two", make: {
    fatalError("invalidated registry invoked factory")
  }) { result in
    precondition(result.isFailure)
    rejected.signal()
  }
  wait(rejected, "invalidated registry accepted a new session")

  let ledger = ReactNativeMqttOperationLedger(queueLabel: "com.cnhongwei.reactnativemqtt.ledger-smoke")
  let firstSettled = DispatchSemaphore(value: 0)
  let secondStarted = DispatchSemaphore(value: 0)
  var firstCompletions = 0
  var secondCompletions = 0
  var firstFinish: ((Result<Any, Error>) -> Void)?
  ledger.submit(topic: "same/topic", generation: 7, timeoutMs: 100, execute: { finish in
    firstFinish = finish
  }, completion: { result in
    precondition(result.isSuccess)
    firstCompletions += 1
    firstSettled.signal()
  })
  ledger.submit(topic: "same/topic", generation: 7, timeoutMs: 100, execute: { finish in
    secondStarted.signal()
    finish(.success(NSNull()))
  }, completion: { result in
    precondition(result.isSuccess)
    secondCompletions += 1
  })
  usleep(20_000)
  precondition(secondStarted.wait(timeout: .now()) == .timedOut, "same-topic operation was not serialized")
  firstFinish?(.success(NSNull()))
  firstFinish?(.success(NSNull()))
  wait(firstSettled, "first ledger operation did not settle")
  wait(secondStarted, "queued same-topic operation did not start")
  usleep(20_000)
  precondition(firstCompletions == 1 && secondCompletions == 1, "ledger settled an operation more than once")

  let timedOut = DispatchSemaphore(value: 0)
  ledger.submit(topic: "timeout/topic", generation: 9, timeoutMs: 1, execute: { _ in
    // Deliberately leave the operation pending until the deadline wins.
  }, completion: { result in
    precondition(result.isFailure)
    timedOut.signal()
  })
  wait(timedOut, "ledger deadline did not settle pending operation")
  print("Swift Session smoke passed: dedupe, reconnect, first-failure stop, dispose barrier, and registry invalidate.")
}

private extension Result {
  var isSuccess: Bool {
    if case .success = self { return true }
    return false
  }

  var isFailure: Bool { !isSuccess }
}

@main
private struct Main {
  static func main() {
    runSmoke()
  }
}
