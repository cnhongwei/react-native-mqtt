import Foundation

enum ReactNativeMqttSessionState: String {
  case idle
  case connecting
  case connected
  case reconnecting
  case disconnecting
  case disconnected
  case closed
}

struct ReactNativeMqttSessionSnapshot {
  let state: ReactNativeMqttSessionState
  let revision: UInt64
  let generation: UInt64
}

protocol ReactNativeMqttSessionTransport: AnyObject {
  func connect(completion: @escaping (Result<Any, Error>) -> Void)
  func disconnect(completion: @escaping (Error?) -> Void)
  func setDisconnectedHandler(_ handler: @escaping (Error?) -> Void)
}

/// Owns one transport lifecycle. All mutable state is accessed on `queue`.
/// Transport callbacks may arrive on arbitrary SDK queues and are immediately
/// re-enqueued, so stale generations cannot mutate a replacement connection.
final class ReactNativeMqttSession {
  typealias SnapshotHandler = (ReactNativeMqttSessionSnapshot) -> Void
  typealias ConnectHandler = (ReactNativeMqttSessionSnapshot, Any) -> Void

  private let queue: DispatchQueue
  private let transport: ReactNativeMqttSessionTransport
  private let reconnectEnabled: Bool
  private let reconnectInitialDelayMs: UInt64
  private let reconnectMaxDelayMs: UInt64
  private let reconnectJitterRatio: Double
  private let connectTimeoutMs: UInt64
  private let disconnectTimeoutMs: UInt64
  private(set) var snapshot: ReactNativeMqttSessionSnapshot
  private var connectWaiters: [(Result<Any, Error>) -> Void] = []
  private var disconnectWaiters: [(Error?) -> Void] = []
  private var reconnectWorkItem: DispatchWorkItem?
  private var disconnectWorkItem: DispatchWorkItem?
  private var reconnectAttempts: UInt64 = 0
  private var lastConnectValue: Any?
  private var userStopRequested = false
  private var disposing = false
  private var disposed = false
  private let disconnectHandler: ((Error?) -> Void)?
  private var snapshotHandler: SnapshotHandler?
  private var connectHandler: ConnectHandler?

  init(
    transport: ReactNativeMqttSessionTransport,
    queueLabel: String,
    reconnectEnabled: Bool = true,
    reconnectInitialDelayMs: UInt64 = 1_000,
    reconnectMaxDelayMs: UInt64 = 30_000,
    reconnectJitterRatio: Double = 0.2,
    connectTimeoutMs: UInt64 = 30_000,
    disconnectTimeoutMs: UInt64 = 5_000,
    disconnectHandler: ((Error?) -> Void)? = nil,
    snapshotHandler: SnapshotHandler? = nil
  ) {
    precondition(reconnectInitialDelayMs > 0 && reconnectMaxDelayMs >= reconnectInitialDelayMs)
    precondition((0...1).contains(reconnectJitterRatio))
    precondition(connectTimeoutMs > 0 && disconnectTimeoutMs > 0)
    self.queue = DispatchQueue(label: queueLabel)
    self.transport = transport
    self.reconnectEnabled = reconnectEnabled
    self.reconnectInitialDelayMs = reconnectInitialDelayMs
    self.reconnectMaxDelayMs = reconnectMaxDelayMs
    self.reconnectJitterRatio = reconnectJitterRatio
    self.connectTimeoutMs = connectTimeoutMs
    self.disconnectTimeoutMs = disconnectTimeoutMs
    self.disconnectHandler = disconnectHandler
    self.snapshot = ReactNativeMqttSessionSnapshot(state: .idle, revision: 0, generation: 0)
    self.snapshotHandler = snapshotHandler
    transport.setDisconnectedHandler { [weak self] error in
      self?.queue.async { self?.transportDisconnected(error) }
    }
  }

  func connect(completion: @escaping (Result<Any, Error>) -> Void) {
    queue.async { [weak self] in self?.connectOnQueue(completion) }
  }

  func disconnect(completion: @escaping (Error?) -> Void) {
    queue.async { [weak self] in self?.disconnectOnQueue(completion) }
  }

  func dispose(completion: @escaping (Error?) -> Void) {
    queue.async { [weak self] in self?.disposeOnQueue(completion) }
  }

  func setSnapshotHandler(_ handler: SnapshotHandler?) {
    queue.async { [weak self] in self?.snapshotHandler = handler }
  }

  func setConnectHandler(_ handler: ConnectHandler?) {
    queue.async { [weak self] in self?.connectHandler = handler }
  }

  private func connectOnQueue(_ completion: @escaping (Result<Any, Error>) -> Void) {
    guard !disposing && !disposed else {
      completion(.failure(ReactNativeMqttSessionError.disposed))
      return
    }
    userStopRequested = false
    if snapshot.state == .connected {
      if let lastConnectValue {
        completion(.success(lastConnectValue))
      } else {
        completion(.failure(ReactNativeMqttSessionError.connectTimedOut))
      }
      return
    }
    connectWaiters.append(completion)
    guard snapshot.state != .connecting && snapshot.state != .reconnecting else { return }
    reconnectWorkItem?.cancel()
    reconnectWorkItem = nil
    beginConnectOnQueue()
  }

  private func beginConnectOnQueue() {
    guard !disposing && !disposed else { return }
    let generation = snapshot.generation &+ 1
    setStateOnQueue(.connecting, generation: generation)
    var completed = false
    let finish: (Result<Any, Error>) -> Void = { [weak self] result in
      self?.queue.async {
        guard let self, !completed else { return }
        completed = true
        self.finishConnectOnQueue(result, generation: generation)
      }
    }
    transport.connect(completion: finish)
    let timeout = DispatchWorkItem { [weak self] in
      guard let self else { return }
      self.queue.async {
        guard !completed, self.snapshot.generation == generation, self.snapshot.state == .connecting else { return }
        completed = true
        self.transport.disconnect { _ in }
        self.finishConnectOnQueue(.failure(ReactNativeMqttSessionError.connectTimedOut), generation: generation)
      }
    }
    queue.asyncAfter(deadline: .now() + .milliseconds(Int(connectTimeoutMs)), execute: timeout)
  }

  private func finishConnectOnQueue(_ result: Result<Any, Error>, generation: UInt64) {
    guard !disposing && !disposed && snapshot.generation == generation else { return }
    switch result {
    case .success(let value):
      lastConnectValue = value
      reconnectAttempts = 0
      setStateOnQueue(.connected, generation: generation)
      connectHandler?(snapshot, value)
      let waiters = connectWaiters
      connectWaiters.removeAll()
      waiters.forEach { $0(result) }
    case .failure(let error):
      setStateOnQueue(.disconnected, generation: generation)
      let waiters = connectWaiters
      connectWaiters.removeAll()
      waiters.forEach { $0(.failure(error)) }
    }
  }

  private func transportDisconnected(_ error: Error?) {
    guard !disposing && !disposed else { return }
    guard snapshot.state == .connected else { return }
    setStateOnQueue(.reconnecting, generation: snapshot.generation)
    if reconnectEnabled && !userStopRequested {
      scheduleReconnectOnQueue()
    } else {
      setStateOnQueue(.disconnected, generation: snapshot.generation)
    }
    disconnectHandler?(error)
  }

  private func scheduleReconnectOnQueue() {
    reconnectAttempts &+= 1
    let exponent = min(reconnectAttempts - 1, 31)
    let base = min(reconnectMaxDelayMs, reconnectInitialDelayMs << exponent)
    let jitter = Double(base) * reconnectJitterRatio
    let random = Double.random(in: -jitter...jitter)
    let delay = UInt64(max(0, Double(base) + random))
    let work = DispatchWorkItem { [weak self] in
      guard let self else { return }
      self.queue.async {
        guard !self.userStopRequested, !self.disposing, !self.disposed, self.snapshot.state == .reconnecting else { return }
        self.beginConnectOnQueue()
      }
    }
    reconnectWorkItem = work
    queue.asyncAfter(deadline: .now() + .milliseconds(Int(delay)), execute: work)
  }

  private func disconnectOnQueue(_ completion: @escaping (Error?) -> Void) {
    userStopRequested = true
    reconnectWorkItem?.cancel()
    reconnectWorkItem = nil
    connectWaiters.forEach { $0(.failure(ReactNativeMqttSessionError.cancelled)) }
    connectWaiters.removeAll()
    guard snapshot.state != .connected && snapshot.state != .connecting && snapshot.state != .reconnecting else {
      disconnectWaiters.append(completion)
      if snapshot.state != .disconnecting { beginDisconnectOnQueue() }
      return
    }
    setStateOnQueue(.disconnected, generation: snapshot.generation)
    completion(nil)
  }

  private func beginDisconnectOnQueue() {
    let generation = snapshot.generation
    setStateOnQueue(.disconnecting, generation: generation)
    var finished = false
    let finish: (Error?) -> Void = { [weak self] error in
      self?.queue.async {
        guard let self, !finished, self.snapshot.generation == generation else { return }
        finished = true
        self.disconnectWorkItem?.cancel()
        self.disconnectWorkItem = nil
        self.setStateOnQueue(.disconnected, generation: generation)
        let waiters = self.disconnectWaiters
        self.disconnectWaiters.removeAll()
        waiters.forEach { $0(error) }
      }
    }
    transport.disconnect(completion: finish)
    let timeout = DispatchWorkItem { [weak self] in
      guard let self else { return }
      self.queue.async {
        guard !finished, self.snapshot.generation == generation else { return }
        finished = true
        self.transport.disconnect { _ in }
        self.setStateOnQueue(.disconnected, generation: generation)
        let waiters = self.disconnectWaiters
        self.disconnectWaiters.removeAll()
        waiters.forEach { $0(ReactNativeMqttSessionError.disconnectTimedOut) }
      }
    }
    disconnectWorkItem = timeout
    queue.asyncAfter(deadline: .now() + .milliseconds(Int(disconnectTimeoutMs)), execute: timeout)
  }

  private func disposeOnQueue(_ completion: @escaping (Error?) -> Void) {
    if disposed {
      completion(nil)
      return
    }
    disposing = true
    userStopRequested = true
    reconnectWorkItem?.cancel()
    reconnectWorkItem = nil
    connectWaiters.forEach { $0(.failure(ReactNativeMqttSessionError.disposed)) }
    connectWaiters.removeAll()
    if snapshot.state == .connected || snapshot.state == .connecting || snapshot.state == .reconnecting {
      disconnectWaiters.append { [weak self] error in self?.closeOnQueue(error, completion: completion) }
      if snapshot.state != .disconnecting { beginDisconnectOnQueue() }
    } else {
      closeOnQueue(nil, completion: completion)
    }
  }

  private func closeOnQueue(_ error: Error?, completion: @escaping (Error?) -> Void) {
    guard !disposed else { completion(nil); return }
    disposed = true
    setStateOnQueue(.closed, generation: snapshot.generation)
    completion(error)
  }

  private func setStateOnQueue(_ state: ReactNativeMqttSessionState, generation: UInt64) {
    snapshot = ReactNativeMqttSessionSnapshot(state: state, revision: snapshot.revision &+ 1, generation: generation)
    snapshotHandler?(snapshot)
  }
}

enum ReactNativeMqttSessionError: Error, Equatable {
  case disposed
  case cancelled
  case connectTimedOut
  case operationTimedOut
  case disconnectTimedOut
}

struct ReactNativeMqttOperationToken: Equatable {
  let operationId: UInt64
  let generation: UInt64
  let topic: String?
}

/// Serial operation ledger for SDK callbacks that do not carry an operation
/// identity. Requests sharing a topic are serialized; different topics may
/// proceed independently. Every request has one settlement gate and one
/// monotonic deadline, so late ACKs and timeout races are harmless.
final class ReactNativeMqttOperationLedger {
  typealias ResultValue = Result<Any, Error>
  typealias Completion = (ResultValue) -> Void
  typealias Executor = (@escaping Completion) -> Void

  private struct Request {
    let token: ReactNativeMqttOperationToken
    let deadline: DispatchTime
    let execute: Executor
    let completion: Completion
  }

  private let queue: DispatchQueue
  private var nextOperationId: UInt64 = 0
  private var queues: [String: [Request]] = [:]
  private var active: [String: ReactNativeMqttOperationToken] = [:]
  private var settled: Set<UInt64> = []

  init(queueLabel: String) {
    queue = DispatchQueue(label: queueLabel)
  }

  func submit(
    topic: String?,
    generation: UInt64,
    timeoutMs: UInt64,
    execute: @escaping Executor,
    completion: @escaping Completion
  ) {
    let key = topic ?? "__untagged_\(UUID().uuidString)"
    queue.async {
      self.nextOperationId &+= 1
      let token = ReactNativeMqttOperationToken(operationId: self.nextOperationId, generation: generation, topic: topic)
      let request = Request(token: token, deadline: .now() + .milliseconds(Int(timeoutMs)), execute: execute, completion: completion)
      self.queues[key, default: []].append(request)
      self.startNextIfNeeded(key)
    }
  }

  private func startNextIfNeeded(_ key: String) {
    guard active[key] == nil, let request = queues[key]?.first else { return }
    active[key] = request.token
    var finished = false
    let finish: Completion = { [weak self] result in
      self?.queue.async {
        guard let self, !finished else { return }
        finished = true
        self.settle(request, key: key, result: result)
      }
    }
    request.execute(finish)
    let timeout = DispatchWorkItem { [weak self] in
      guard let self else { return }
      self.queue.async {
        guard !finished else { return }
        finished = true
        self.settle(request, key: key, result: .failure(ReactNativeMqttSessionError.operationTimedOut))
      }
    }
    queue.asyncAfter(deadline: request.deadline, execute: timeout)
  }

  private func settle(_ request: Request, key: String, result: ResultValue) {
    guard settled.insert(request.token.operationId).inserted else { return }
    request.completion(result)
    if queues[key]?.isEmpty == false { queues[key]?.removeFirst() }
    active.removeValue(forKey: key)
    if queues[key]?.isEmpty == true { queues.removeValue(forKey: key) }
    else { startNextIfNeeded(key) }
  }
}

/// Serial ownership for all native sessions. `invalidate` is a terminal epoch
/// barrier: after it starts no later open can create a session.
final class ReactNativeMqttSessionRegistry {
  private let queue = DispatchQueue(label: "com.cnhongwei.reactnativemqtt.session-registry")
  private var sessions: [String: ReactNativeMqttSession] = [:]
  private(set) var epoch: UInt64 = 0
  private var invalidated = false

  func open(_ instanceId: String, make: @escaping () -> ReactNativeMqttSession, completion: @escaping (Result<ReactNativeMqttSession, Error>) -> Void) {
    queue.async {
      guard !self.invalidated else {
        completion(.failure(ReactNativeMqttSessionError.disposed))
        return
      }
      if let existing = self.sessions[instanceId] {
        completion(.success(existing))
        return
      }
      let session = make()
      self.sessions[instanceId] = session
      completion(.success(session))
    }
  }

  func remove(_ instanceId: String, completion: @escaping () -> Void) {
    queue.async {
      guard let session = self.sessions.removeValue(forKey: instanceId) else { completion(); return }
      session.dispose { _ in completion() }
    }
  }

  func invalidate(completion: @escaping () -> Void) {
    queue.async {
      self.invalidated = true
      self.epoch &+= 1
      let sessions = Array(self.sessions.values)
      self.sessions.removeAll()
      guard !sessions.isEmpty else { completion(); return }
      func disposeNext(_ index: Int) {
        guard index < sessions.count else {
          DispatchQueue.global(qos: .userInitiated).async(execute: completion)
          return
        }
        sessions[index].dispose { _ in
          self.queue.async { disposeNext(index + 1) }
        }
      }
      disposeNext(0)
    }
  }
}
