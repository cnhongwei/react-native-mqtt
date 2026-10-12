import Foundation

/// A deterministic monotonic clock used only by the native test target.
final class NativeScenarioClock {
  private(set) var nowMilliseconds: Int = 0
  private var nextTimerID = 0
  private var timers: [Int: (at: Int, callback: () -> Void)] = [:]

  @discardableResult
  func schedule(after delayMilliseconds: Int, _ callback: @escaping () -> Void) -> Int {
    precondition(delayMilliseconds >= 0)
    nextTimerID += 1
    timers[nextTimerID] = (nowMilliseconds + delayMilliseconds, callback)
    return nextTimerID
  }

  func cancel(_ timerID: Int) {
    timers.removeValue(forKey: timerID)
  }

  func advance(by deltaMilliseconds: Int) {
    precondition(deltaMilliseconds >= 0)
    advance(to: nowMilliseconds + deltaMilliseconds)
  }

  func advance(to targetMilliseconds: Int) {
    precondition(targetMilliseconds >= nowMilliseconds)
    while let next = timers
      .sorted(by: { lhs, rhs in lhs.value.at == rhs.value.at ? lhs.key < rhs.key : lhs.value.at < rhs.value.at })
      .first,
      next.value.at <= targetMilliseconds {
      timers.removeValue(forKey: next.key)
      nowMilliseconds = next.value.at
      next.value.callback()
    }
    nowMilliseconds = targetMilliseconds
  }
}

/// A callback-controlled transport fixture. It deliberately has no CocoaMQTT dependency.
final class NativeScenarioTransport {
  struct Pending<T> {
    let complete: (T) -> Void
    let fail: (Error) -> Void
  }

  private(set) var connectAttempts = 0
  private(set) var publishAttempts: [(topic: String, payload: Data)] = []
  private(set) var disconnectAttempts = 0
  var pendingConnect: [Pending<String>] = []
  var pendingPublish: [Pending<Void>] = []
  var pendingDisconnect: [Pending<Void>] = []
  var onMessage: ((String, Data) -> Void)?
  var onDisconnect: ((String?) -> Void)?

  func connect(completion: @escaping (Result<String, Error>) -> Void) {
    connectAttempts += 1
    pendingConnect.append(Pending(
      complete: { completion(.success($0)) },
      fail: { completion(.failure($0)) }
    ))
  }

  func connectImmediately(failing error: Error, completion: @escaping (Result<String, Error>) -> Void) {
    connectAttempts += 1
    completion(.failure(error))
  }

  func publish(topic: String, payload: Data, completion: @escaping (Result<Void, Error>) -> Void) {
    publishAttempts.append((topic, payload))
    pendingPublish.append(Pending(
      complete: { completion(.success($0)) },
      fail: { completion(.failure($0)) }
    ))
  }

  func disconnect(completion: @escaping (Result<Void, Error>) -> Void) {
    disconnectAttempts += 1
    pendingDisconnect.append(Pending(
      complete: { completion(.success($0)) },
      fail: { completion(.failure($0)) }
    ))
  }

  func completeConnect(at index: Int, with value: String) {
    pendingConnect.remove(at: index).complete(value)
  }

  func repeatConnectCallback(at index: Int, with value: String) {
    pendingConnect[index].complete(value)
  }

  func failConnect(at index: Int, with error: Error) {
    pendingConnect.remove(at: index).fail(error)
  }

  func completePublish(at index: Int) {
    pendingPublish.remove(at: index).complete(())
  }

  func failPublish(at index: Int, with error: Error) {
    pendingPublish.remove(at: index).fail(error)
  }

  func publishImmediately(failing error: Error, topic: String, payload: Data, completion: @escaping (Result<Void, Error>) -> Void) {
    publishAttempts.append((topic, payload))
    completion(.failure(error))
  }

  func completeDisconnect(at index: Int) {
    pendingDisconnect.remove(at: index).complete(())
  }

  func emitMessage(_ topic: String, payload: Data) {
    onMessage?(topic, payload)
  }

  func emitMessageAfter(_ clock: NativeScenarioClock, delayMilliseconds: Int, topic: String, payload: Data) {
    clock.schedule(after: delayMilliseconds) { [weak self] in
      self?.emitMessage(topic, payload: payload)
    }
  }

  func emitDisconnectAfter(_ clock: NativeScenarioClock, delayMilliseconds: Int, reason: String?) {
    clock.schedule(after: delayMilliseconds) { [weak self] in
      self?.onDisconnect?(reason)
    }
  }
}

enum NativeScenarioAction {
  case connect
  case completeConnect(index: Int, value: String)
  case failConnect(index: Int, error: Error)
  case delayedMessage(milliseconds: Int, topic: String, payload: Data)
  case delayedDisconnect(milliseconds: Int, reason: String?)
  case advance(milliseconds: Int)
}

/// Applies the same transport-only actions used by the Android scenario runner.
final class NativeScenarioRunner {
  let clock: NativeScenarioClock
  let transport: NativeScenarioTransport

  init(clock: NativeScenarioClock = NativeScenarioClock(), transport: NativeScenarioTransport = NativeScenarioTransport()) {
    self.clock = clock
    self.transport = transport
  }

  @discardableResult
  func run(_ actions: [NativeScenarioAction]) -> NativeScenarioRunner {
    for action in actions {
      switch action {
      case .connect:
        transport.connect { _ in }
      case let .completeConnect(index, value):
        transport.completeConnect(at: index, with: value)
      case let .failConnect(index, error):
        transport.failConnect(at: index, with: error)
      case let .delayedMessage(milliseconds, topic, payload):
        transport.emitMessageAfter(clock, delayMilliseconds: milliseconds, topic: topic, payload: payload)
      case let .delayedDisconnect(milliseconds, reason):
        transport.emitDisconnectAfter(clock, delayMilliseconds: milliseconds, reason: reason)
      case let .advance(milliseconds):
        clock.advance(by: milliseconds)
      }
    }
    return self
  }
}
