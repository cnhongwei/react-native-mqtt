import XCTest

final class NativeSessionScenarioTests: XCTestCase {
  func testVirtualClockPreservesDelayedCallbackOrderAndCancellation() {
    let clock = NativeScenarioClock()
    let transport = NativeScenarioTransport()
    var events: [String] = []

    let cancelled = clock.schedule(after: 25) { events.append("cancelled") }
    transport.onMessage = { topic, payload in
      events.append("message:\(topic):\(payload.count)")
    }
    transport.onDisconnect = { reason in
      events.append("disconnect:\(reason ?? "none")")
    }
    transport.emitMessageAfter(clock, delayMilliseconds: 20, topic: "late", payload: Data([1, 2]))
    transport.emitDisconnectAfter(clock, delayMilliseconds: 10, reason: "network")
    clock.cancel(cancelled)

    clock.advance(by: 10)
    XCTAssertEqual(events, ["disconnect:network"])
    clock.advance(by: 10)
    XCTAssertEqual(events, ["disconnect:network", "message:late:2"])
    XCTAssertEqual(clock.nowMilliseconds, 20)
  }

  func testConnectAndPublishCanCompleteOutOfOrderOrFailImmediately() {
    let transport = NativeScenarioTransport()
    var connectResults: [String] = []
    var publishResults: [String] = []

    transport.connect { result in
      connectResults.append((try? result.get()) ?? "failed")
    }
    transport.connect { result in
      connectResults.append((try? result.get()) ?? "failed")
    }
    transport.publish(topic: "devices/a", payload: Data([1, 2])) { result in
      publishResults.append(result.isSuccess ? "success" : "failed")
    }
    XCTAssertEqual(transport.connectAttempts, 2)
    XCTAssertEqual(transport.publishAttempts.count, 1)

    transport.completeConnect(at: 1, with: "second")
    transport.failConnect(at: 0, with: NSError(domain: "scenario", code: 1))
    transport.failPublish(at: 0, with: NSError(domain: "scenario", code: 2))

    XCTAssertEqual(connectResults, ["second", "failed"])
    XCTAssertEqual(publishResults, ["failed"])

    var immediateFailure = false
    transport.connectImmediately(failing: NSError(domain: "scenario", code: 3)) { result in
      immediateFailure = result.isFailure
    }
    transport.publishImmediately(failing: NSError(domain: "scenario", code: 4), topic: "devices/b", payload: Data()) { result in
      immediateFailure = immediateFailure && result.isFailure
    }
    XCTAssertTrue(immediateFailure)
  }

  func testTransportCanRepeatAStaleCallbackWithoutChangingPendingOrder() {
    let transport = NativeScenarioTransport()
    var callbacks: [String] = []
    transport.connect { result in callbacks.append((try? result.get()) ?? "failed") }

    transport.repeatConnectCallback(at: 0, with: "stale")
    transport.repeatConnectCallback(at: 0, with: "stale-again")
    XCTAssertEqual(callbacks, ["stale", "stale-again"])
    XCTAssertEqual(transport.pendingConnect.count, 1)
  }

  func testRunnerAppliesSharedActionVocabulary() {
    let runner = NativeScenarioRunner()
    var events: [String] = []
    runner.transport.onMessage = { topic, _ in events.append(topic) }
    runner.run([
      .delayedMessage(milliseconds: 20, topic: "late", payload: Data([1])),
      .delayedMessage(milliseconds: 10, topic: "early", payload: Data([2])),
      .advance(milliseconds: 10),
    ])
    XCTAssertEqual(events, ["early"])
    runner.run([.advance(milliseconds: 10)])
    XCTAssertEqual(events, ["early", "late"])
  }
}

private extension Result where Failure == Error {
  var isSuccess: Bool {
    if case .success = self { return true }
    return false
  }

  var isFailure: Bool {
    !isSuccess
  }
}
