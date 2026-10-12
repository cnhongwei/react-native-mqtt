import CocoaMQTT
import Foundation

/// Captures the MQTT CONNACK flags at the public socket boundary.
/// CocoaMQTT 2.4.0 keeps this bit inside its private FrameConnAck, so the
/// adapter observes the already-framed header/payload callbacks and forwards
/// every callback unchanged to CocoaMQTT.
final class ReactNativeMqttConnAckProbe: NSObject, CocoaMQTTSocketProtocol, CocoaMQTTSocketDelegate {
  private var socket: CocoaMQTTSocketProtocol
  private weak var downstream: CocoaMQTTSocketDelegate?
  private var downstreamQueue: DispatchQueue?
  private var isConnAck = false
  private(set) var sessionPresent: Bool?

  init(socket: CocoaMQTTSocketProtocol) {
    self.socket = socket
    super.init()
  }

  var enableSSL: Bool {
    get { socket.enableSSL }
    set { socket.enableSSL = newValue }
  }

  func setDelegate(_ delegate: CocoaMQTTSocketDelegate?, delegateQueue: DispatchQueue?) {
    downstream = delegate
    downstreamQueue = delegateQueue
    socket.setDelegate(self, delegateQueue: delegateQueue)
  }

  func connect(toHost host: String, onPort port: UInt16) throws {
    try socket.connect(toHost: host, onPort: port)
  }

  func connect(toHost host: String, onPort port: UInt16, withTimeout timeout: TimeInterval) throws {
    try socket.connect(toHost: host, onPort: port, withTimeout: timeout)
  }

  func disconnect() { socket.disconnect() }

  func readData(toLength length: UInt, withTimeout timeout: TimeInterval, tag: Int) {
    socket.readData(toLength: length, withTimeout: timeout, tag: tag)
  }

  func write(_ data: Data, withTimeout timeout: TimeInterval, tag: Int) {
    socket.write(data, withTimeout: timeout, tag: tag)
  }

  func socketConnected(_ socket: CocoaMQTTSocketProtocol) {
    forward { $0.socketConnected(self) }
  }

  func socket(_ socket: CocoaMQTTSocketProtocol, didReceive trust: SecTrust, completionHandler: @escaping (Bool) -> Void) {
    forward { $0.socket(self, didReceive: trust, completionHandler: completionHandler) }
  }

  func socketUrlSession(_ socket: CocoaMQTTSocketProtocol, didReceiveTrust trust: SecTrust, didReceiveChallenge challenge: URLAuthenticationChallenge, completionHandler: @escaping (URLSession.AuthChallengeDisposition, URLCredential?) -> Void) {
    forward { $0.socketUrlSession(self, didReceiveTrust: trust, didReceiveChallenge: challenge, completionHandler: completionHandler) }
  }

  func socket(_ socket: CocoaMQTTSocketProtocol, didWriteDataWithTag tag: Int) {
    forward { $0.socket(self, didWriteDataWithTag: tag) }
  }

  func socket(_ socket: CocoaMQTTSocketProtocol, didRead data: Data, withTag tag: Int) {
    if tag == 0, let header = data.first {
      isConnAck = header >> 4 == 2
    } else if tag == 2, isConnAck, let flags = data.first {
      sessionPresent = flags & 1 == 1
      isConnAck = false
    }
    forward { $0.socket(self, didRead: data, withTag: tag) }
  }

  func socketDidDisconnect(_ socket: CocoaMQTTSocketProtocol, withError error: Error?) {
    forward { $0.socketDidDisconnect(self, withError: error) }
  }

  private func forward(_ body: @escaping (CocoaMQTTSocketDelegate) -> Void) {
    guard let downstream else { return }
    if let downstreamQueue {
      downstreamQueue.async { body(downstream) }
    } else {
      body(downstream)
    }
  }
}
