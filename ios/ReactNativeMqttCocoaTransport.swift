import CocoaMQTT
import Foundation
import Security

private extension Array {
  subscript(safe index: Int) -> Element? {
    indices.contains(index) ? self[index] : nil
  }
}

private func mqttReasonError(_ message: String, reasonCode: Int, reasonString: String? = nil) -> NSError {
  NSError(
    domain: "ReactNativeMqtt",
    code: reasonCode,
    userInfo: [
      NSLocalizedDescriptionKey: message,
      "reasonCode": reasonCode,
      "reasonString": reasonString ?? message,
    ]
  )
}

struct ReactNativeMqttCocoaConnAck {
  let sessionPresent: Bool
  let reasonCode: UInt8
  let reasonString: String?
  let assignedClientIdentifier: String?
  let userProperties: [CocoaMQTTUserProperty]
}

private func configureCocoaSocket(_ socket: CocoaMQTTSocketProtocol, serverName: String, caBase64: String?, p12Base64: String?, p12Password: String?, insecure: Bool) throws -> [SecCertificate] {
  guard caBase64 != nil || p12Base64 != nil || insecure else { return [] }
  var certificates = [SecCertificate]()
  if let caBase64 {
    guard let data = Data(base64Encoded: caBase64), let certificate = SecCertificateCreateWithData(nil, data as CFData) else {
      throw NSError(domain: "ReactNativeMqtt", code: 40, userInfo: [NSLocalizedDescriptionKey: "tls.caBase64 is not a valid certificate"])
    }
    certificates.append(certificate)
  }
  var identity: CocoaMQTTClientIdentity?
  if let p12Base64 {
    guard let data = Data(base64Encoded: p12Base64) else {
      throw NSError(domain: "ReactNativeMqtt", code: 41, userInfo: [NSLocalizedDescriptionKey: "tls.p12Base64 is not valid base64"])
    }
    var imported: CFArray?
    let status = SecPKCS12Import(data as NSData, [kSecImportExportPassphrase as String: p12Password ?? ""] as NSDictionary, &imported)
    guard status == errSecSuccess, let items = imported as? [[String: Any]], let rawIdentity = items.first?[kSecImportItemIdentity as String] else {
      throw NSError(domain: "ReactNativeMqtt", code: 42, userInfo: [NSLocalizedDescriptionKey: "tls.p12Base64 or tls.p12Password is invalid"])
    }
    guard CFGetTypeID(rawIdentity as CFTypeRef) == SecIdentityGetTypeID() else {
      throw NSError(domain: "ReactNativeMqtt", code: 42, userInfo: [NSLocalizedDescriptionKey: "tls.p12Base64 did not contain a client identity"])
    }
    // Bridge the checked Core Foundation object, not the Swift `Any`
    // existential (which has a different representation on arm64).
    let secIdentity = unsafeBitCast(rawIdentity as CFTypeRef, to: SecIdentity.self)
    identity = CocoaMQTTClientIdentity(identity: secIdentity)
  }
  if let raw = socket as? CocoaMQTTSocket {
    raw.tlsServerName = serverName
    raw.trustedServerCertificates = certificates
    raw.clientIdentity = identity
    raw.usesSystemTrustStore = !insecure && certificates.isEmpty
    raw.manuallyEvaluateTrust = insecure
  } else if let webSocket = socket as? CocoaMQTTWebSocket {
    webSocket.tlsServerName = serverName
    webSocket.trustedServerCertificates = certificates
    webSocket.clientIdentity = identity
    webSocket.usesSystemTrustStore = !insecure && certificates.isEmpty
    webSocket.manuallyEvaluateTrust = insecure
  }
  return certificates
}

private func commonName(_ certificate: SecCertificate) -> String? {
  var value: CFString?
  guard SecCertificateCopyCommonName(certificate, &value) == errSecSuccess else { return nil }
  return value as String?
}

private func hasSubjectAlternativeNames(_ certificate: SecCertificate) -> Bool {
  let data = SecCertificateCopyData(certificate) as Data
  let oid: [UInt8] = [0x06, 0x03, 0x55, 0x1d, 0x11]
  return data.withUnsafeBytes { bytes in
    guard let base = bytes.baseAddress else { return false }
    guard data.count >= oid.count else { return false }
    let values = base.assumingMemoryBound(to: UInt8.self)
    return (0...(data.count - oid.count)).contains { index in
      oid.enumerated().allSatisfy { values[index + $0.offset] == $0.element }
    }
  }
}

private func evaluateConfiguredServerTrust(_ trust: SecTrust, serverName: String, certificates: [SecCertificate], allowInvalidHostnameForTesting: Bool = false) -> Bool {
  let leaf = SecTrustGetCertificateAtIndex(trust, 0)
  let cnFallback = allowInvalidHostnameForTesting && serverName.caseInsensitiveCompare("test.mosquitto.org") == .orderedSame && leaf.map { !hasSubjectAlternativeNames($0) && commonName($0)?.caseInsensitiveCompare(serverName) == .orderedSame } == true
  guard SecTrustSetPolicies(trust, cnFallback ? SecPolicyCreateBasicX509() : SecPolicyCreateSSL(true, serverName as CFString)) == errSecSuccess else { return false }
  if !certificates.isEmpty {
    guard SecTrustSetAnchorCertificates(trust, certificates as CFArray) == errSecSuccess,
          SecTrustSetAnchorCertificatesOnly(trust, true) == errSecSuccess else { return false }
  }
  var error: CFError?
  return SecTrustEvaluateWithError(trust, &error)
}

private protocol ReactNativeMqttCocoaTransporting: AnyObject {
  func connect(completion: @escaping (Result<ReactNativeMqttCocoaConnAck, Error>) -> Void)
  func disconnect()
  func disconnect(completion: @escaping (Error?) -> Void)
  func setDisconnectedHandler(_ handler: @escaping (NSNumber?, String?) -> Void)
  func setMessageHandler(_ handler: @escaping (String, NSData, NSNumber, Bool, NSDictionary?) -> Void)
  func subscribe(filter: String, qos: UInt8, noLocal: Bool, retainAsPublished: Bool, retainHandling: UInt8, completion: @escaping (Result<NSNumber, Error>) -> Void)
  func subscribeBatch(_ subscriptions: [(filter: String, qos: UInt8, noLocal: Bool, retainAsPublished: Bool, retainHandling: UInt8)], completion: @escaping (Result<[NSNumber], Error>) -> Void)
  func unsubscribe(filter: String, completion: @escaping (Result<Void, Error>) -> Void)
  func publish(topic: String, payload: [UInt8], qos: UInt8, retained: Bool, properties: NSDictionary?, completion: @escaping (Result<Void, Error>) -> Void)
}

/// A single-owner MQTT 5 transport used by the native session.
/// CocoaMQTT's automatic reconnect is deliberately disabled; retry ownership
/// remains in the Session state machine.
final class ReactNativeMqttCocoaTransport: NSObject, CocoaMQTT5Delegate, ReactNativeMqttCocoaTransporting {
  private let probe: ReactNativeMqttConnAckProbe
  private let mqtt: CocoaMQTT5
  private let callbackQueue = DispatchQueue(label: "com.cnhongwei.reactnativemqtt.cocoa-callback")
  private var connectCompletion: ((Result<ReactNativeMqttCocoaConnAck, Error>) -> Void)?
  private var subscribeCompletion: ((Result<NSNumber, Error>) -> Void)?
  private var subscribeBatchCompletion: ((Result<[NSNumber], Error>) -> Void)?
  private var pendingSubscribeFilters: [String] = []
  private var unsubscribeCompletion: ((Result<Void, Error>) -> Void)?
  private var publishCompletions: [UInt16: (Result<Void, Error>) -> Void] = [:]
  private var messageHandler: ((String, NSData, NSNumber, Bool, NSDictionary?) -> Void)?
  private var disconnectedHandler: ((NSNumber?, String?) -> Void)?
  private var serverDisconnectReasonCode: NSNumber?
  private var remoteDisconnectNotified = false
  private var disconnectCompletion: ((Error?) -> Void)?
  private var suppressDisconnectHandler = false
  private var configurationError: Error?
  private let insecureTrust: Bool
  private let allowInvalidHostnameForTesting: Bool
  private let tlsServerName: String
  private var configuredServerCertificates = [SecCertificate]()

  init(clientId: String, host: String, port: UInt16, transport: String = "tcp", path: String = "/mqtt", username: String? = nil, password: String? = nil, caBase64: String? = nil, p12Base64: String? = nil, p12Password: String? = nil, insecure: Bool = false, allowInvalidHostnameForTesting: Bool = false, keepAlive: UInt16 = 60, cleanStart: Bool = true, sessionExpiryInterval: UInt32 = 0, receiveMaximum: UInt16 = UInt16.max, userProperties: [NSDictionary] = [], willTopic: String? = nil, willPayloadBase64: String? = nil, willQos: UInt8 = 0, willRetained: Bool = false) {
    var socket: CocoaMQTTSocketProtocol = (transport == "ws" || transport == "wss") ? CocoaMQTTWebSocket(uri: path) : CocoaMQTTSocket()
    socket.enableSSL = transport == "tls" || transport == "wss"
    probe = ReactNativeMqttConnAckProbe(socket: socket)
    mqtt = CocoaMQTT5(clientID: clientId, host: host, port: port, socket: probe)
    insecureTrust = insecure
    self.allowInvalidHostnameForTesting = allowInvalidHostnameForTesting
    tlsServerName = host
    super.init()
    do { configuredServerCertificates = try configureCocoaSocket(socket, serverName: host, caBase64: caBase64, p12Base64: p12Base64, p12Password: p12Password, insecure: insecure) } catch { configurationError = error }
    mqtt.username = username
    mqtt.password = password
    mqtt.keepAlive = keepAlive
    mqtt.cleanSession = cleanStart
    let connectProperties = MqttConnectProperties()
    connectProperties.sessionExpiryInterval = sessionExpiryInterval
    connectProperties.receiveMaximum = receiveMaximum
    connectProperties.userPropertyList = userProperties.compactMap { pair in
      guard let key = pair["key"] as? String, let value = pair["value"] as? String else { return nil }
      return CocoaMQTTUserProperty(key: key, value: value)
    }
    mqtt.connectProperties = connectProperties
    if let willTopic, let willPayloadBase64, let willPayload = Data(base64Encoded: willPayloadBase64) {
      mqtt.willMessage = CocoaMQTT5Message(topic: willTopic, payload: [UInt8](willPayload), qos: CocoaMQTTQoS(rawValue: willQos) ?? .qos0, retained: willRetained)
    } else if willTopic != nil || willPayloadBase64 != nil {
      configurationError = NSError(domain: "ReactNativeMqtt", code: 43, userInfo: [NSLocalizedDescriptionKey: "willPayloadBase64 is invalid"])
    }
    mqtt.autoReconnect = false
    mqtt.delegate = self
    mqtt.delegateQueue = callbackQueue
  }

  func connect(completion: @escaping (Result<ReactNativeMqttCocoaConnAck, Error>) -> Void) {
    if let configurationError { completion(.failure(configurationError)); return }
    remoteDisconnectNotified = false
    connectCompletion = completion
    guard mqtt.connect() else {
      connectCompletion = nil
      completion(.failure(NSError(domain: "ReactNativeMqtt", code: 1, userInfo: [NSLocalizedDescriptionKey: "CocoaMQTT rejected connect before socket I/O"])))
      return
    }
  }

  func disconnect() {
    suppressDisconnectHandler = true
    mqtt.disconnect()
  }

  func disconnect(completion: @escaping (Error?) -> Void) {
    suppressDisconnectHandler = true
    disconnectCompletion = completion
    mqtt.disconnect()
  }

  func setDisconnectedHandler(_ handler: @escaping (NSNumber?, String?) -> Void) {
    disconnectedHandler = handler
  }

  func setMessageHandler(_ handler: @escaping (String, NSData, NSNumber, Bool, NSDictionary?) -> Void) {
    messageHandler = handler
  }

  func subscribe(filter: String, qos: UInt8, noLocal: Bool, retainAsPublished: Bool, retainHandling: UInt8, completion: @escaping (Result<NSNumber, Error>) -> Void) {
    subscribeBatch([(filter, qos, noLocal, retainAsPublished, retainHandling)]) { result in
      switch result {
      case .success(let granted): completion(.success(granted[0]))
      case .failure(let error): completion(.failure(error))
      }
    }
  }

  func subscribeBatch(_ subscriptions: [(filter: String, qos: UInt8, noLocal: Bool, retainAsPublished: Bool, retainHandling: UInt8)], completion: @escaping (Result<[NSNumber], Error>) -> Void) {
    guard !subscriptions.isEmpty else { completion(.success([])); return }
    let parsed = subscriptions.map { item -> MqttSubscription? in
      guard let qos = CocoaMQTTQoS(rawValue: item.qos), qos != .FAILURE,
            let retainHandling = CocoaRetainHandlingOption(rawValue: item.retainHandling) else { return nil }
      let subscription = MqttSubscription(topic: item.filter, qos: qos)
      subscription.noLocal = item.noLocal
      subscription.retainAsPublished = item.retainAsPublished
      subscription.retainHandling = retainHandling
      return subscription
    }
    guard parsed.allSatisfy({ $0 != nil }) else {
      completion(.failure(NSError(domain: "ReactNativeMqtt", code: 10, userInfo: [NSLocalizedDescriptionKey: "Invalid subscription options"])))
      return
    }
    pendingSubscribeFilters = subscriptions.map(\.filter)
    subscribeBatchCompletion = completion
    mqtt.subscribe(parsed.compactMap { $0 })
  }

  private func completeSubscribeBatch(success: NSDictionary, failed: [String], reasonCodes: [UInt8] = []) {
    guard let completion = subscribeBatchCompletion else { return }
    let results = pendingSubscribeFilters.enumerated().map { index, filter -> NSNumber in
      if let granted = success[filter] as? NSNumber { return granted }
      if let reason = reasonCodes[safe: index] { return NSNumber(value: reason) }
      // A missing or malformed per-filter ACK is a protocol failure; never
      // turn an unaccounted entry into a successful subscription.
      return NSNumber(value: 0x80)
    }
    subscribeBatchCompletion = nil
    pendingSubscribeFilters = []
    completion(.success(results))
  }

  private func failSubscribeBatch(_ error: Error) {
    subscribeBatchCompletion?(.failure(error))
    subscribeBatchCompletion = nil
    pendingSubscribeFilters = []
  }

  func unsubscribe(filter: String, completion: @escaping (Result<Void, Error>) -> Void) {
    unsubscribeCompletion = completion
    mqtt.unsubscribe(filter)
  }

  func publish(topic: String, payload: [UInt8], qos: UInt8, retained: Bool, properties: NSDictionary?, completion: @escaping (Result<Void, Error>) -> Void) {
    guard let qos = CocoaMQTTQoS(rawValue: qos), qos != .FAILURE else {
      completion(.failure(NSError(domain: "ReactNativeMqtt", code: 11, userInfo: [NSLocalizedDescriptionKey: "Invalid publish QoS"])))
      return
    }
    let message = CocoaMQTT5Message(topic: topic, payload: payload, qos: qos, retained: retained)
    let publishProperties = MqttPublishProperties()
    if let properties {
      publishProperties.contentType = properties["contentType"] as? String
      publishProperties.responseTopic = properties["responseTopic"] as? String
      if let correlationData = properties["correlationData"] as? NSData {
        publishProperties.correlationData = [UInt8](correlationData)
      }
      if let expiry = properties["messageExpiryIntervalSeconds"] as? NSNumber { publishProperties.messageExpiryInterval = expiry.uint32Value }
      if let indicator = properties["payloadFormatIndicator"] as? NSNumber { publishProperties.payloadFormatIndicator = PayloadFormatIndicator(rawValue: indicator.uint8Value) }
      if let userProperties = properties["userProperties"] as? [NSDictionary] {
        publishProperties.userProperties = userProperties.compactMap { pair in
          guard let key = pair["key"] as? String, let value = pair["value"] as? String else { return nil }
          return CocoaMQTTUserProperty(key: key, value: value)
        }
      }
    }
    let packetId = UInt16(truncatingIfNeeded: mqtt.publish(message, properties: publishProperties))
    if qos == .qos0 {
      completion(.success(()))
    } else {
      publishCompletions[packetId] = completion
    }
  }

  func mqtt5(_ mqtt5: CocoaMQTT5, didConnectAck ack: CocoaMQTTCONNACKReasonCode, connAckData: MqttDecodeConnAck?) {
    guard let sessionPresent = probe.sessionPresent else {
      connectCompletion?(.failure(NSError(domain: "ReactNativeMqtt", code: 2, userInfo: [NSLocalizedDescriptionKey: "CocoaMQTT did not expose CONNACK sessionPresent"])))
      connectCompletion = nil
      return
    }
    let result = ReactNativeMqttCocoaConnAck(sessionPresent: sessionPresent, reasonCode: ack.rawValue, reasonString: connAckData?.reasonString, assignedClientIdentifier: connAckData?.assignedClientIdentifier, userProperties: connAckData?.userProperties ?? [])
    connectCompletion?(.success(result))
    connectCompletion = nil
  }

  func mqtt5(_ mqtt5: CocoaMQTT5, didPublishMessage message: CocoaMQTT5Message, id: UInt16) {}
  func mqtt5(_ mqtt5: CocoaMQTT5, didPublishAck id: UInt16, pubAckData: MqttDecodePubAck?) {
    guard pubAckData?.reasonCode == nil || pubAckData?.reasonCode == .success else {
      let reason = pubAckData?.reasonCode?.rawValue ?? 0x80
      publishCompletions.removeValue(forKey: id)?(.failure(mqttReasonError("PUBACK rejected (reason 0x\(String(reason, radix: 16)))", reasonCode: Int(reason), reasonString: pubAckData?.reasonString)))
      return
    }
    publishCompletions.removeValue(forKey: id)?(.success(()))
  }
  func mqtt5(_ mqtt5: CocoaMQTT5, didPublishRec id: UInt16, pubRecData: MqttDecodePubRec?) {
    guard pubRecData?.reasonCode == nil || pubRecData?.reasonCode == .success else {
      let reason = pubRecData?.reasonCode?.rawValue ?? 0x80
      publishCompletions.removeValue(forKey: id)?(.failure(mqttReasonError("PUBREC rejected (reason 0x\(String(reason, radix: 16)))", reasonCode: Int(reason), reasonString: pubRecData?.reasonString)))
      return
    }
    // QoS 2 is complete only after PUBCOMP; PUBREC merely advances the exchange.
  }
  func mqtt5(_ mqtt5: CocoaMQTT5, didPublishComplete id: UInt16, pubCompData: MqttDecodePubComp?) {
    guard pubCompData?.reasonCode == nil || pubCompData?.reasonCode == .success else {
      let reason = pubCompData?.reasonCode?.rawValue ?? 0x80
      publishCompletions.removeValue(forKey: id)?(.failure(mqttReasonError("PUBCOMP rejected (reason 0x\(String(reason, radix: 16)))", reasonCode: Int(reason), reasonString: pubCompData?.reasonString)))
      return
    }
    publishCompletions.removeValue(forKey: id)?(.success(()))
  }
  func mqtt5(_ mqtt5: CocoaMQTT5, didReceiveMessage message: CocoaMQTT5Message, id: UInt16, publishData: MqttDecodePublish?) {
    var metadata: [String: Any] = [:]
    if let publishData {
      metadata["userProperties"] = publishData.userProperties.map { ["key": $0.key, "value": $0.value] }
      if let contentType = publishData.contentType { metadata["contentType"] = contentType }
      if let responseTopic = publishData.responseTopic { metadata["responseTopic"] = responseTopic }
      if let correlationData = publishData.correlationData { metadata["correlationData"] = NSData(bytes: correlationData, length: correlationData.count) }
      if let expiry = publishData.messageExpiryInterval { metadata["messageExpiryIntervalSeconds"] = NSNumber(value: expiry) }
      if let indicator = publishData.payloadFormatIndicator { metadata["payloadFormatIndicator"] = NSNumber(value: indicator.rawValue) }
    }
    messageHandler?(message.topic, NSData(bytes: message.payload, length: message.payload.count), NSNumber(value: message.qos.rawValue), message.retained, metadata as NSDictionary)
  }
  func mqtt5(_ mqtt5: CocoaMQTT5, didSubscribeTopics success: NSDictionary, failed: [String], subAckData: MqttDecodeSubAck?) {
    completeSubscribeBatch(success: success, failed: failed, reasonCodes: subAckData?.reasonCodes.map(\.rawValue) ?? [])
    subscribeCompletion = nil
  }
  func mqtt5(_ mqtt5: CocoaMQTT5, didUnsubscribeTopics topics: [String], unsubAckData: MqttDecodeUnsubAck?) {
    if let reasonCode = unsubAckData?.reasonCodes.first?.rawValue, reasonCode >= 0x80 {
      unsubscribeCompletion?(.failure(mqttReasonError("UNSUBACK rejected", reasonCode: Int(reasonCode), reasonString: unsubAckData?.reasonString)))
    } else {
      unsubscribeCompletion?(.success(()))
    }
    unsubscribeCompletion = nil
  }
  func mqtt5(_ mqtt5: CocoaMQTT5, didReceiveDisconnectReasonCode reasonCode: CocoaMQTTDISCONNECTReasonCode) {
    serverDisconnectReasonCode = NSNumber(value: reasonCode.rawValue)
    // The MQTT DISCONNECT frame is already authoritative at this point. Do
    // not wait for CocoaMQTT's later socket callback: on some transports that
    // callback is delayed or omitted after a broker-initiated disconnect.
    if !suppressDisconnectHandler && !remoteDisconnectNotified {
      remoteDisconnectNotified = true
      disconnectedHandler?(serverDisconnectReasonCode, nil)
    }
  }
  func mqtt5(_ mqtt5: CocoaMQTT5, didReceiveAuthReasonCode reasonCode: CocoaMQTTAUTHReasonCode) {}
  func mqtt5(_ mqtt5: CocoaMQTT5, didReceive trust: SecTrust, completionHandler: @escaping (Bool) -> Void) {
    completionHandler(insecureTrust || (!configuredServerCertificates.isEmpty && evaluateConfiguredServerTrust(trust, serverName: tlsServerName, certificates: configuredServerCertificates, allowInvalidHostnameForTesting: allowInvalidHostnameForTesting)))
  }
  func mqtt5UrlSession(_ mqtt: CocoaMQTT5, didReceiveTrust trust: SecTrust, didReceiveChallenge challenge: URLAuthenticationChallenge, completionHandler: @escaping (URLSession.AuthChallengeDisposition, URLCredential?) -> Void) {
    if insecureTrust {
      completionHandler(.useCredential, URLCredential(trust: trust))
    } else if !configuredServerCertificates.isEmpty {
      let accepted = evaluateConfiguredServerTrust(trust, serverName: tlsServerName, certificates: configuredServerCertificates, allowInvalidHostnameForTesting: allowInvalidHostnameForTesting)
      completionHandler(accepted ? .useCredential : .cancelAuthenticationChallenge, accepted ? URLCredential(trust: trust) : nil)
    } else {
      completionHandler(.performDefaultHandling, nil)
    }
  }
  func mqtt5DidPing(_ mqtt5: CocoaMQTT5) {}
  func mqtt5DidReceivePong(_ mqtt5: CocoaMQTT5) {}
  func mqtt5DidDisconnect(_ mqtt5: CocoaMQTT5, withError err: Error?) {
    if let completion = connectCompletion {
      completion(.failure(err ?? NSError(domain: "ReactNativeMqtt", code: 3, userInfo: [NSLocalizedDescriptionKey: "CocoaMQTT disconnected before CONNACK"])))
      connectCompletion = nil
    }
    let error = err ?? NSError(domain: "ReactNativeMqtt", code: 3, userInfo: [NSLocalizedDescriptionKey: "CocoaMQTT disconnected"])
    failSubscribeBatch(error)
    subscribeCompletion?(.failure(error))
    unsubscribeCompletion?(.failure(error))
    publishCompletions.values.forEach { $0(.failure(error)) }
    subscribeCompletion = nil
    unsubscribeCompletion = nil
    publishCompletions.removeAll()
    disconnectCompletion?(err)
    disconnectCompletion = nil
    // CocoaMQTT records a remote DISCONNECT before invoking this callback.
    // Read the SDK-owned value here as the source of truth: the separate
    // didReceiveDisconnectReasonCode delegate callback can be delivered out
    // of order relative to the disconnect callback on some transports.
    if let reason = mqtt5.lastDisconnectReason,
       reason.source == .remote {
      serverDisconnectReasonCode = NSNumber(value: reason.reasonCode.rawValue)
    }
    let suppress = suppressDisconnectHandler
    let alreadyNotified = remoteDisconnectNotified
    suppressDisconnectHandler = false
    if !suppress && !alreadyNotified { disconnectedHandler?(serverDisconnectReasonCode, nil) }
    remoteDisconnectNotified = false
    serverDisconnectReasonCode = nil
  }
}

/// MQTT 3.1.1 adapter. CocoaMQTT does not expose a session-present bit for
/// MQTT 3.1.1, so its CONNACK is reported with the protocol-defined false value.
final class ReactNativeMqttCocoa3Transport: NSObject, CocoaMQTTDelegate, ReactNativeMqttCocoaTransporting {
  private let mqtt: CocoaMQTT
  private let callbackQueue = DispatchQueue(label: "com.cnhongwei.reactnativemqtt.cocoa3-callback")
  private var connectCompletion: ((Result<ReactNativeMqttCocoaConnAck, Error>) -> Void)?
  private var subscribeCompletion: ((Result<NSNumber, Error>) -> Void)?
  private var subscribeBatchCompletion: ((Result<[NSNumber], Error>) -> Void)?
  private var pendingSubscribeFilters: [String] = []
  private var unsubscribeCompletion: ((Result<Void, Error>) -> Void)?
  private var publishCompletions: [UInt16: (Result<Void, Error>) -> Void] = [:]
  private var messageHandler: ((String, NSData, NSNumber, Bool, NSDictionary?) -> Void)?
  private var disconnectedHandler: ((NSNumber?, String?) -> Void)?
  private var disconnectCompletion: ((Error?) -> Void)?
  private var suppressDisconnectHandler = false
  private var configurationError: Error?
  private let insecureTrust: Bool
  private let allowInvalidHostnameForTesting: Bool
  private let tlsServerName: String
  private var configuredServerCertificates = [SecCertificate]()

  init(clientId: String, host: String, port: UInt16, transport: String = "tcp", path: String = "/mqtt", username: String? = nil, password: String? = nil, caBase64: String? = nil, p12Base64: String? = nil, p12Password: String? = nil, insecure: Bool = false, allowInvalidHostnameForTesting: Bool = false, keepAlive: UInt16 = 60, cleanSession: Bool = true, willTopic: String? = nil, willPayloadBase64: String? = nil, willQos: UInt8 = 0, willRetained: Bool = false) {
    var socket: CocoaMQTTSocketProtocol = (transport == "ws" || transport == "wss") ? CocoaMQTTWebSocket(uri: path) : CocoaMQTTSocket()
    socket.enableSSL = transport == "tls" || transport == "wss"
    mqtt = CocoaMQTT(clientID: clientId, host: host, port: port, socket: socket)
    insecureTrust = insecure
    self.allowInvalidHostnameForTesting = allowInvalidHostnameForTesting
    tlsServerName = host
    super.init()
    do { configuredServerCertificates = try configureCocoaSocket(socket, serverName: host, caBase64: caBase64, p12Base64: p12Base64, p12Password: p12Password, insecure: insecure) } catch { configurationError = error }
    mqtt.username = username
    mqtt.password = password
    mqtt.keepAlive = keepAlive
    mqtt.cleanSession = cleanSession
    if let willTopic, let willPayloadBase64, let willPayload = Data(base64Encoded: willPayloadBase64) {
      mqtt.willMessage = CocoaMQTTMessage(topic: willTopic, payload: [UInt8](willPayload), qos: CocoaMQTTQoS(rawValue: willQos) ?? .qos0, retained: willRetained)
    } else if willTopic != nil || willPayloadBase64 != nil {
      configurationError = NSError(domain: "ReactNativeMqtt", code: 43, userInfo: [NSLocalizedDescriptionKey: "willPayloadBase64 is invalid"])
    }
    mqtt.autoReconnect = false
    mqtt.delegate = self
    mqtt.delegateQueue = callbackQueue
  }

  func connect(completion: @escaping (Result<ReactNativeMqttCocoaConnAck, Error>) -> Void) {
    if let configurationError { completion(.failure(configurationError)); return }
    connectCompletion = completion
    guard mqtt.connect() else {
      connectCompletion = nil
      completion(.failure(NSError(domain: "ReactNativeMqtt", code: 21, userInfo: [NSLocalizedDescriptionKey: "CocoaMQTT rejected connect before socket I/O"])))
      return
    }
  }

  func disconnect() { suppressDisconnectHandler = true; mqtt.disconnect() }

  func disconnect(completion: @escaping (Error?) -> Void) {
    suppressDisconnectHandler = true
    disconnectCompletion = completion
    mqtt.disconnect()
  }

  func setDisconnectedHandler(_ handler: @escaping (NSNumber?, String?) -> Void) {
    disconnectedHandler = handler
  }

  func setMessageHandler(_ handler: @escaping (String, NSData, NSNumber, Bool, NSDictionary?) -> Void) {
    messageHandler = handler
  }

  func subscribe(filter: String, qos: UInt8, noLocal: Bool, retainAsPublished: Bool, retainHandling: UInt8, completion: @escaping (Result<NSNumber, Error>) -> Void) {
    subscribeBatch([(filter, qos, noLocal, retainAsPublished, retainHandling)]) { result in
      switch result {
      case .success(let granted): completion(.success(granted[0]))
      case .failure(let error): completion(.failure(error))
      }
    }
  }

  func subscribeBatch(_ subscriptions: [(filter: String, qos: UInt8, noLocal: Bool, retainAsPublished: Bool, retainHandling: UInt8)], completion: @escaping (Result<[NSNumber], Error>) -> Void) {
    guard subscriptions.allSatisfy({ $0.noLocal == false && $0.retainAsPublished == false && $0.retainHandling == 0 }) else {
      completion(.failure(NSError(domain: "ReactNativeMqtt", code: 22, userInfo: [NSLocalizedDescriptionKey: "MQTT 3.1.1 does not support the requested subscription options"])))
      return
    }
    let qosValues = subscriptions.map { CocoaMQTTQoS(rawValue: $0.qos) }
    guard qosValues.allSatisfy({ $0 != nil && $0! != .FAILURE }) else {
      completion(.failure(NSError(domain: "ReactNativeMqtt", code: 22, userInfo: [NSLocalizedDescriptionKey: "Invalid subscription QoS"])))
      return
    }
    pendingSubscribeFilters = subscriptions.map(\.filter)
    subscribeBatchCompletion = completion
    mqtt.subscribe(zip(subscriptions, qosValues).map { ($0.0.filter, $0.1!) })
  }

  private func completeSubscribeBatch(success: NSDictionary, failed: [String]) {
    guard let completion = subscribeBatchCompletion else { return }
    let results = pendingSubscribeFilters.map { filter -> NSNumber in
      if let granted = success[filter] as? NSNumber { return granted }
      return NSNumber(value: failed.contains(filter) ? 0x80 : 0x80)
    }
    subscribeBatchCompletion = nil
    pendingSubscribeFilters = []
    completion(.success(results))
  }

  private func failSubscribeBatch(_ error: Error) {
    subscribeBatchCompletion?(.failure(error))
    subscribeBatchCompletion = nil
    pendingSubscribeFilters = []
  }

  func unsubscribe(filter: String, completion: @escaping (Result<Void, Error>) -> Void) {
    unsubscribeCompletion = completion
    mqtt.unsubscribe(filter)
  }

  func publish(topic: String, payload: [UInt8], qos: UInt8, retained: Bool, properties: NSDictionary?, completion: @escaping (Result<Void, Error>) -> Void) {
    guard let qos = CocoaMQTTQoS(rawValue: qos), qos != .FAILURE else {
      completion(.failure(NSError(domain: "ReactNativeMqtt", code: 23, userInfo: [NSLocalizedDescriptionKey: "Invalid publish QoS"])))
      return
    }
    let packetId = mqtt.publish(CocoaMQTTMessage(topic: topic, payload: payload, qos: qos, retained: retained))
    guard packetId >= 0 else {
      completion(.failure(NSError(domain: "ReactNativeMqtt", code: 24, userInfo: [NSLocalizedDescriptionKey: "CocoaMQTT rejected publish"])))
      return
    }
    if qos == .qos0 { completion(.success(())) }
    else { publishCompletions[UInt16(packetId)] = completion }
  }

  func mqtt(_ mqtt: CocoaMQTT, didConnectAck ack: CocoaMQTTConnAck) {
    let result = ReactNativeMqttCocoaConnAck(sessionPresent: false, reasonCode: ack.rawValue, reasonString: nil, assignedClientIdentifier: nil, userProperties: [])
    connectCompletion?(.success(result))
    connectCompletion = nil
  }
  func mqtt(_ mqtt: CocoaMQTT, didReceive trust: SecTrust, completionHandler: @escaping (Bool) -> Void) {
    completionHandler(insecureTrust || (!configuredServerCertificates.isEmpty && evaluateConfiguredServerTrust(trust, serverName: tlsServerName, certificates: configuredServerCertificates, allowInvalidHostnameForTesting: allowInvalidHostnameForTesting)))
  }
  func mqttUrlSession(_ mqtt: CocoaMQTT, didReceiveTrust trust: SecTrust, didReceiveChallenge challenge: URLAuthenticationChallenge, completionHandler: @escaping (URLSession.AuthChallengeDisposition, URLCredential?) -> Void) {
    if insecureTrust {
      completionHandler(.useCredential, URLCredential(trust: trust))
    } else if !configuredServerCertificates.isEmpty {
      let accepted = evaluateConfiguredServerTrust(trust, serverName: tlsServerName, certificates: configuredServerCertificates)
      completionHandler(accepted ? .useCredential : .cancelAuthenticationChallenge, accepted ? URLCredential(trust: trust) : nil)
    } else {
      completionHandler(.performDefaultHandling, nil)
    }
  }
  func mqtt(_ mqtt: CocoaMQTT, didPublishMessage message: CocoaMQTTMessage, id: UInt16) {}
  func mqtt(_ mqtt: CocoaMQTT, didPublishAck id: UInt16) { publishCompletions.removeValue(forKey: id)?(.success(())) }
  func mqtt(_ mqtt: CocoaMQTT, didPublishComplete id: UInt16) { publishCompletions.removeValue(forKey: id)?(.success(())) }
  func mqtt(_ mqtt: CocoaMQTT, didReceiveMessage message: CocoaMQTTMessage, id: UInt16) {
    messageHandler?(message.topic, NSData(bytes: message.payload, length: message.payload.count), NSNumber(value: message.qos.rawValue), message.retained, nil)
  }
  func mqtt(_ mqtt: CocoaMQTT, didSubscribeTopics success: NSDictionary, failed: [String]) {
    completeSubscribeBatch(success: success, failed: failed)
    subscribeCompletion = nil
  }
  func mqtt(_ mqtt: CocoaMQTT, didUnsubscribeTopics topics: [String]) { unsubscribeCompletion?(.success(())); unsubscribeCompletion = nil }
  func mqttDidPing(_ mqtt: CocoaMQTT) {}
  func mqttDidReceivePong(_ mqtt: CocoaMQTT) {}
  func mqttDidDisconnect(_ mqtt: CocoaMQTT, withError err: Error?) {
    let error = err ?? NSError(domain: "ReactNativeMqtt", code: 26, userInfo: [NSLocalizedDescriptionKey: "CocoaMQTT disconnected"])
    connectCompletion?(.failure(error)); connectCompletion = nil
    failSubscribeBatch(error)
    subscribeCompletion?(.failure(error)); subscribeCompletion = nil
    unsubscribeCompletion?(.failure(error)); unsubscribeCompletion = nil
    publishCompletions.values.forEach { $0(.failure(error)) }
    publishCompletions.removeAll()
    disconnectCompletion?(err)
    disconnectCompletion = nil
    let suppress = suppressDisconnectHandler
    suppressDisconnectHandler = false
    if !suppress { disconnectedHandler?(nil, nil) }
  }
}

/// Objective-C boundary for the TurboModule. CocoaMQTT types never cross this
/// boundary, so the ObjC++ session only receives Foundation values.
private final class ReactNativeMqttSessionTransportAdapter: ReactNativeMqttSessionTransport {
  let underlying: ReactNativeMqttCocoaTransporting
  private var sessionDisconnectedHandler: ((Error?) -> Void)?
  var publicDisconnectedHandler: ((Error?) -> Void)?

  init(underlying: ReactNativeMqttCocoaTransporting) {
    self.underlying = underlying
  }

  func connect(completion: @escaping (Result<Any, Error>) -> Void) {
    underlying.connect { result in
      switch result {
      case .success(let ack): completion(.success(ack))
      case .failure(let error): completion(.failure(error))
      }
    }
  }

  func disconnect(completion: @escaping (Error?) -> Void) {
    underlying.disconnect(completion: completion)
  }

  func setDisconnectedHandler(_ handler: @escaping (Error?) -> Void) {
    sessionDisconnectedHandler = handler
    underlying.setDisconnectedHandler { [weak self] reasonCode, reasonString in
      var userInfo: [String: Any] = [:]
      if let reasonCode { userInfo["reasonCode"] = reasonCode }
      if let reasonString { userInfo["reasonString"] = reasonString }
      let error = userInfo.isEmpty ? nil : NSError(domain: "ReactNativeMqtt", code: 0, userInfo: userInfo)
      self?.sessionDisconnectedHandler?(error)
    }
  }
}

@objc(ReactNativeMqttCocoaBridge)
public final class ReactNativeMqttCocoaBridge: NSObject {
  private let transport: ReactNativeMqttCocoaTransporting
  private let session: ReactNativeMqttSession
  private let sessionTransport: ReactNativeMqttSessionTransportAdapter
  private let operationLedger: ReactNativeMqttOperationLedger
  private var sessionEventHandler: ((String, NSNumber, NSNumber, NSDictionary?) -> Void)?

  @objc(initWithClientId:host:port:protocolVersion:transport:path:username:password:caBase64:p12Base64:p12Password:insecure:allowInvalidHostnameForTesting:keepAlive:cleanStart:cleanSession:sessionExpiryInterval:receiveMaximum:userProperties:willTopic:willPayloadBase64:willQos:willRetained:reconnectEnabled:reconnectInitialDelayMs:reconnectMaxDelayMs:reconnectJitterRatio:connectTimeoutMs:disconnectTimeoutMs:)
  public init(clientId: String, host: String, port: NSNumber, protocolVersion: NSNumber, transport: String, path: String, username: String?, password: String?, caBase64: String?, p12Base64: String?, p12Password: String?, insecure: Bool, allowInvalidHostnameForTesting: Bool, keepAlive: NSNumber, cleanStart: Bool, cleanSession: Bool, sessionExpiryInterval: NSNumber, receiveMaximum: NSNumber, userProperties: [NSDictionary], willTopic: String?, willPayloadBase64: String?, willQos: NSNumber, willRetained: Bool, reconnectEnabled: Bool, reconnectInitialDelayMs: NSNumber, reconnectMaxDelayMs: NSNumber, reconnectJitterRatio: NSNumber, connectTimeoutMs: NSNumber, disconnectTimeoutMs: NSNumber) {
    if protocolVersion.intValue == 4 {
      self.transport = ReactNativeMqttCocoa3Transport(clientId: clientId, host: host, port: UInt16(truncating: port), transport: transport, path: path, username: username, password: password, caBase64: caBase64, p12Base64: p12Base64, p12Password: p12Password, insecure: insecure, allowInvalidHostnameForTesting: allowInvalidHostnameForTesting, keepAlive: UInt16(truncating: keepAlive), cleanSession: cleanSession, willTopic: willTopic, willPayloadBase64: willPayloadBase64, willQos: UInt8(truncating: willQos), willRetained: willRetained)
    } else {
      self.transport = ReactNativeMqttCocoaTransport(clientId: clientId, host: host, port: UInt16(truncating: port), transport: transport, path: path, username: username, password: password, caBase64: caBase64, p12Base64: p12Base64, p12Password: p12Password, insecure: insecure, allowInvalidHostnameForTesting: allowInvalidHostnameForTesting, keepAlive: UInt16(truncating: keepAlive), cleanStart: cleanStart, sessionExpiryInterval: UInt32(truncating: sessionExpiryInterval), receiveMaximum: UInt16(truncating: receiveMaximum), userProperties: userProperties, willTopic: willTopic, willPayloadBase64: willPayloadBase64, willQos: UInt8(truncating: willQos), willRetained: willRetained)
    }
    self.sessionTransport = ReactNativeMqttSessionTransportAdapter(underlying: self.transport)
    self.session = ReactNativeMqttSession(
      transport: self.sessionTransport,
      queueLabel: "com.cnhongwei.reactnativemqtt.cocoa-session",
      reconnectEnabled: reconnectEnabled,
      reconnectInitialDelayMs: reconnectInitialDelayMs.uint64Value,
      reconnectMaxDelayMs: reconnectMaxDelayMs.uint64Value,
      reconnectJitterRatio: reconnectJitterRatio.doubleValue,
      connectTimeoutMs: connectTimeoutMs.uint64Value,
      disconnectTimeoutMs: disconnectTimeoutMs.uint64Value,
      disconnectHandler: { [weak sessionTransport] error in sessionTransport?.publicDisconnectedHandler?(error) }
    )
    self.operationLedger = ReactNativeMqttOperationLedger(queueLabel: "com.cnhongwei.reactnativemqtt.cocoa-operations")
    super.init()
    session.setSnapshotHandler { [weak self] snapshot in
      guard snapshot.state != .connected else { return }
      self?.sessionEventHandler?(snapshot.state.rawValue, NSNumber(value: snapshot.revision), NSNumber(value: snapshot.generation), nil)
    }
    session.setConnectHandler { [weak self] snapshot, value in
      guard let ack = value as? ReactNativeMqttCocoaConnAck else { return }
      self?.sessionEventHandler?(snapshot.state.rawValue, NSNumber(value: snapshot.revision), NSNumber(value: snapshot.generation), [
        "sessionPresent": ack.sessionPresent,
        "reasonCode": Int(ack.reasonCode),
        "reasonString": ack.reasonString as Any,
        "assignedClientId": ack.assignedClientIdentifier as Any,
        "userProperties": ack.userProperties.map { ["key": $0.key, "value": $0.value] },
      ])
    }
  }

  @objc(connectWithCompletion:)
  public func connect(completion: @escaping ([String: Any]?, NSError?) -> Void) {
    session.connect { result in
      switch result {
      case .success(let value):
        guard let ack = value as? ReactNativeMqttCocoaConnAck else {
          completion(nil, NSError(domain: "ReactNativeMqtt", code: 28, userInfo: [NSLocalizedDescriptionKey: "Invalid CONNACK result"]))
          return
        }
        completion([
          "sessionPresent": ack.sessionPresent,
          "reasonCode": Int(ack.reasonCode),
          "reasonString": ack.reasonString as Any,
          "assignedClientId": ack.assignedClientIdentifier as Any,
          "userProperties": ack.userProperties.map { ["key": $0.key, "value": $0.value] },
        ], nil)
      case .failure(let error):
        completion(nil, error as NSError)
      }
    }
  }

  @objc public func disconnect() {
    session.disconnect { _ in }
  }

  @objc(disconnectWithCompletion:)
  public func disconnect(completion: @escaping (NSError?) -> Void) {
    session.disconnect { error in completion(error as NSError?) }
  }

  @objc(disposeWithCompletion:)
  public func dispose(completion: @escaping (NSError?) -> Void) {
    session.dispose { error in completion(error as NSError?) }
  }

  @objc(setDisconnectedHandler:)
  public func setDisconnectedHandler(_ handler: @escaping (NSNumber?, String?) -> Void) {
    sessionTransport.publicDisconnectedHandler = { error in
      let info = (error as NSError?)?.userInfo
      handler(info?["reasonCode"] as? NSNumber, info?["reasonString"] as? String)
    }
  }

  @objc(setSessionEventHandler:)
  public func setSessionEventHandler(_ handler: @escaping (String, NSNumber, NSNumber, NSDictionary?) -> Void) {
    sessionEventHandler = handler
  }

  @objc(setMessageHandler:)
  public func setMessageHandler(_ handler: @escaping (String, NSData, NSNumber, Bool, NSDictionary?) -> Void) {
    transport.setMessageHandler(handler)
  }

  @objc(subscribeFilter:qos:noLocal:retainAsPublished:retainHandling:completion:)
  public func subscribe(filter: String, qos: NSNumber, noLocal: Bool, retainAsPublished: Bool, retainHandling: NSNumber, completion: @escaping (NSNumber?, NSError?) -> Void) {
    operationLedger.submit(topic: filter, generation: session.snapshot.generation, timeoutMs: 30_000, execute: { [transport] finish in
      transport.subscribe(filter: filter, qos: qos.uint8Value, noLocal: noLocal, retainAsPublished: retainAsPublished, retainHandling: retainHandling.uint8Value) { result in
        switch result {
        case .success(let granted): finish(.success(granted))
        case .failure(let error): finish(.failure(error))
        }
      }
    }, completion: { result in
      switch result {
      case .success(let value): completion(value as? NSNumber, nil)
      case .failure(let error): completion(nil, error as NSError)
      }
    })
  }

  @objc(subscribeBatch:completion:)
  public func subscribeBatch(_ subscriptions: [NSDictionary], completion: @escaping ([NSNumber]?, NSError?) -> Void) {
    let parsed = subscriptions.compactMap { item -> (filter: String, qos: UInt8, noLocal: Bool, retainAsPublished: Bool, retainHandling: UInt8)? in
      guard let filter = item["filter"] as? String,
            let qos = item["qos"] as? NSNumber,
            let noLocal = item["noLocal"] as? NSNumber,
            let retainAsPublished = item["retainAsPublished"] as? NSNumber,
            let retainHandling = item["retainHandling"] as? NSNumber else { return nil }
      return (filter, qos.uint8Value, noLocal.boolValue, retainAsPublished.boolValue, retainHandling.uint8Value)
    }
    guard parsed.count == subscriptions.count else {
      completion(nil, NSError(domain: "ReactNativeMqtt", code: 27, userInfo: [NSLocalizedDescriptionKey: "Invalid subscription batch"])); return
    }
    transport.subscribeBatch(parsed) { result in
      switch result {
      case .success(let granted): completion(granted, nil)
      case .failure(let error): completion(nil, error as NSError)
      }
    }
  }

  @objc(unsubscribeFilter:completion:)
  public func unsubscribe(filter: String, completion: @escaping (NSError?) -> Void) {
    operationLedger.submit(topic: filter, generation: session.snapshot.generation, timeoutMs: 30_000, execute: { [transport] finish in
      transport.unsubscribe(filter: filter) { result in
        switch result {
        case .success: finish(.success(NSNull()))
        case .failure(let error): finish(.failure(error))
        }
      }
    }, completion: { result in
      switch result {
      case .success: completion(nil)
      case .failure(let error): completion(error as NSError)
      }
    })
  }

  @objc(publishTopic:payload:qos:retained:properties:completion:)
  public func publish(topic: String, payload: NSData, qos: NSNumber, retained: Bool, properties: NSDictionary?, completion: @escaping (NSError?) -> Void) {
    transport.publish(topic: topic, payload: [UInt8](payload as Data), qos: qos.uint8Value, retained: retained, properties: properties) { result in
      switch result {
      case .success: completion(nil)
      case .failure(let error): completion(error as NSError)
      }
    }
  }
}
