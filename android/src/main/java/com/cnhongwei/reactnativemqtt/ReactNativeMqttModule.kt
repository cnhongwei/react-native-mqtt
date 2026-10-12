package com.cnhongwei.reactnativemqtt

import android.util.Base64
import com.facebook.react.bridge.Arguments
import com.facebook.react.bridge.Promise
import com.facebook.react.bridge.ReadableMap
import com.facebook.react.bridge.ReadableArray
import com.facebook.react.bridge.ReactApplicationContext
import com.facebook.react.bridge.WritableMap
import com.facebook.react.bridge.WritableArray
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.ConcurrentHashMap
import java.util.ArrayDeque

internal fun decodeStrictBase64(encoded: String): ByteArray {
  require(isStrictBase64Shape(encoded)) { "payloadBase64 contains invalid characters or padding" }
  val decoded = ByteArray(encoded.length / 4 * 3 - encoded.count { it == '=' })
  var outputIndex = 0
  var inputIndex = 0
  while (inputIndex < encoded.length) {
    val first = base64Value(encoded[inputIndex])
    val second = base64Value(encoded[inputIndex + 1])
    val third = if (encoded[inputIndex + 2] == '=') 0 else base64Value(encoded[inputIndex + 2])
    val fourth = if (encoded[inputIndex + 3] == '=') 0 else base64Value(encoded[inputIndex + 3])
    require(first >= 0 && second >= 0 && third >= 0 && fourth >= 0) { "payloadBase64 is invalid" }
    val combined = (first shl 18) or (second shl 12) or (third shl 6) or fourth
    if (outputIndex < decoded.size) decoded[outputIndex++] = (combined ushr 16).toByte()
    if (outputIndex < decoded.size) decoded[outputIndex++] = (combined ushr 8).toByte()
    if (outputIndex < decoded.size) decoded[outputIndex++] = combined.toByte()
    inputIndex += 4
  }
  require(encodeStrictBase64(decoded) == encoded) {
    "payloadBase64 has non-canonical padding or trailing bits"
  }
  return decoded
}

private fun encodeStrictBase64(bytes: ByteArray): String {
  val alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
  val output = StringBuilder((bytes.size + 2) / 3 * 4)
  var index = 0
  while (index < bytes.size) {
    val first = bytes[index].toInt() and 0xff
    val second = if (index + 1 < bytes.size) bytes[index + 1].toInt() and 0xff else 0
    val third = if (index + 2 < bytes.size) bytes[index + 2].toInt() and 0xff else 0
    output.append(alphabet[first ushr 2])
    output.append(alphabet[((first and 3) shl 4) or (second ushr 4)])
    output.append(if (index + 1 < bytes.size) alphabet[((second and 15) shl 2) or (third ushr 6)] else '=')
    output.append(if (index + 2 < bytes.size) alphabet[third and 63] else '=')
    index += 3
  }
  return output.toString()
}

internal fun isStrictBase64Shape(encoded: String): Boolean =
  encoded.length % 4 == 0 && encoded.matches(Regex("[A-Za-z0-9+/]*={0,2}")) &&
    (if (encoded.endsWith("==")) base64Value(encoded[encoded.length - 3]) and 15 == 0
    else if (encoded.endsWith("=")) base64Value(encoded[encoded.length - 2]) and 3 == 0
    else true)

private fun base64Value(value: Char): Int =
  when (value) {
    in 'A'..'Z' -> value - 'A'
    in 'a'..'z' -> value - 'a' + 26
    in '0'..'9' -> value - '0' + 52
    '+' -> 62
    '/' -> 63
    else -> -1
  }

internal fun findMqttSessionException(error: Throwable): MqttSessionException? {
  var current: Throwable? = error
  repeat(8) {
    when (current) {
      is MqttSessionException -> return current
      null -> return null
      else -> current = current.cause
    }
  }
  return null
}

class ReactNativeMqttModule(reactContext: ReactApplicationContext) :
  NativeReactNativeMqttSpec(reactContext) {

  private val batchIds = AtomicLong(0)
  private val streamEpochs = ConcurrentHashMap<String, Long>()
  private val batchLedger = BatchLedger()
  private val deliveryStates = ConcurrentHashMap<String, DeliveryState>()
  private lateinit var registry: MqttSessionRegistry

  init {
    registry = MqttSessionRegistry(onMessage = { instanceId, generation, message ->
    val delivery = deliveryStates[instanceId] ?: return@MqttSessionRegistry
    if (!delivery.enqueue(generation, message)) {
      val droppedMessages = delivery.recordDrop()
      if (delivery.takeOverflowDiagnostic()) {
        val diagnostic = Arguments.createMap().apply {
          putString("instanceId", instanceId)
          putDouble("generation", generation.toDouble())
          putString("code", "QUEUE_OVERFLOW")
          putString("message", "Message delivery queue is full")
          putString("operation", "onMessageBatch")
          putMap("metrics", Arguments.createMap().apply {
            putInt("queuedMessages", delivery.queuedMessages())
            putInt("queuedBytes", delivery.queuedBytes().coerceAtMost(Int.MAX_VALUE.toLong()).toInt())
            putInt("pendingOperations", registry.pendingOperations(instanceId))
            putLong("droppedMessages", droppedMessages)
            putInt("reconnectAttempts", registry.reconnectAttempts(instanceId).coerceAtMost(Int.MAX_VALUE.toLong()).toInt())
          })
        }
        emitOnDiagnostic(diagnostic)
      }
      if (delivery.overflowPolicy == "disconnect") {
        registry.disconnect(instanceId).whenComplete { snapshot, error ->
          if (error == null && snapshot != null) emitOnSessionSnapshot(snapshotMap(snapshot))
        }
      }
      return@MqttSessionRegistry
    }
    emitNextBatch(instanceId)
  }, onTransportLost = { instanceId, generation, disconnect ->
    if (disconnect != null) {
      emitOnDiagnostic(Arguments.createMap().apply {
        putString("instanceId", instanceId)
        putDouble("generation", generation.toDouble())
        putString("code", "SERVER_DISCONNECT")
        putString("message", "Broker disconnected the MQTT session")
        putString("operation", "disconnect")
        putInt("reasonCode", disconnect.reasonCode)
        disconnect.reasonString?.let { putString("reasonString", it) }
      })
    }
  }, onSessionSnapshot = { snapshot ->
    emitOnSessionSnapshot(snapshotMap(snapshot))
  })
  }

  private fun emitNextBatch(instanceId: String) {
    val delivery = deliveryStates[instanceId] ?: return
    val next = delivery.startNext() ?: return
    val epoch = streamEpochs.computeIfAbsent(instanceId) { 1L }
    val batchId = batchIds.incrementAndGet()
    val token = BatchToken(epoch, batchId)
    if (!batchLedger.reserve(instanceId, token)) {
      delivery.rollbackNext(next)
      return
    }
    val messages = Arguments.createArray().apply {
      next.forEach { (generation, message) ->
          pushMap(Arguments.createMap().apply {
            putString("instanceId", instanceId)
            putDouble("generation", generation.toDouble())
          putString("topic", message.topic)
          putString("payloadBase64", Base64.encodeToString(message.payload, Base64.NO_WRAP))
          putInt("qos", message.qos)
          putBoolean("retained", message.retained)
          if (message.userProperties.isNotEmpty()) {
            putArray("userProperties", Arguments.createArray().apply {
              message.userProperties.forEach { property ->
                pushMap(Arguments.createMap().apply {
                  putString("key", property["key"])
                  putString("value", property["value"])
                })
              }
            })
          }
          message.contentType?.let { putString("contentType", it) }
          message.responseTopic?.let { putString("responseTopic", it) }
          message.correlationData?.let { putString("correlationDataBase64", Base64.encodeToString(it, Base64.NO_WRAP)) }
          message.messageExpiryIntervalSeconds?.let { putDouble("messageExpiryIntervalSeconds", it.toDouble()) }
          message.payloadFormatIndicator?.let { putInt("payloadFormatIndicator", it) }
        })
      }
    }
    val batch = Arguments.createMap().apply {
      putString("instanceId", instanceId)
      putDouble("streamEpoch", epoch.toDouble())
      putDouble("batchId", batchId.toDouble())
      putArray("messages", messages)
    }
    emitOnMessageBatch(batch)
  }

  private fun snapshotMap(snapshot: Map<String, Any?>): WritableMap = Arguments.createMap().apply {
    putString("instanceId", snapshot["instanceId"] as? String ?: "")
    putDouble("revision", (snapshot["revision"] as? Number)?.toDouble() ?: 0.0)
    putDouble("generation", (snapshot["generation"] as? Number)?.toDouble() ?: 0.0)
    putString("state", snapshot["state"] as? String ?: "closed")
    val ack = snapshot["lastConnAck"] as? Map<*, *>
    if (ack == null) putNull("lastConnAck") else putMap("lastConnAck", Arguments.createMap().apply {
      putBoolean("sessionPresent", ack["sessionPresent"] as? Boolean ?: false)
      putInt("reasonCode", (ack["reasonCode"] as? Number)?.toInt() ?: 0)
      (ack["reasonString"] as? String)?.let { putString("reasonString", it) }
      (ack["assignedClientId"] as? String)?.let { putString("assignedClientId", it) }
      val properties = ack["userProperties"] as? List<*>
      if (properties != null) putArray("userProperties", Arguments.createArray().apply {
        properties.forEach { property ->
          val pair = property as? Map<*, *> ?: return@forEach
          pushMap(Arguments.createMap().apply {
            putString("key", pair["key"] as? String ?: "")
            putString("value", pair["value"] as? String ?: "")
          })
        }
      })
    })
    putDouble("subscriptionsRevision", (snapshot["subscriptionsRevision"] as? Number)?.toDouble() ?: 0.0)
    putString("subscriptionsStatus", snapshot["subscriptionsStatus"] as? String ?: "ready")
    putString("deliveryStatus", snapshot["deliveryStatus"] as? String ?: "active")
  }

  override fun getCapabilities(promise: Promise) {
    promise.resolve(mapOf(
      "wireVersion" to 1,
      "platform" to "android",
      "protocolVersions" to listOf(4, 5),
      "transports" to listOf("tcp", "tls", "ws", "wss"),
      "supportsSessionPresent" to true,
      "supportsDuplicateUserProperties" to true
    ))
  }

  private fun <T> forward(
    promise: Promise,
    future: java.util.concurrent.CompletableFuture<T>,
    operation: String? = null,
  ) {
    future.whenComplete { value, error ->
      if (error == null) {
        promise.resolve(bridgeValue(value))
      }
      else {
        val cause = findMqttSessionException(error) ?: error
        val userInfo = Arguments.createMap()
        if (cause is MqttSessionException) {
          (cause.operation ?: operation)?.let { userInfo.putString("operation", it) }
          cause.generation?.let { userInfo.putDouble("generation", it.toDouble()) }
          // Keep reasonCode=0: presence, not truthiness, carries its meaning.
          cause.reasonCode?.let { userInfo.putInt("reasonCode", it) }
          cause.reasonString?.let { userInfo.putString("reasonString", it) }
          cause.outcome?.let { userInfo.putString("outcome", it) }
        }
        promise.reject(
          if (cause is MqttSessionException) cause.errorCode else "ERR_UNKNOWN",
          cause.message,
          cause,
          userInfo
        )
      }
    }
  }

  private fun emitOperationDiagnostic(instanceId: String, operation: String, error: Throwable) {
    val cause = findMqttSessionException(error)
    emitOnDiagnostic(Arguments.createMap().apply {
      putString("instanceId", instanceId)
      putString("code", cause?.errorCode ?: "ERR_UNKNOWN")
      putString("message", cause?.message ?: error.message ?: "Operation failed")
      putString("operation", cause?.operation ?: operation)
      cause?.generation?.let { putDouble("generation", it.toDouble()) }
      cause?.reasonCode?.let { putInt("reasonCode", it) }
      cause?.reasonString?.let { putString("reasonString", it) }
      cause?.outcome?.let { putString("outcome", it) }
    })
  }

  private fun bridgeValue(value: Any?): Any? = when (value) {
    is Map<*, *> -> Arguments.createMap().also { map ->
      value.forEach { (key, nested) -> putBridgeValue(map, key.toString(), nested) }
    }
    is List<*> -> Arguments.createArray().also { array ->
      value.forEach { nested -> pushBridgeValue(array, nested) }
    }
    else -> value
  }

  private fun putBridgeValue(map: WritableMap, key: String, value: Any?) {
    when (value) {
      null -> map.putNull(key)
      is Boolean -> map.putBoolean(key, value)
      is Number -> map.putDouble(key, value.toDouble())
      is String -> map.putString(key, value)
      is Map<*, *> -> map.putMap(key, bridgeValue(value) as WritableMap)
      is List<*> -> map.putArray(key, bridgeValue(value) as WritableArray)
      else -> map.putString(key, value.toString())
    }
  }

  private fun pushBridgeValue(array: WritableArray, value: Any?) {
    when (value) {
      null -> array.pushNull()
      is Boolean -> array.pushBoolean(value)
      is Number -> array.pushDouble(value.toDouble())
      is String -> array.pushString(value)
      is Map<*, *> -> array.pushMap(bridgeValue(value) as WritableMap)
      is List<*> -> array.pushArray(bridgeValue(value) as WritableArray)
      else -> array.pushString(value.toString())
    }
  }

  override fun openSession(instanceId: String, options: ReadableMap, promise: Promise) {
    MqttOptionsParser.parse(options).fold(
      onSuccess = { parsed ->
        val open = registry.open(instanceId, parsed)
        forward(promise, open.thenApply { snapshot ->
          deliveryStates.putIfAbsent(instanceId, DeliveryState(parsed))
          snapshotMap(snapshot)
        }, "openSession")
      },
      onFailure = { promise.reject("ERR_INVALID_OPTIONS", it.message) }
    )
  }
  override fun connect(instanceId: String, promise: Promise) {
    val future = registry.connect(instanceId)
    future.whenComplete { _, error ->
      if (error != null) emitOperationDiagnostic(instanceId, "connect", error)
    }
    forward(promise, future, "connect")
  }
  override fun disconnect(instanceId: String, promise: Promise) = forward(promise, registry.disconnect(instanceId), "disconnect")
  override fun close(instanceId: String, promise: Promise) {
    val future = registry.close(instanceId)
    future.whenComplete { _, _ ->
      batchLedger.clear(instanceId)
      streamEpochs.remove(instanceId)
      deliveryStates.remove(instanceId)?.clear()
    }
    forward(promise, future, "close")
  }
  override fun getSnapshot(instanceId: String, promise: Promise) = forward(promise, registry.snapshot(instanceId), "getSnapshot")
  override fun setSubscription(instanceId: String, filter: String, options: ReadableMap, intentRevision: Double, promise: Promise) {
    if (!validTopicFilter(filter)) {
      promise.reject("ERR_INVALID_OPTIONS", "topic filter is invalid")
      return
    }
    if (!hasOnlyKeys(options, "qos", "noLocal", "retainAsPublished", "retainHandling")) {
      promise.reject("ERR_INVALID_OPTIONS", "unsupported subscription option")
      return
    }
    val revision = revision(intentRevision, promise) ?: return
    val qos = integer(options, "qos", 1, 0, 2, promise) ?: return
    val noLocal = boolean(options, "noLocal", false, promise) ?: return
    val retainAsPublished = boolean(options, "retainAsPublished", false, promise) ?: return
    val retainHandling = integer(options, "retainHandling", 0, 0, 2, promise) ?: return
    forward(promise, registry.setSubscription(instanceId, filter, qos, noLocal, retainAsPublished, retainHandling, revision), "setSubscription")
  }
  override fun removeSubscription(instanceId: String, filter: String, intentRevision: Double, promise: Promise) {
    if (!validTopicFilter(filter)) {
      promise.reject("ERR_INVALID_OPTIONS", "topic filter is invalid")
      return
    }
    val revision = revision(intentRevision, promise) ?: return
    forward(promise, registry.removeSubscription(instanceId, filter, revision), "removeSubscription")
  }
  override fun awaitSubscriptions(instanceId: String, generation: Double, desiredRevision: Double, promise: Promise) {
    val generationValue = revision(generation, promise) ?: return
    val desiredValue = revision(desiredRevision, promise) ?: return
    forward(promise, registry.awaitSubscriptions(instanceId, generationValue, desiredValue), "awaitSubscriptions")
  }
  override fun publish(instanceId: String, request: ReadableMap, promise: Promise) {
    if (!hasOnlyKeys(request, "topic", "payloadBase64", "qos", "retained", "userProperties", "contentType", "responseTopic", "correlationDataBase64", "messageExpiryIntervalSeconds", "payloadFormatIndicator")) {
      promise.reject("ERR_INVALID_OPTIONS", "unsupported publish option")
      return
    }
    val topic = request.getString("topic")
    val encoded = request.getString("payloadBase64")
    if (topic.isNullOrEmpty() || !validTopicName(topic) || encoded == null) {
      promise.reject("ERR_INVALID_OPTIONS", "publish topic and payloadBase64 are invalid")
      return
    }
    val payload = try {
      decodeStrictBase64(encoded)
    } catch (error: IllegalArgumentException) {
      promise.reject("ERR_INVALID_PAYLOAD", "payloadBase64 is invalid", error)
      return
    }
    val qos = integer(request, "qos", 1, 0, 2, promise) ?: return
    val retained = boolean(request, "retained", false, promise) ?: return
    val properties = publishProperties(request, promise) ?: return
    forward(promise, registry.publish(instanceId, topic, payload, qos, retained, properties), "publish")
  }
  override fun ackMessageBatch(instanceId: String, streamEpoch: Double, batchId: Double, promise: Promise) {
    if (!streamEpoch.isFinite() || !batchId.isFinite() || streamEpoch < 0 || batchId < 0 || streamEpoch != streamEpoch.toLong().toDouble() || batchId != batchId.toLong().toDouble()) {
      promise.reject("ERR_INVALID_OPTIONS", "ACK token must contain non-negative integers")
      return
    }
    val token = BatchToken(streamEpoch.toLong(), batchId.toLong())
    if (batchLedger.acknowledge(instanceId, token)) {
      deliveryStates[instanceId]?.acknowledge()
      emitNextBatch(instanceId)
    }
    promise.resolve(null)
  }

  override fun invalidate() {
    batchLedger.clear()
    deliveryStates.values.forEach { it.clear() }
    deliveryStates.clear()
    registry.invalidate()
    super.invalidate()
  }

  companion object {
    const val NAME = NativeReactNativeMqttSpec.NAME
  }

  private fun revision(value: Double, promise: Promise): Long? {
    if (!value.isFinite() || value < 0 || value != value.toLong().toDouble()) {
      promise.reject("ERR_INVALID_OPTIONS", "revision must be a non-negative integer")
      return null
    }
    return value.toLong()
  }

  private fun integer(options: ReadableMap, key: String, default: Int, min: Int, max: Int, promise: Promise): Int? {
    val value = if (options.hasKey(key) && !options.isNull(key)) options.getDouble(key) else default.toDouble()
    if (!value.isFinite() || value != value.toInt().toDouble() || value < min || value > max) {
      promise.reject("ERR_INVALID_OPTIONS", "$key must be an integer in $min..$max")
      return null
    }
    return value.toInt()
  }

  private fun boolean(options: ReadableMap, key: String, default: Boolean, promise: Promise): Boolean? {
    if (!options.hasKey(key) || options.isNull(key)) return default
    return try {
      options.getBoolean(key)
    } catch (error: Throwable) {
      promise.reject("ERR_INVALID_OPTIONS", "$key must be a boolean", error)
      null
    }
  }

  private fun publishProperties(options: ReadableMap, promise: Promise): MqttPublishProperties? {
    fun string(key: String): String? {
      if (!options.hasKey(key) || options.isNull(key)) return null
      return try { options.getString(key) }
      catch (error: Throwable) {
        promise.reject("ERR_INVALID_OPTIONS", "$key must be a string", error)
        null
      }
    }
    val contentType = string("contentType")
    if (options.hasKey("contentType") && !options.isNull("contentType") && contentType == null) return null
    val responseTopic = string("responseTopic")
    if (options.hasKey("responseTopic") && !options.isNull("responseTopic") && responseTopic == null) return null
    if (responseTopic != null && !validTopicName(responseTopic)) {
      promise.reject("ERR_INVALID_OPTIONS", "responseTopic is not a valid topic name")
      return null
    }
    val correlation = string("correlationDataBase64")?.let {
      try { decodeStrictBase64(it) } catch (error: IllegalArgumentException) {
        promise.reject("ERR_INVALID_PAYLOAD", "correlationDataBase64 is invalid", error)
        return null
      }
    }
    val expiry = if (!options.hasKey("messageExpiryIntervalSeconds") || options.isNull("messageExpiryIntervalSeconds")) null else {
      val value = options.getDouble("messageExpiryIntervalSeconds")
      if (!value.isFinite() || value != value.toLong().toDouble() || value !in 0.0..4294967295.0) {
        promise.reject("ERR_INVALID_OPTIONS", "messageExpiryIntervalSeconds must be an integer in 0..4294967295")
        return null
      }
      value.toLong()
    }
    val format = if (!options.hasKey("payloadFormatIndicator") || options.isNull("payloadFormatIndicator")) null else {
      val value = options.getDouble("payloadFormatIndicator")
      if (!value.isFinite() || value != value.toInt().toDouble() || value !in 0.0..1.0) {
        promise.reject("ERR_INVALID_OPTIONS", "payloadFormatIndicator must be 0 or 1")
        return null
      }
      value.toInt()
    }
    val userProperties: List<Pair<String, String>> = if (!options.hasKey("userProperties") || options.isNull("userProperties")) emptyList() else try {
      val array: ReadableArray = options.getArray("userProperties") ?: throw IllegalArgumentException("userProperties must be an array")
      val result = mutableListOf<Pair<String, String>>()
        for (index in 0 until array.size()) {
          val item = array.getMap(index) ?: throw IllegalArgumentException("userProperties[$index] must be an object")
          val key = item.getString("key") ?: throw IllegalArgumentException("userProperties[$index].key must be a string")
          val value = item.getString("value") ?: throw IllegalArgumentException("userProperties[$index].value must be a string")
          require(key.toByteArray(Charsets.UTF_8).size <= 65535 && value.toByteArray(Charsets.UTF_8).size <= 65535) { "userProperties[$index] exceeds the MQTT UTF-8 string limit" }
          result.add(key to value)
        }
      result
    } catch (error: Throwable) {
      promise.reject("ERR_INVALID_OPTIONS", error.message ?: "userProperties is invalid", error)
      return null
    }
    return MqttPublishProperties(userProperties, contentType, responseTopic, correlation, expiry, format)
  }

  private fun hasOnlyKeys(options: ReadableMap, vararg allowed: String): Boolean {
    val accepted = allowed.toSet()
    val iterator = options.keySetIterator()
    while (iterator.hasNextKey()) if (iterator.nextKey() !in accepted) return false
    return true
  }

  private fun validTopicName(topic: String): Boolean = topic.isNotEmpty() && validMqttUtf8(topic) && !topic.contains('+') && !topic.contains('#') && topic.toByteArray(Charsets.UTF_8).size <= 65535

  private fun validTopicFilter(filter: String): Boolean {
    if (filter.isEmpty() || !validMqttUtf8(filter) || filter.startsWith("\$share/") || filter.toByteArray(Charsets.UTF_8).size > 65535) return false
    val levels = filter.split('/')
    return levels.withIndex().all { (index, level) ->
      (!level.contains('+') || level == "+") && (!level.contains('#') || (level == "#" && index == levels.lastIndex))
    }
  }

  private fun validMqttUtf8(value: String): Boolean = isValidMqttUtf8(value)

}

internal data class BatchToken(val streamEpoch: Long, val batchId: Long)

internal class DeliveryState(private val options: ParsedMqttOptions) {
  private val queue = ArrayDeque<Pair<Long, MqttMessageData>>()
  private var bufferedBytes = 0L
  private var inFlight = false
  private var inFlightMessages = 0
  private var inFlightBytes = 0
  private var droppedMessageCount = 0L
  private var overflowDiagnosticEmitted = false

  val overflowPolicy: String get() = options.overflowPolicy

  @Synchronized
  fun enqueue(generation: Long, message: MqttMessageData): Boolean {
    if (message.payload.size > options.maxMessageBytes) return false
    val bufferedMessages = queue.size + inFlightMessages
    if (bufferedMessages >= options.maxQueuedMessages ||
      bufferedBytes + message.payload.size + inFlightBytes > options.maxQueuedBytes) return false
    queue.addLast(generation to message)
    bufferedBytes += message.payload.size
    return true
  }

  @Synchronized
  fun startNext(): List<Pair<Long, MqttMessageData>>? {
    if (inFlight || queue.isEmpty()) return null
    val next = mutableListOf<Pair<Long, MqttMessageData>>()
    var batchBytes = 0
    while (queue.isNotEmpty() && next.size < options.batchMaxMessages) {
      val candidate = queue.first()
      val candidateBytes = candidate.second.payload.size
      if (next.isNotEmpty() && batchBytes + candidateBytes > options.batchMaxBytes) break
      queue.removeFirst()
      next += candidate
      batchBytes += candidateBytes
      bufferedBytes -= candidateBytes
    }
    inFlight = true
    inFlightMessages = next.size
    inFlightBytes = batchBytes
    return next
  }

  @Synchronized
  fun rollbackNext(next: List<Pair<Long, MqttMessageData>>) {
    if (!inFlight) return
    inFlight = false
    inFlightMessages = 0
    inFlightBytes = 0
    next.asReversed().forEach { item ->
      queue.addFirst(item)
      bufferedBytes += item.second.payload.size
    }
  }

  @Synchronized
  fun acknowledge() {
    inFlight = false
    inFlightMessages = 0
    inFlightBytes = 0
    overflowDiagnosticEmitted = false
  }

  @Synchronized
  fun queuedMessages(): Int = queue.size + inFlightMessages

  @Synchronized
  fun queuedBytes(): Long = bufferedBytes + inFlightBytes

  @Synchronized
  fun recordDrop(): Long {
    droppedMessageCount += 1
    return droppedMessageCount
  }

  @Synchronized
  fun takeOverflowDiagnostic(): Boolean {
    if (overflowDiagnosticEmitted) return false
    overflowDiagnosticEmitted = true
    return true
  }

  @Synchronized
  fun clear() {
    queue.clear()
    bufferedBytes = 0
    inFlight = false
    inFlightMessages = 0
    inFlightBytes = 0
    droppedMessageCount = 0
    overflowDiagnosticEmitted = false
  }
}

internal class BatchLedger {
  private val outstanding = ConcurrentHashMap<String, BatchToken>()

  fun reserve(instanceId: String, token: BatchToken): Boolean =
    outstanding.putIfAbsent(instanceId, token) == null

  fun acknowledge(instanceId: String, token: BatchToken): Boolean =
    outstanding.remove(instanceId, token)

  fun clear(instanceId: String) = outstanding.remove(instanceId)

  fun clear() = outstanding.clear()
}
