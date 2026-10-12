package com.cnhongwei.reactnativemqtt

import com.facebook.react.bridge.ReadableMap
import com.facebook.react.bridge.ReadableArray

internal fun isValidMqttUtf8(value: String): Boolean {
  var index = 0
  while (index < value.length) {
    val character = value[index]
    if (character == '\u0000') return false
    if (character.isHighSurrogate()) {
      if (index + 1 >= value.length || !value[index + 1].isLowSurrogate()) return false
      index += 2
    } else {
      if (character.isLowSurrogate()) return false
      index += 1
    }
  }
  return true
}

internal data class ParsedMqttOptions(
  val host: String,
  val port: Int,
  val protocolVersion: Int,
  val transport: String,
  val clientId: String = "rn-${java.util.UUID.randomUUID().toString().replace("-", "").take(21)}",
  val username: String? = null,
  val password: String? = null,
  val willTopic: String? = null,
  val willPayloadBase64: String? = null,
  val willQos: Int = 0,
  val willRetained: Boolean = false,
  val keepAliveSeconds: Int = 60,
  val cleanSession: Boolean = true,
  val cleanStart: Boolean = true,
  val sessionExpiryIntervalSeconds: Long = 0,
  val userProperties: List<Pair<String, String>> = emptyList(),
  val path: String = if (transport == "ws" || transport == "wss") "/mqtt" else "",
  val connectTimeoutMs: Long = 30_000,
  val operationTimeoutMs: Long = 30_000,
  val disconnectTimeoutMs: Long = 5_000,
  val reconnectEnabled: Boolean = true,
  val reconnectInitialDelayMs: Long = 1_000,
  val reconnectMaxDelayMs: Long = 30_000,
  val reconnectJitterRatio: Double = 0.2,
  val tlsCaBase64: String? = null,
  val tlsP12Base64: String? = null,
  val tlsP12Password: String? = null,
  val tlsInsecure: Boolean = false,
  val tlsAllowInvalidHostnameForTesting: Boolean = false,
  val receiveMaximum: Int = 65535,
  val maxQueuedMessages: Int = 1024,
  val maxQueuedBytes: Long = 8L * 1024 * 1024,
  val maxMessageBytes: Int = 1024 * 1024,
  val batchMaxMessages: Int = 64,
  val batchMaxBytes: Int = 256 * 1024,
  val batchAckTimeoutMs: Long = 15_000,
  val overflowPolicy: String = "disconnect",
)

internal object MqttOptionsParser {
  fun parse(options: ReadableMap): Result<ParsedMqttOptions> = runCatching {
    val allowed = setOf(
      "host", "port", "protocolVersion", "transport", "path", "clientId", "username", "password",
      "keepAliveSeconds", "connectTimeoutMs", "operationTimeoutMs", "disconnectTimeoutMs", "reconnectEnabled",
      "reconnectInitialDelayMs", "reconnectMaxDelayMs", "reconnectJitterRatio", "tlsCaBase64", "tlsP12Base64",
      "tlsP12Password", "tlsInsecure", "tlsAllowInvalidHostnameForTesting", "willTopic", "willPayloadBase64", "willQos", "willRetained",
      "cleanSession", "cleanStart", "sessionExpiryIntervalSeconds", "receiveMaximum", "userProperties",
      "maxQueuedMessages", "maxQueuedBytes", "maxMessageBytes", "batchMaxMessages", "batchMaxBytes",
      "batchAckTimeoutMs", "overflowPolicy"
    )
    val iterator = options.keySetIterator()
    while (iterator.hasNextKey()) {
      val key = iterator.nextKey()
      require(key in allowed) { "unsupported option: $key" }
    }
    val host = options.getString("host")
      ?: throw IllegalArgumentException("host must be a non-empty string")
    require(host.isNotEmpty()) { "host must be a non-empty string" }
    val port = options.getDouble("port").toInt().also {
      require(options.getDouble("port") == it.toDouble() && it in 1..65535) {
        "port must be an integer in 1..65535"
      }
    }
    val protocolVersion = if (options.hasKey("protocolVersion")) {
      options.getDouble("protocolVersion").toInt().also {
        require(options.getDouble("protocolVersion") == it.toDouble() && it in 4..5) {
          "protocolVersion must be 4 or 5"
        }
      }
    } else 4
    val transport = if (options.hasKey("transport")) {
      options.getString("transport") ?: throw IllegalArgumentException("transport must be a string")
    } else "tcp"
    require(transport in setOf("tcp", "tls", "ws", "wss")) { "transport is invalid" }
    val hasPath = options.hasKey("path") && !options.isNull("path")
    require(!hasPath || transport == "ws" || transport == "wss") { "path is only valid for ws/wss" }
    val path = if (hasPath) options.getString("path") else if (transport == "ws" || transport == "wss") "/mqtt" else ""
    require(path != null && (transport == "tcp" || transport == "tls" || (path.isNotEmpty() && path.startsWith("/")))) { "path must start with /" }
    val clientId = if (options.hasKey("clientId") && !options.isNull("clientId")) {
      options.getString("clientId") ?: throw IllegalArgumentException("clientId must be a string")
    } else "rn-${java.util.UUID.randomUUID().toString().replace("-", "").take(21)}"
    require(isValidMqttUtf8(clientId) && (clientId.isNotEmpty() || (protocolVersion == 5 && readBoolean(options, "cleanStart", true))) && clientId.toByteArray(Charsets.UTF_8).size <= 65535) { "clientId is invalid" }
    val username = if (options.hasKey("username") && !options.isNull("username")) options.getString("username") else null
    val password = if (options.hasKey("password") && !options.isNull("password")) options.getString("password") else null
    require(username == null || (isValidMqttUtf8(username) && username.toByteArray(Charsets.UTF_8).size <= 65535)) { "username is invalid" }
    require(password == null || (isValidMqttUtf8(password) && password.toByteArray(Charsets.UTF_8).size <= 65535)) { "password is invalid" }
    require(password == null || username != null) { "password requires username" }
    val willTopic = readString(options, "willTopic")
    val willPayloadBase64 = readString(options, "willPayloadBase64")
    require((willTopic == null) == (willPayloadBase64 == null)) { "willTopic and willPayloadBase64 must be provided together" }
    require(willTopic == null || (isValidMqttUtf8(willTopic) && willTopic.isNotEmpty() && !willTopic.contains('+') && !willTopic.contains('#') && willTopic.toByteArray(Charsets.UTF_8).size <= 65535)) { "willTopic is invalid" }
    willPayloadBase64?.let { require(isStrictBase64Shape(it)) { "willPayloadBase64 is invalid" } }
    val willQos = readInt(options, "willQos", 0, 0, 2)
    val willRetained = readBoolean(options, "willRetained", false)
    val keepAlive = readInt(options, "keepAliveSeconds", 60, 0, 65535)
    val cleanSession = readBoolean(options, "cleanSession", true)
    val cleanStart = readBoolean(options, "cleanStart", true)
    require(protocolVersion != 4 || !hasNonNull(options, "cleanStart", "sessionExpiryIntervalSeconds", "receiveMaximum", "userProperties")) {
      "MQTT5 options are not valid for protocolVersion 4"
    }
    require(protocolVersion != 5 || !hasNonNull(options, "cleanSession")) {
      "cleanSession is not valid for protocolVersion 5"
    }
    val expiry = readLong(options, "sessionExpiryIntervalSeconds", 0, 0, 4294967295L)
    val receiveMaximum = readInt(options, "receiveMaximum", 65535, 1, 65535)
    val userProperties = if (options.hasKey("userProperties") && !options.isNull("userProperties")) {
      val array: ReadableArray = options.getArray("userProperties") ?: throw IllegalArgumentException("userProperties must be an array")
      (0 until array.size()).map { index ->
        val pair = array.getMap(index) ?: throw IllegalArgumentException("userProperties[$index] must be an object")
        val key = pair.getString("key") ?: throw IllegalArgumentException("userProperties[$index].key must be a string")
        val value = pair.getString("value") ?: throw IllegalArgumentException("userProperties[$index].value must be a string")
        require(isValidMqttUtf8(key) && key.toByteArray(Charsets.UTF_8).size <= 65535) { "userProperties[$index].key is invalid" }
        require(isValidMqttUtf8(value) && value.toByteArray(Charsets.UTF_8).size <= 65535) { "userProperties[$index].value is invalid" }
        key to value
      }
    } else emptyList()
    val connectTimeout = readLong(options, "connectTimeoutMs", 30_000, 1, Long.MAX_VALUE)
    val operationTimeout = readLong(options, "operationTimeoutMs", 30_000, 1, Long.MAX_VALUE)
    val disconnectTimeout = readLong(options, "disconnectTimeoutMs", 5_000, 1, Long.MAX_VALUE)
    val reconnectInitial = readLong(options, "reconnectInitialDelayMs", 1_000, 1, Long.MAX_VALUE)
    val reconnectMax = readLong(options, "reconnectMaxDelayMs", 30_000, 1, Long.MAX_VALUE)
    require(reconnectMax >= reconnectInitial) { "reconnectMaxDelayMs must be >= reconnectInitialDelayMs" }
    val jitter = readDouble(options, "reconnectJitterRatio", 0.2)
    require(jitter in 0.0..1.0) { "reconnectJitterRatio must be in 0..1" }
    val tlsFields = listOf("tlsCaBase64", "tlsP12Base64", "tlsP12Password", "tlsInsecure", "tlsAllowInvalidHostnameForTesting")
    require(transport == "tls" || transport == "wss" || !hasNonNull(options, *tlsFields.toTypedArray())) { "tls options are only valid for tls/wss" }
    val p12 = readString(options, "tlsP12Base64")
    val p12Password = readString(options, "tlsP12Password")
    require(p12Password == null || p12 != null) { "tlsP12Password requires tlsP12Base64" }
    val maxQueuedMessages = readInt(options, "maxQueuedMessages", 1024, 1, 1_000_000)
    val maxQueuedBytes = readLong(options, "maxQueuedBytes", 8L * 1024 * 1024, 1, Long.MAX_VALUE)
    val maxMessageBytes = readInt(options, "maxMessageBytes", 1024 * 1024, 1, Int.MAX_VALUE)
    val batchMaxMessages = readInt(options, "batchMaxMessages", 64, 1, 64)
    val batchMaxBytes = readInt(options, "batchMaxBytes", 256 * 1024, 1, 256 * 1024)
    val batchAckTimeout = readLong(options, "batchAckTimeoutMs", 15_000, 1, Long.MAX_VALUE)
    val overflowPolicy = readString(options, "overflowPolicy") ?: "disconnect"
    require(overflowPolicy == "disconnect" || overflowPolicy == "drop-newest") { "overflowPolicy is invalid" }
    ParsedMqttOptions(host, port, protocolVersion, transport, clientId, username, password, willTopic, willPayloadBase64, willQos, willRetained, keepAlive, cleanSession, cleanStart, expiry, userProperties,
      path ?: "", connectTimeout, operationTimeout, disconnectTimeout, readBoolean(options, "reconnectEnabled", true), reconnectInitial,
      reconnectMax, jitter, readString(options, "tlsCaBase64"), p12, p12Password, readBoolean(options, "tlsInsecure", false), readBoolean(options, "tlsAllowInvalidHostnameForTesting", false), receiveMaximum,
      maxQueuedMessages, maxQueuedBytes, maxMessageBytes, batchMaxMessages, batchMaxBytes, batchAckTimeout, overflowPolicy)
  }

  private fun hasNonNull(options: ReadableMap, vararg keys: String): Boolean = keys.any { options.hasKey(it) && !options.isNull(it) }
  private fun readString(options: ReadableMap, key: String): String? = if (options.hasKey(key) && !options.isNull(key)) options.getString(key) else null
  private fun readBoolean(options: ReadableMap, key: String, default: Boolean): Boolean = if (options.hasKey(key) && !options.isNull(key)) options.getBoolean(key) else default
  private fun readDouble(options: ReadableMap, key: String, default: Double): Double = if (options.hasKey(key) && !options.isNull(key)) options.getDouble(key) else default
  private fun readLong(options: ReadableMap, key: String, default: Long, min: Long, max: Long): Long {
    val value = readDouble(options, key, default.toDouble())
    require(value.isFinite() && value == value.toLong().toDouble() && value >= min && value <= max) { "$key must be an integer in $min..$max" }
    return value.toLong()
  }
  private fun readInt(options: ReadableMap, key: String, default: Int, min: Int, max: Int): Int = readLong(options, key, default.toLong(), min.toLong(), max.toLong()).toInt()
}
