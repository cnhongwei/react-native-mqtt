package com.cnhongwei.reactnativemqtt

import java.io.ByteArrayInputStream
import java.nio.ByteBuffer
import java.security.KeyStore
import java.security.Provider
import java.security.cert.CertificateFactory
import java.security.cert.X509Certificate
import javax.net.ssl.HttpsURLConnection
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import javax.net.SocketFactory
import javax.net.ssl.HostnameVerifier
import javax.net.ssl.KeyManagerFactory
import javax.net.ssl.ManagerFactoryParameters
import javax.net.ssl.SSLContext
import javax.net.ssl.TrustManager
import javax.net.ssl.TrustManagerFactory
import javax.net.ssl.TrustManagerFactorySpi
import javax.net.ssl.X509TrustManager
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken
import org.eclipse.paho.client.mqttv3.MqttAsyncClient
import org.eclipse.paho.client.mqttv3.MqttCallback
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended
import org.eclipse.paho.client.mqttv3.MqttConnectOptions
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import org.eclipse.paho.client.mqttv3.MqttMessage
import org.eclipse.paho.mqttv5.client.IMqttMessageListener
import org.eclipse.paho.mqttv5.client.MqttCallback as Mqtt5Callback
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions
import org.eclipse.paho.mqttv5.client.MqttDisconnectResponse
import org.eclipse.paho.mqttv5.client.MqttAsyncClient as Mqtt5AsyncClient
import org.eclipse.paho.mqttv5.common.MqttException
import org.eclipse.paho.mqttv5.common.MqttMessage as Mqtt5Message
import org.eclipse.paho.mqttv5.common.MqttSubscription
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence as Mqtt5MemoryPersistence
import org.eclipse.paho.mqttv5.common.packet.MqttProperties
import org.eclipse.paho.mqttv5.common.packet.MqttConnAck as PahoConnAck
import org.eclipse.paho.mqttv5.common.packet.MqttPubAck
import org.eclipse.paho.mqttv5.common.packet.MqttPubComp
import org.eclipse.paho.mqttv5.common.packet.MqttPubRec
import org.eclipse.paho.mqttv5.common.packet.MqttSubAck
import org.eclipse.paho.mqttv5.common.packet.MqttUnsubAck
import org.eclipse.paho.mqttv5.common.packet.UserProperty

internal fun decodeTlsBase64(encoded: String): ByteArray {
  require(encoded.length % 4 == 0) { "TLS Base64 length must be a multiple of four" }
  if (encoded.isEmpty()) return ByteArray(0)
  fun value(c: Char): Int = when (c) {
    in 'A'..'Z' -> c.code - 'A'.code
    in 'a'..'z' -> c.code - 'a'.code + 26
    in '0'..'9' -> c.code - '0'.code + 52
    '+' -> 62
    '/' -> 63
    else -> -1
  }
  val padding = if (encoded.endsWith("==")) 2 else if (encoded.endsWith("=")) 1 else 0
  val output = ByteArray(encoded.length / 4 * 3 - padding)
  var out = 0
  var i = 0
  while (i < encoded.length) {
    val a = value(encoded[i]); val b = value(encoded[i + 1])
    val c = if (encoded[i + 2] == '=') -2 else value(encoded[i + 2])
    val d = if (encoded[i + 3] == '=') -2 else value(encoded[i + 3])
    require(a >= 0 && b >= 0 && (c == -2 || c >= 0) && (d == -2 || d >= 0) && !(c == -2 && d != -2)) { "TLS Base64 is invalid" }
    require(i + 4 == encoded.length || (c >= 0 && d >= 0)) { "TLS Base64 padding must be final" }
    if (c == -2) require((b and 15) == 0) { "TLS Base64 has non-zero trailing bits" }
    if (d == -2 && c >= 0) require((c and 3) == 0) { "TLS Base64 has non-zero trailing bits" }
    val bits = (a shl 18) or (b shl 12) or ((if (c >= 0) c else 0) shl 6) or (if (d >= 0) d else 0)
    if (out < output.size) output[out++] = (bits ushr 16).toByte()
    if (c >= 0 && out < output.size) output[out++] = (bits ushr 8).toByte()
    if (d >= 0 && out < output.size) output[out++] = bits.toByte()
    i += 4
  }
  return output
}

internal data class MqttConnAck(
  val sessionPresent: Boolean,
  val reasonCode: Int? = null,
  val reasonString: String? = null,
  val assignedClientId: String? = null,
  val userProperties: List<Map<String, String>> = emptyList(),
)
internal data class MqttDisconnect(val reasonCode: Int, val reasonString: String?)
internal data class MqttSubscriptionResult(val grantedQos: Int, val reasonCode: Int)
internal data class MqttSubscriptionRequest(val filter: String, val qos: Int, val noLocal: Boolean, val retainAsPublished: Boolean, val retainHandling: Int)
internal data class MqttPublishProperties(
  val userProperties: List<Pair<String, String>> = emptyList(), val contentType: String? = null,
  val responseTopic: String? = null, val correlationData: ByteArray? = null,
  val messageExpiryIntervalSeconds: Long? = null, val payloadFormatIndicator: Int? = null,
)
internal data class MqttMessageData(
  val topic: String, val payload: ByteArray, val qos: Int, val retained: Boolean,
  val userProperties: List<Map<String, String>> = emptyList(), val contentType: String? = null,
  val responseTopic: String? = null, val correlationData: ByteArray? = null,
  val messageExpiryIntervalSeconds: Long? = null, val payloadFormatIndicator: Int? = null,
)

internal interface MqttTransport {
  fun connect(): CompletableFuture<MqttConnAck>
  /** True when a lost network connection can be re-established on this client. */
  fun supportsInPlaceReconnect(): Boolean = false
  fun disconnect(): CompletableFuture<Void>
  fun subscribe(filter: String, qos: Int, noLocal: Boolean, retainAsPublished: Boolean, retainHandling: Int, onMessage: ((MqttMessageData) -> Unit)? = null): CompletableFuture<MqttSubscriptionResult>
  fun subscribeBatch(requests: List<MqttSubscriptionRequest>): CompletableFuture<List<MqttSubscriptionResult>> {
    if (requests.isEmpty()) return CompletableFuture.completedFuture(emptyList())
    val futures = requests.map { subscribe(it.filter, it.qos, it.noLocal, it.retainAsPublished, it.retainHandling) }
    return CompletableFuture.allOf(*futures.toTypedArray()).thenApply { futures.map { it.join() } }
  }
  fun unsubscribe(filter: String): CompletableFuture<Int>
  fun publish(topic: String, payload: ByteArray, qos: Int, retained: Boolean): CompletableFuture<Void>
  fun publish(topic: String, payload: ByteArray, qos: Int, retained: Boolean, properties: MqttPublishProperties): CompletableFuture<Void> = publish(topic, payload, qos, retained)
  fun setDisconnectedListener(listener: (MqttDisconnect?) -> Unit)
}

internal class InsecureTrustManagerFactorySpi : TrustManagerFactorySpi() {
  override fun engineInit(keystore: KeyStore?) = Unit
  override fun engineInit(spec: ManagerFactoryParameters?) = Unit
  override fun engineGetTrustManagers(): Array<TrustManager> = arrayOf(object : X509TrustManager {
    override fun getAcceptedIssuers() = emptyArray<X509Certificate>()
    override fun checkClientTrusted(chain: Array<X509Certificate>, authType: String) = Unit
    override fun checkServerTrusted(chain: Array<X509Certificate>, authType: String) = Unit
  })
}

private fun scopedMosquittoHostnameVerifier(expectedHost: String): HostnameVerifier = HostnameVerifier { host, session ->
  if (!expectedHost.equals("test.mosquitto.org", ignoreCase = true) || !host.equals(expectedHost, ignoreCase = true)) return@HostnameVerifier false
  val leaf = runCatching { session.peerCertificates.firstOrNull() as? X509Certificate }.getOrNull() ?: return@HostnameVerifier false
  val sans = runCatching { leaf.subjectAlternativeNames.orEmpty().filter { it.size >= 2 && it[0] == 2 }.mapNotNull { it[1] as? String } }.getOrDefault(emptyList())
  if (sans.isNotEmpty()) return@HostnameVerifier HttpsURLConnection.getDefaultHostnameVerifier().verify(host, session)
  val commonName = Regex("(?:^|,)\\s*CN=([^,]+)", RegexOption.IGNORE_CASE)
    .find(leaf.subjectX500Principal.name)
    ?.groupValues
    ?.getOrNull(1)
    ?.trim()
  commonName?.equals(host, ignoreCase = true) == true
}

internal class PahoMqttTransport(private val options: ParsedMqttOptions, private val onMessage: (MqttMessageData) -> Unit = {}) : MqttTransport {
  companion object {
    init {
      org.eclipse.paho.client.mqttv3.logging.LoggerFactory.setLogger(PahoLogger::class.java.name)
      org.eclipse.paho.mqttv5.client.logging.LoggerFactory.setLogger(PahoLogger::class.java.name)
    }
  }

  private var mqtt3: MqttAsyncClient? = null
  private var mqtt5: Mqtt5AsyncClient? = null
  @Volatile private var reconnecting3: CompletableFuture<MqttConnAck>? = null
  @Volatile private var reconnecting5: CompletableFuture<MqttConnAck>? = null
  @Volatile private var connected3 = false
  @Volatile private var connected5 = false
  private var disconnectedListener: (MqttDisconnect?) -> Unit = {}

  override fun setDisconnectedListener(listener: (MqttDisconnect?) -> Unit) { disconnectedListener = listener }
  override fun supportsInPlaceReconnect(): Boolean = true
  override fun connect(): CompletableFuture<MqttConnAck> = if (options.protocolVersion == 5) connect5() else connect3()
  override fun disconnect(): CompletableFuture<Void> = try {
    when {
      mqtt5 != null -> disconnect5(mqtt5!!)
      mqtt3 != null -> disconnect3(mqtt3!!)
      else -> CompletableFuture.completedFuture(null)
    }
  } catch (error: Throwable) { failed("ERR_DISCONNECT", error.message ?: "Paho disconnect failed") }

  override fun subscribe(filter: String, qos: Int, noLocal: Boolean, retainAsPublished: Boolean, retainHandling: Int, onMessage: ((MqttMessageData) -> Unit)?): CompletableFuture<MqttSubscriptionResult> {
    val callback = onMessage ?: this.onMessage
    return try {
      if (options.protocolVersion == 5) {
        val subscription = MqttSubscription(filter, qos).also { it.isNoLocal = noLocal; it.isRetainAsPublished = retainAsPublished; it.retainHandling = retainHandling }
        val token = mqtt5?.subscribe(subscription) ?: return failed("ERR_NOT_CONNECTED", "MQTT 5 client is not connected")
        tokenFuture(token, token).thenApply { ack -> ensureV5Success(ack); MqttSubscriptionResult(ack.getGrantedQos().firstOrNull() ?: -1, ack.getReasonCodes().firstOrNull() ?: -1) }
      } else {
        val token = mqtt3?.subscribe(filter, qos, org.eclipse.paho.client.mqttv3.IMqttMessageListener { topic, message -> callback(topic.toMessageData(message)) })
          ?: return failed("ERR_NOT_CONNECTED", "MQTT 3.1.1 client is not connected")
        tokenFuture(token, token).thenApply { ack -> MqttSubscriptionResult(ack.getGrantedQos().firstOrNull() ?: -1, ack.getGrantedQos().firstOrNull() ?: -1) }
      }
    } catch (error: Throwable) { failed("ERR_SUBSCRIBE", error.message ?: "Paho subscribe failed") }
  }

  override fun subscribeBatch(requests: List<MqttSubscriptionRequest>): CompletableFuture<List<MqttSubscriptionResult>> {
    if (requests.isEmpty()) return CompletableFuture.completedFuture(emptyList())
    return try {
      if (options.protocolVersion == 5) {
        val subscriptions = requests.map { request -> MqttSubscription(request.filter, request.qos).also { it.isNoLocal = request.noLocal; it.isRetainAsPublished = request.retainAsPublished; it.retainHandling = request.retainHandling } }.toTypedArray()
        val token = mqtt5?.subscribe(subscriptions) ?: return failed("ERR_NOT_CONNECTED", "MQTT 5 client is not connected")
        tokenFuture(token, token).thenApply { ack ->
          ensureV5Success(ack)
          require(ack.getReasonCodes().size == requests.size) { "SUBACK item count did not match request" }
          ack.getReasonCodes().mapIndexed { index, reason -> MqttSubscriptionResult(ack.getGrantedQos().getOrElse(index) { -1 }, reason) }
        }
      } else {
        val token = mqtt3?.subscribe(requests.map { it.filter }.toTypedArray(), requests.map { it.qos }.toIntArray()) ?: return failed("ERR_NOT_CONNECTED", "MQTT 3.1.1 client is not connected")
        tokenFuture(token, Unit).thenApply {
          require(token.getGrantedQos().size == requests.size) { "SUBACK item count did not match request" }
          token.getGrantedQos().map { MqttSubscriptionResult(it, it) }
        }
      }
    } catch (error: Throwable) { failed("ERR_SUBSCRIBE", error.message ?: "Paho subscribe failed") }
  }

  override fun unsubscribe(filter: String): CompletableFuture<Int> = try {
    if (options.protocolVersion == 5) {
      val token = mqtt5?.unsubscribe(filter) ?: return failed("ERR_NOT_CONNECTED", "MQTT 5 client is not connected")
      tokenFuture(token, Unit).thenApply { ensureV5Success(token); token.getReasonCodes().firstOrNull() ?: -1 }
    } else {
      val token = mqtt3?.unsubscribe(filter) ?: return failed("ERR_NOT_CONNECTED", "MQTT 3.1.1 client is not connected")
      tokenFuture(token, 0)
    }
  } catch (error: Throwable) { failed("ERR_UNSUBSCRIBE", error.message ?: "Paho unsubscribe failed") }

  override fun publish(topic: String, payload: ByteArray, qos: Int, retained: Boolean): CompletableFuture<Void> = publish(topic, payload, qos, retained, MqttPublishProperties())
  override fun publish(topic: String, payload: ByteArray, qos: Int, retained: Boolean, properties: MqttPublishProperties): CompletableFuture<Void> {
    if (options.protocolVersion != 5 && (properties.userProperties.isNotEmpty() || properties.contentType != null || properties.responseTopic != null || properties.correlationData != null || properties.messageExpiryIntervalSeconds != null || properties.payloadFormatIndicator != null)) return failed("ERR_INVALID_OPTIONS", "MQTT 5 publish properties are not valid for MQTT 3.1.1")
    return try {
      if (options.protocolVersion == 5) {
        val message = Mqtt5Message(payload, qos, retained, properties.toPaho())
        val token = mqtt5?.publish(topic, message) ?: return failed("ERR_NOT_CONNECTED", "MQTT 5 client is not connected")
        tokenFuture(token, Unit).thenApply { ensureV5Success(token); null }
      } else {
        val message = MqttMessage(payload).also { it.qos = qos; it.isRetained = retained }
        val token = mqtt3?.publish(topic, message) ?: return failed("ERR_NOT_CONNECTED", "MQTT 3.1.1 client is not connected")
        tokenFuture(token, Unit).thenApply { null }
      }
    } catch (error: Throwable) { failed("ERR_PUBLISH_FAILED", error.message ?: "Paho publish failed") }
  }

  private fun connect3(): CompletableFuture<MqttConnAck> = try {
    val client = mqtt3 ?: MqttAsyncClient(uri(), options.clientId, MemoryPersistence()).also { created ->
      created.setCallback(object : MqttCallbackExtended {
        override fun connectionLost(cause: Throwable?) { disconnectedListener(null) }
        override fun messageArrived(topic: String, message: MqttMessage) { onMessage(topic.toMessageData(message)) }
        override fun deliveryComplete(token: IMqttDeliveryToken) = Unit
        override fun connectComplete(reconnect: Boolean, serverURI: String) {
          if (reconnect) reconnecting3?.complete(MqttConnAck(true, 0))
        }
      })
      mqtt3 = created
    }
    if (!client.isConnected && reconnecting3 == null && connected3) {
      CompletableFuture<MqttConnAck>().also { waiter ->
        reconnecting3 = waiter
        try {
          client.reconnect()
          CompletableFuture.runAsync { awaitReconnect(client.isConnected) { client.isConnected } }
            .whenComplete { _, error -> if (error != null) waiter.completeExceptionally(error) else waiter.complete(MqttConnAck(true, 0)) }
        } catch (error: Throwable) {
          reconnecting3 = null
          waiter.completeExceptionally(error)
        }
      }.whenComplete { _, _ -> reconnecting3 = null }
    } else {
      val connect = MqttConnectOptions().apply {
      mqttVersion = MqttConnectOptions.MQTT_VERSION_3_1_1; isCleanSession = options.cleanSession; keepAliveInterval = options.keepAliveSeconds; connectionTimeout = options.connectTimeoutSeconds(); isAutomaticReconnect = false
      options.username?.let { userName = it }; options.password?.let { password = it.toCharArray() }; socketFactory()?.let { setSocketFactory(it) }; hostnameVerifier()?.let { setSSLHostnameVerifier(it) }; setHttpsHostnameVerificationEnabled(!options.tlsInsecure && !options.tlsAllowInvalidHostnameForTesting)
      options.willTopic?.let { setWill(it, decodeStrictBase64(options.willPayloadBase64!!), options.willQos, options.willRetained) }
      }
      val connectToken = client.connect(connect)
      tokenFuture(connectToken, connectToken).thenApply { token ->
        connected3 = true
        MqttConnAck(token.getSessionPresent())
      }
    }
  } catch (error: Throwable) { failed("ERR_CONNECT", error.message ?: "Paho MQTT 3.1.1 connect failed") }

  private fun disconnect3(client: MqttAsyncClient): CompletableFuture<Void> = CompletableFuture.supplyAsync {
    try {
      client.disconnect(1000).waitForCompletion(2000)
    } catch (_: Throwable) {
      runCatching { client.disconnectForcibly() }
    } finally {
      runCatching { client.close() }
      if (mqtt3 === client) mqtt3 = null
    }
    null
  }

  private fun connect5(): CompletableFuture<MqttConnAck> = try {
    val client = mqtt5 ?: Mqtt5AsyncClient(uri(), options.clientId, Mqtt5MemoryPersistence()).also { created ->
      created.setCallback(object : Mqtt5Callback {
        override fun disconnected(response: MqttDisconnectResponse) { disconnectedListener(MqttDisconnect(response.returnCode, response.reasonString)) }
        override fun mqttErrorOccurred(exception: MqttException) = Unit
        override fun messageArrived(topic: String, message: Mqtt5Message) { onMessage(topic.toMessageData(message)) }
        override fun deliveryComplete(token: org.eclipse.paho.mqttv5.client.IMqttToken) = Unit
        override fun connectComplete(reconnect: Boolean, serverURI: String) {
          if (reconnect) reconnecting5?.complete(MqttConnAck(true, 0))
        }
        override fun authPacketArrived(reasonCode: Int, properties: MqttProperties) = Unit
      })
      mqtt5 = created
    }
    if (!client.isConnected && reconnecting5 == null && connected5) {
      CompletableFuture<MqttConnAck>().also { waiter ->
        reconnecting5 = waiter
        try {
          client.reconnect()
          CompletableFuture.runAsync { awaitReconnect(client.isConnected) { client.isConnected } }
            .whenComplete { _, error -> if (error != null) waiter.completeExceptionally(error) else waiter.complete(MqttConnAck(true, 0)) }
        } catch (error: Throwable) {
          reconnecting5 = null
          waiter.completeExceptionally(error)
        }
      }.whenComplete { _, _ -> reconnecting5 = null }
    } else {
      val connect = MqttConnectionOptions().apply {
      setCleanStart(options.cleanStart); keepAliveInterval = options.keepAliveSeconds; connectionTimeout = options.connectTimeoutSeconds(); isAutomaticReconnect = false; setSessionExpiryInterval(options.sessionExpiryIntervalSeconds); setReceiveMaximum(options.receiveMaximum)
      options.username?.let { userName = it }; options.password?.let { password = it.toByteArray(Charsets.UTF_8) }; socketFactory()?.let { setSocketFactory(it) }; hostnameVerifier()?.let { setSSLHostnameVerifier(it) }; setHttpsHostnameVerificationEnabled(!options.tlsInsecure && !options.tlsAllowInvalidHostnameForTesting)
      options.userProperties.takeIf { it.isNotEmpty() }?.let { userProperties = it.map { p -> UserProperty(p.first, p.second) } }
      options.willTopic?.let { setWill(it, Mqtt5Message(decodeStrictBase64(options.willPayloadBase64!!), options.willQos, options.willRetained, MqttProperties())) }
      }
      val connectToken = client.connect(connect)
      tokenFuture(connectToken, connectToken).thenApply { token ->
        ensureV5Success(token)
        connected5 = true
        val properties = token.getResponseProperties()
        MqttConnAck(token.getSessionPresent(), token.getReasonCodes()?.firstOrNull() ?: 0, properties?.getReasonString(), properties?.getAssignedClientIdentifier(), properties?.getUserProperties()?.map { mapOf("key" to it.key, "value" to it.value) } ?: emptyList())
      }
    }
  } catch (error: Throwable) { failed("ERR_CONNECT", error.message ?: "Paho MQTT 5 connect failed") }

  private fun disconnect5(client: Mqtt5AsyncClient): CompletableFuture<Void> = CompletableFuture.supplyAsync {
    try {
      client.disconnect(1000).waitForCompletion(2000)
    } catch (_: Throwable) {
      runCatching { client.disconnectForcibly() }
    } finally {
      runCatching { client.close() }
      if (mqtt5 === client) mqtt5 = null
    }
    null
  }

  private fun uri() = "${when (options.transport) {
    "tls" -> "ssl"
    "ws", "wss" -> options.transport
    else -> "tcp"
  }}://${options.host}:${options.port}${if (options.transport == "ws" || options.transport == "wss") options.path else ""}"
  private fun awaitReconnect(initiallyConnected: Boolean, connected: () -> Boolean) {
    if (initiallyConnected) return
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(options.connectTimeoutSeconds().toLong())
    while (!connected()) {
      if (System.nanoTime() >= deadline) throw TimeoutException("Paho reconnect timed out")
      Thread.sleep(10)
    }
  }
  private fun <T> tokenFuture(token: org.eclipse.paho.mqttv5.client.IMqttToken, value: T): CompletableFuture<T> = CompletableFuture.supplyAsync { token.waitForCompletion(); value }
  private fun ensureV5Success(token: org.eclipse.paho.mqttv5.client.IMqttToken) {
    token.getException()?.let { throw it }
    val codes = token.getReasonCodes()?.takeIf { it.isNotEmpty() } ?: when (val response = token.getResponse()) {
      is PahoConnAck -> intArrayOf(response.returnCode)
      is MqttPubAck -> intArrayOf(response.returnCode)
      is MqttPubRec -> intArrayOf(response.returnCode)
      is MqttPubComp -> intArrayOf(response.returnCode)
      is MqttSubAck -> response.returnCodes
      is MqttUnsubAck -> response.returnCodes
      else -> null
    }
    codes?.firstOrNull { it >= 0x80 }?.let { throw MqttException(it) }
  }
  private fun <T> tokenFuture(token: org.eclipse.paho.client.mqttv3.IMqttToken, value: T): CompletableFuture<T> = CompletableFuture.supplyAsync { token.waitForCompletion(); value }
  private fun <T> failed(code: String, message: String): CompletableFuture<T> = CompletableFuture<T>().also { it.completeExceptionally(IllegalStateException("$code: $message")) }
  private fun ParsedMqttOptions.connectTimeoutSeconds() = (connectTimeoutMs / 1000).coerceAtLeast(1).coerceAtMost(Int.MAX_VALUE.toLong()).toInt()
  private fun hostnameVerifier(): HostnameVerifier? = when {
    options.tlsInsecure -> HostnameVerifier { _, _ -> true }
    options.tlsAllowInvalidHostnameForTesting -> scopedMosquittoHostnameVerifier(options.host)
    else -> null
  }
  private fun socketFactory(): SocketFactory? = if (options.transport == "tls" || options.transport == "wss") sslContext().socketFactory else null

  private fun sslContext(): SSLContext = SSLContext.getInstance("TLS").also { context ->
    val trust = if (options.tlsInsecure && options.tlsCaBase64 == null) arrayOf<TrustManager>(object : X509TrustManager { override fun getAcceptedIssuers() = emptyArray<X509Certificate>(); override fun checkClientTrusted(chain: Array<X509Certificate>, authType: String) = Unit; override fun checkServerTrusted(chain: Array<X509Certificate>, authType: String) = Unit }) else TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm()).also { factory ->
      if (options.tlsCaBase64 == null) {
        factory.init(null as KeyStore?)
      } else {
        // Use the same bundled provider as the client-certificate path. Android's
        // platform PKIX implementation can ignore a certificate-only PKCS12 store
        // on some API/provider combinations, leaving no trust anchor at runtime.
        val store = KeyStore.getInstance("BKS", BouncyCastleProvider()).apply { load(null, null) }
        store.setCertificateEntry("react-native-mqtt-ca", CertificateFactory.getInstance("X.509").generateCertificate(ByteArrayInputStream(decodeTlsBase64(options.tlsCaBase64))))
        factory.init(store)
      }
    }.trustManagers
    val key = options.tlsP12Base64?.let { encoded ->
      val store = KeyStore.getInstance("PKCS12", BouncyCastleProvider()).apply { load(ByteArrayInputStream(decodeTlsBase64(encoded)), options.tlsP12Password?.toCharArray()) }
      KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm()).also { it.init(store, options.tlsP12Password?.toCharArray()) }.keyManagers
    }
    context.init(key, trust, null)
  }

  private fun MqttPublishProperties.toPaho() = MqttProperties().also { p ->
    p.userProperties = userProperties.map { UserProperty(it.first, it.second) }; contentType?.let { p.contentType = it }; responseTopic?.let { p.responseTopic = it }; correlationData?.let { p.correlationData = it }; messageExpiryIntervalSeconds?.let { p.messageExpiryInterval = it }; payloadFormatIndicator?.let { p.payloadFormat = it != 0 }
  }
  private fun String.toMessageData(message: Mqtt5Message) = MqttMessageData(this, message.payload, message.qos, message.isRetained, message.properties.userProperties.map { mapOf("key" to it.key, "value" to it.value) }, message.properties.contentType, message.properties.responseTopic, message.properties.correlationData, message.properties.messageExpiryInterval, if (message.properties.getPayloadFormat()) 1 else 0)
  private fun String.toMessageData(message: MqttMessage) = MqttMessageData(this, message.payload, message.qos, message.isRetained)
  private fun ByteBuffer.toByteArray() = duplicate().let { copy -> ByteArray(copy.remaining()).also(copy::get) }
}
