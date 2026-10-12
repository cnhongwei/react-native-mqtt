package com.cnhongwei.reactnativemqtt

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Assume.assumeTrue
import org.junit.Test
import java.util.Base64
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.net.ServerSocket
import java.net.Socket
import kotlin.concurrent.thread

private class InProcessRetransmissionProxy(private val targetPort: Int) : AutoCloseable {
  private val server = ServerSocket(0)
  private val accepted = mutableListOf<Socket>()
  private val publishPackets = mutableListOf<ByteArray>()
  private val acceptThread = thread(start = true, name = "mqtt-retransmission-proxy") {
    while (!server.isClosed) {
      try {
        val client = server.accept()
        synchronized(accepted) { accepted += client }
        thread(start = true, name = "mqtt-retransmission-proxy-connection") {
          forwardConnection(client)
        }
      } catch (_: java.net.SocketException) {
        break
      }
    }
  }

  val port: Int get() = server.localPort

  fun publishedPackets(): List<ByteArray> = synchronized(publishPackets) { publishPackets.map { it.copyOf() } }

  private fun forwardConnection(client: Socket) {
    val upstream = Socket("127.0.0.1", targetPort)
    synchronized(accepted) { accepted += upstream }
    val firstConnection = synchronized(publishPackets) { publishPackets.isEmpty() }
    val clientToServer = thread(start = true, name = "mqtt-retransmission-proxy-client") {
      try {
        while (!client.isClosed && !upstream.isClosed) {
          val packet = readMqttPacket(client)
          if (packet[0].toInt() ushr 4 == 3) {
            synchronized(publishPackets) { publishPackets += packet }
            upstream.getOutputStream().write(packet)
            upstream.getOutputStream().flush()
            if (firstConnection) {
              Thread.sleep(150)
              client.close()
              upstream.close()
              break
            }
          } else {
            upstream.getOutputStream().write(packet)
            upstream.getOutputStream().flush()
          }
        }
      } catch (_: Exception) {
        runCatching { client.close() }
        runCatching { upstream.close() }
      }
    }
    try {
      while (!client.isClosed && !upstream.isClosed) {
        val packet = readMqttPacket(upstream)
        val packetType = packet[0].toInt() ushr 4
        if (firstConnection && packetType == 4) continue
        client.getOutputStream().write(packet)
        client.getOutputStream().flush()
      }
    } catch (_: Exception) {
      runCatching { client.close() }
      runCatching { upstream.close() }
    } finally {
      clientToServer.join(2_000)
      runCatching { client.close() }
      runCatching { upstream.close() }
    }
  }

  override fun close() {
    server.close()
    synchronized(accepted) { accepted.toList().forEach { runCatching { it.close() } } }
    acceptThread.join(2_000)
  }
}

private fun readMqttPacket(socket: Socket): ByteArray {
  val input = socket.getInputStream()
  val header = input.read().also { require(it >= 0) }
  val lengthBytes = mutableListOf<Int>()
  var multiplier = 1
  var remainingLength = 0
  do {
    val value = input.read().also { require(it >= 0) }
    lengthBytes += value
    remainingLength += (value and 0x7f) * multiplier
    multiplier *= 128
  } while ((value and 0x80) != 0)
  val body = ByteArray(remainingLength)
  var bodyOffset = 0
  while (bodyOffset < body.size) {
    val count = input.read(body, bodyOffset, body.size - bodyOffset)
    require(count > 0)
    bodyOffset += count
  }
  return byteArrayOf(header.toByte()) + lengthBytes.map { it.toByte() }.toByteArray() + body
}

private fun mqttPacketId(packet: ByteArray): Int {
  require(packet[0].toInt() ushr 4 == 3) { "packet is not PUBLISH" }
  var index = 1
  while ((packet[index].toInt() and 0x80) != 0) index += 1
  index += 1
  val topicLength = ((packet[index].toInt() and 0xff) shl 8) or (packet[index + 1].toInt() and 0xff)
  index += 2 + topicLength
  require(packet[0].toInt() and 0x06 != 0) { "PUBLISH packet has no packet id" }
  return ((packet[index].toInt() and 0xff) shl 8) or (packet[index + 1].toInt() and 0xff)
}

class PahoMqttTransportIntegrationTest {
  @Test
  fun mqtt5ConnectWritesConfiguredReceiveMaximumProperty() {
    val server = ServerSocket(0)
    val packet = java.util.concurrent.CompletableFuture<ByteArray>()
    val serverThread = thread(start = true, name = "mqtt5-connect-inspector") {
      server.use { listener ->
        listener.accept().use { socket ->
          val connect = readMqttPacket(socket)
          packet.complete(connect)
          socket.getOutputStream().write(byteArrayOf(0x20, 0x03, 0x00, 0x00, 0x00))
          socket.getOutputStream().flush()
        }
      }
    }
    val transport = PahoMqttTransport(
      ParsedMqttOptions(
        host = "127.0.0.1",
        port = server.localPort,
        protocolVersion = 5,
        transport = "tcp",
        clientId = "rn-wire-${System.currentTimeMillis()}",
        receiveMaximum = 17,
      ),
    )
    try {
      assertEquals(0, transport.connect().get(10, TimeUnit.SECONDS).reasonCode)
      val connectPacket = packet.get(10, TimeUnit.SECONDS)
      assertEquals(17, mqtt5ConnectProperty(connectPacket, 0x21))
    } finally {
      runCatching { transport.disconnect().get(2, TimeUnit.SECONDS) }
      serverThread.join(2_000)
    }
  }

  @Test
  fun controlledMosquittoRejectsInvalidCredentials() {
    assumeTrue(System.getenv("RUN_MQTT_LOCAL_TESTS") == "1")
    val transport = PahoMqttTransport(
      ParsedMqttOptions(
        host = System.getenv("MQTT_TEST_HOST") ?: "127.0.0.1",
        port = 1885,
        protocolVersion = 5,
        transport = "tcp",
        clientId = "rn-invalid-auth-${System.currentTimeMillis()}",
        username = "test",
        password = "wrong",
      )
    )
    val failure = runCatching { transport.connect().get(10, TimeUnit.SECONDS) }.exceptionOrNull()
    assertTrue("invalid credentials must reject the connection", failure != null)
  }

  @Test
  fun controlledMosquittoAcceptsDeterministicCredentials() {
    assumeTrue(System.getenv("RUN_MQTT_LOCAL_TESTS") == "1")
    val transport = PahoMqttTransport(
      ParsedMqttOptions(
        host = System.getenv("MQTT_TEST_HOST") ?: "127.0.0.1",
        port = 1884,
        protocolVersion = 5,
        transport = "tcp",
        clientId = "rn-valid-auth-${System.currentTimeMillis()}",
        username = "test",
        password = "test",
      )
    )
    assertEquals(0, transport.connect().get(10, TimeUnit.SECONDS).reasonCode)
    transport.disconnect().get(10, TimeUnit.SECONDS)
  }

  @Test
  fun controlledMosquittoWebSocketRoundTripPreservesBinaryPayload() {
    assumeTrue(System.getenv("RUN_MQTT_LOCAL_TESTS") == "1")
    val topic = "rn/ws/${System.currentTimeMillis()}"
    val received = CountDownLatch(1)
    var payload: ByteArray? = null
    val transport = PahoMqttTransport(
      ParsedMqttOptions(
        host = System.getenv("MQTT_TEST_HOST") ?: "127.0.0.1",
        port = 8080,
        protocolVersion = 5,
        transport = "ws",
        path = "/mqtt",
        clientId = "rn-ws-${System.currentTimeMillis()}",
      )
    ) { message ->
      payload = message.payload
      received.countDown()
    }
    assertEquals(0, transport.connect().get(10, TimeUnit.SECONDS).reasonCode)
    try {
      assertEquals(1, transport.subscribe(topic, 1, false, false, 0).get(10, TimeUnit.SECONDS).grantedQos)
      val expected = byteArrayOf(0, 127, -1, 7)
      transport.publish(topic, expected, 1, false).get(10, TimeUnit.SECONDS)
      assertTrue("WebSocket listener must deliver the published payload", received.await(10, TimeUnit.SECONDS))
      assertEquals(expected.toList(), payload!!.toList())
    } finally {
      transport.disconnect().get(10, TimeUnit.SECONDS)
    }
  }

  @Test
  fun controlledMosquittoPreservesSessionAndRetainedMessage() {
    assumeTrue(System.getenv("RUN_MQTT_LOCAL_TESTS") == "1")
    val suffix = "${System.currentTimeMillis()}"
    val sessionOptions = ParsedMqttOptions(
      host = System.getenv("MQTT_TEST_HOST") ?: "127.0.0.1",
      port = 1883,
      protocolVersion = 5,
      transport = "tcp",
      clientId = "rn-session-$suffix",
      cleanStart = true,
      sessionExpiryIntervalSeconds = 60,
    )
    val first = PahoMqttTransport(sessionOptions)
    assertEquals(0, first.connect().get(10, TimeUnit.SECONDS).reasonCode)
    assertEquals(1, first.subscribe("rn/session/$suffix", 1, false, false, 0).get(10, TimeUnit.SECONDS).grantedQos)
    first.disconnect().get(10, TimeUnit.SECONDS)

    val resumed = PahoMqttTransport(sessionOptions.copy(cleanStart = false))
    assertEquals(true, resumed.connect().get(10, TimeUnit.SECONDS).sessionPresent)
    resumed.disconnect().get(10, TimeUnit.SECONDS)

    val retainedTopic = "rn/retained/$suffix"
    val publisher = PahoMqttTransport(sessionOptions.copy(clientId = "rn-publisher-$suffix", cleanStart = true))
    publisher.connect().get(10, TimeUnit.SECONDS)
    publisher.publish(retainedTopic, byteArrayOf(0, 255.toByte(), 7), 1, true).get(10, TimeUnit.SECONDS)
    publisher.disconnect().get(10, TimeUnit.SECONDS)

    val received = CountDownLatch(1)
    var retainedPayload: ByteArray? = null
    val subscriber = PahoMqttTransport(sessionOptions.copy(clientId = "rn-subscriber-$suffix")) {
      retainedPayload = it.payload
      received.countDown()
    }
    subscriber.connect().get(10, TimeUnit.SECONDS)
    subscriber.subscribe(retainedTopic, 1, false, false, 0).get(10, TimeUnit.SECONDS)
    assertEquals(true, received.await(10, TimeUnit.SECONDS))
    assertEquals(listOf<Byte>(0, -1, 7), retainedPayload!!.toList())
    subscriber.disconnect().get(10, TimeUnit.SECONDS)
  }

  @Test
  fun controlledMosquittoPublishesMqtt5WillAfterAbruptClientClose() {
    assumeTrue(System.getenv("RUN_MQTT_LOCAL_TESTS") == "1")
    val suffix = "${System.currentTimeMillis()}"
    val willTopic = "rn/will/$suffix"
    val received = CountDownLatch(1)
    var willPayload: ByteArray? = null
    val subscriber = PahoMqttTransport(
      ParsedMqttOptions(
        host = System.getenv("MQTT_TEST_HOST") ?: "127.0.0.1",
        port = 1883,
        protocolVersion = 5,
        transport = "tcp",
        clientId = "rn-will-subscriber-$suffix",
      ),
    ) {
      if (it.topic == willTopic) {
        willPayload = it.payload
        received.countDown()
      }
    }
    val willClient = PahoMqttTransport(
      ParsedMqttOptions(
        host = System.getenv("MQTT_TEST_HOST") ?: "127.0.0.1",
        port = 2891,
        protocolVersion = 5,
        transport = "tcp",
        clientId = "rn-will-client-$suffix",
        willTopic = willTopic,
        willPayloadBase64 = Base64.getEncoder().encodeToString(byteArrayOf(0, 127, -1)),
        willQos = 1,
        willRetained = false,
      ),
    )
    assertEquals(0, subscriber.connect().get(10, TimeUnit.SECONDS).reasonCode)
    try {
      assertEquals(1, subscriber.subscribe(willTopic, 1, false, false, 0).get(10, TimeUnit.SECONDS).grantedQos)
      assertEquals(0, willClient.connect().get(10, TimeUnit.SECONDS).reasonCode)
      assertTrue("abrupt close must cause the broker to publish the MQTT 5 Will", received.await(10, TimeUnit.SECONDS))
      assertEquals(listOf<Byte>(0, 127, -1), willPayload!!.toList())
    } finally {
      runCatching { willClient.disconnect().get(2, TimeUnit.SECONDS) }
      runCatching { subscriber.disconnect().get(10, TimeUnit.SECONDS) }
    }
  }

  @Test
  fun controlledDelayedProxyDelaysBrokerCompletion() {
    assumeTrue(System.getenv("RUN_MQTT_LOCAL_TESTS") == "1")
    val startedAt = System.nanoTime()
    val transport = PahoMqttTransport(
      ParsedMqttOptions(
        host = System.getenv("MQTT_TEST_HOST") ?: "127.0.0.1",
        port = 2883,
        protocolVersion = 5,
        transport = "tcp",
        clientId = "rn-delayed-${System.currentTimeMillis()}",
      )
    )
    assertEquals(0, transport.connect().get(10, TimeUnit.SECONDS).reasonCode)
    val elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt)
    assertTrue("local proxy must delay broker completion", elapsedMillis >= 200)
    transport.disconnect().get(10, TimeUnit.SECONDS)
  }

  @Test
  fun controlledAckDropProxyLeavesSubscribeUnsettled() {
    assumeTrue(System.getenv("RUN_MQTT_LOCAL_TESTS") == "1")
    val transport = PahoMqttTransport(
      ParsedMqttOptions(
        host = System.getenv("MQTT_TEST_HOST") ?: "127.0.0.1",
        port = 2884,
        protocolVersion = 5,
        transport = "tcp",
        clientId = "rn-ack-drop-${System.currentTimeMillis()}",
      )
    )
    assertEquals(0, transport.connect().get(10, TimeUnit.SECONDS).reasonCode)
    var timedOut = false
    try {
      transport.subscribe("rn/missing-suback", 1, false, false, 0).get(1, TimeUnit.SECONDS)
    } catch (_: java.util.concurrent.TimeoutException) {
      timedOut = true
    }
    assertTrue("dropped SUBACK must not resolve as success", timedOut)
    transport.disconnect().get(10, TimeUnit.SECONDS)
  }

  @Test
  fun controlledNegativeSubackIsReportedAsRejected() {
    assumeTrue(System.getenv("RUN_MQTT_LOCAL_TESTS") == "1")
    val transport = PahoMqttTransport(
      ParsedMqttOptions(
        host = System.getenv("MQTT_TEST_HOST") ?: "127.0.0.1",
        port = 2885,
        protocolVersion = 5,
        transport = "tcp",
        clientId = "rn-negative-suback-${System.currentTimeMillis()}",
      )
    )
    assertEquals(0, transport.connect().get(10, TimeUnit.SECONDS).reasonCode)
    try {
      val failure = runCatching {
        transport.subscribe("rn/negative-suback", 1, false, false, 0).get(10, TimeUnit.SECONDS)
      }.exceptionOrNull()
      assertTrue("negative SUBACK must reject the operation", failure != null)
      val subAck = failure?.cause as? org.eclipse.paho.mqttv5.common.MqttException
      assertTrue("failure must preserve the MQTT 5 SUBACK", subAck != null)
      assertEquals(0x87, subAck!!.reasonCode)
    } finally {
      // Paho closes the client after a negative SUBACK; cleanup is therefore
      // allowed to observe the already-closed transport.
      runCatching { transport.disconnect().get(10, TimeUnit.SECONDS) }
    }
  }

  @Test
  fun controlledNegativePubackIsReportedAsRejected() {
    assumeTrue(System.getenv("RUN_MQTT_LOCAL_TESTS") == "1")
    val transport = PahoMqttTransport(
      ParsedMqttOptions(
        host = System.getenv("MQTT_TEST_HOST") ?: "127.0.0.1",
        port = 2886,
        protocolVersion = 5,
        transport = "tcp",
        clientId = "rn-negative-puback-${System.currentTimeMillis()}",
      )
    )
    assertEquals(0, transport.connect().get(10, TimeUnit.SECONDS).reasonCode)
    try {
      val failure = runCatching {
        transport.publish("rn/negative-puback", byteArrayOf(1), 1, false).get(10, TimeUnit.SECONDS)
      }.exceptionOrNull()
      assertTrue("negative PUBACK must reject the operation", failure != null)
      val pubAck = failure?.cause as? org.eclipse.paho.mqttv5.common.MqttException
      assertTrue("failure must preserve the MQTT 5 PUBACK", pubAck != null)
      assertEquals(0x87, pubAck!!.reasonCode)
    } finally {
      runCatching { transport.disconnect().get(10, TimeUnit.SECONDS) }
    }
  }

  @Test
  fun controlledNegativeUnsubackIsReportedAsRejected() {
    assumeTrue(System.getenv("RUN_MQTT_LOCAL_TESTS") == "1")
    val transport = PahoMqttTransport(
      ParsedMqttOptions(
        host = System.getenv("MQTT_TEST_HOST") ?: "127.0.0.1",
        port = 2887,
        protocolVersion = 5,
        transport = "tcp",
        clientId = "rn-negative-unsuback-${System.currentTimeMillis()}",
      )
    )
    assertEquals(0, transport.connect().get(10, TimeUnit.SECONDS).reasonCode)
    try {
      transport.subscribe("rn/negative-unsuback", 1, false, false, 0).get(10, TimeUnit.SECONDS)
      val failure = runCatching {
        transport.unsubscribe("rn/negative-unsuback").get(10, TimeUnit.SECONDS)
      }.exceptionOrNull()
      assertTrue("negative UNSUBACK must reject the operation", failure != null)
      val unsubAck = failure?.cause as? org.eclipse.paho.mqttv5.common.MqttException
      assertTrue("failure must preserve the MQTT 5 UNSUBACK", unsubAck != null)
      assertEquals(0x87, unsubAck!!.reasonCode)
    } finally {
      runCatching { transport.disconnect().get(10, TimeUnit.SECONDS) }
    }
  }

  @Test
  fun controlledNegativePubrecIsReportedAsRejected() {
    assumeTrue(System.getenv("RUN_MQTT_LOCAL_TESTS") == "1")
    val transport = PahoMqttTransport(
      ParsedMqttOptions(
        host = System.getenv("MQTT_TEST_HOST") ?: "127.0.0.1",
        port = 2888,
        protocolVersion = 5,
        transport = "tcp",
        clientId = "rn-negative-pubrec-${System.currentTimeMillis()}",
      )
    )
    assertEquals(0, transport.connect().get(10, TimeUnit.SECONDS).reasonCode)
    try {
      val failure = runCatching {
        transport.publish("rn/negative-pubrec", byteArrayOf(1), 2, false).get(10, TimeUnit.SECONDS)
      }.exceptionOrNull()
      assertTrue("negative PUBREC must reject the operation", failure != null)
      val pubRec = failure?.cause as? org.eclipse.paho.mqttv5.common.MqttException
      assertTrue("failure must preserve the MQTT 5 PUBREC", pubRec != null)
      assertEquals(0x87, pubRec!!.reasonCode)
    } finally {
      runCatching { transport.disconnect().get(10, TimeUnit.SECONDS) }
    }
  }

  @Test
  fun controlledNegativePubcompCannotResolveAsSuccess() {
    assumeTrue(System.getenv("RUN_MQTT_LOCAL_TESTS") == "1")
    val transport = PahoMqttTransport(
      ParsedMqttOptions(
        host = System.getenv("MQTT_TEST_HOST") ?: "127.0.0.1",
        port = 2890,
        protocolVersion = 5,
        transport = "tcp",
        clientId = "rn-negative-pubcomp-${System.currentTimeMillis()}",
      )
    )
    assertEquals(0, transport.connect().get(10, TimeUnit.SECONDS).reasonCode)
    try {
      val failure = runCatching {
        transport.publish("rn/negative-pubcomp", byteArrayOf(1), 2, false).get(10, TimeUnit.SECONDS)
      }.exceptionOrNull()
      assertTrue("negative PUBCOMP must not resolve as success", failure != null)
      assertTrue("Paho must reject a negative PUBCOMP", failure != null)
    } finally {
      runCatching { transport.disconnect().get(10, TimeUnit.SECONDS) }
    }
  }

  @Test
  fun controlledQos2PublishRemainsPendingAfterPubrecUntilPubcomp() {
    assumeTrue(System.getenv("RUN_MQTT_LOCAL_TESTS") == "1")
    val transport = PahoMqttTransport(
      ParsedMqttOptions(
        host = System.getenv("MQTT_TEST_HOST") ?: "127.0.0.1",
        port = 2892,
        protocolVersion = 5,
        transport = "tcp",
        clientId = "rn-qos2-pubcomp-${System.currentTimeMillis()}",
      )
    )
    assertEquals(0, transport.connect().get(10, TimeUnit.SECONDS).reasonCode)
    try {
      val startedAt = System.nanoTime()
      val publish = transport.publish("rn/qos2-pubcomp", byteArrayOf(1, 2, 3), 2, false)
      Thread.sleep(150)
      assertTrue("QoS 2 publish must remain pending before delayed PUBCOMP", !publish.isDone)
      publish.get(10, TimeUnit.SECONDS)
      assertTrue("PUBCOMP delay must be observable", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt) >= 400)
    } finally {
      runCatching { transport.disconnect().get(10, TimeUnit.SECONDS) }
    }
  }

  @Test
  fun controlledQos2DuplicatePubcompSettlesPublishOnce() {
    assumeTrue(System.getenv("RUN_MQTT_LOCAL_TESTS") == "1")
    val transport = PahoMqttTransport(
      ParsedMqttOptions(
        host = System.getenv("MQTT_TEST_HOST") ?: "127.0.0.1",
        port = 2893,
        protocolVersion = 5,
        transport = "tcp",
        clientId = "rn-qos2-duplicate-pubcomp-${System.currentTimeMillis()}",
      )
    )
    assertEquals(0, transport.connect().get(10, TimeUnit.SECONDS).reasonCode)
    try {
      val publish = transport.publish("rn/qos2-duplicate-pubcomp", byteArrayOf(4, 5, 6), 2, false)
      publish.get(10, TimeUnit.SECONDS)
      assertTrue("duplicate PUBCOMP must not turn a successful publish into a failure", publish.isDone && !publish.isCompletedExceptionally)
    } finally {
      runCatching { transport.disconnect().get(10, TimeUnit.SECONDS) }
    }
  }

  @Test
  fun controlledQos2MissingPubcompDoesNotResolvePublish() {
    assumeTrue(System.getenv("RUN_MQTT_LOCAL_TESTS") == "1")
    val transport = PahoMqttTransport(
      ParsedMqttOptions(
        host = System.getenv("MQTT_TEST_HOST") ?: "127.0.0.1",
        port = 2894,
        protocolVersion = 5,
        transport = "tcp",
        clientId = "rn-qos2-missing-pubcomp-${System.currentTimeMillis()}",
      )
    )
    assertEquals(0, transport.connect().get(10, TimeUnit.SECONDS).reasonCode)
    try {
      val publish = transport.publish("rn/qos2-missing-pubcomp", byteArrayOf(7, 8, 9), 2, false)
      try {
        publish.get(1, TimeUnit.SECONDS)
        fail("missing PUBCOMP must not resolve publish")
      } catch (_: java.util.concurrent.TimeoutException) {
        // Expected: PUBREC was accepted but PUBCOMP never arrived.
      }
      assertTrue("missing PUBCOMP must leave publish unsettled", !publish.isDone)
    } finally {
      transport.disconnect().get(10, TimeUnit.SECONDS)
    }
  }

  @Test
  fun controlledQos2DisconnectAfterPubrecDoesNotResolvePublish() {
    assumeTrue(System.getenv("RUN_MQTT_LOCAL_TESTS") == "1")
    val transport = PahoMqttTransport(
      ParsedMqttOptions(
        host = System.getenv("MQTT_TEST_HOST") ?: "127.0.0.1",
        port = 2895,
        protocolVersion = 5,
        transport = "tcp",
        clientId = "rn-qos2-disconnect-pubrec-${System.currentTimeMillis()}",
      )
    )
    assertEquals(0, transport.connect().get(10, TimeUnit.SECONDS).reasonCode)
    try {
      val publish = transport.publish("rn/qos2-disconnect-pubrec", byteArrayOf(10, 11), 2, false)
      val failure = runCatching { publish.get(10, TimeUnit.SECONDS) }.exceptionOrNull()
      assertTrue("disconnect after PUBREC must not resolve QoS 2 publish", failure != null)
      assertTrue("disconnect path must settle the publish future once", publish.isCompletedExceptionally)
    } finally {
      runCatching { transport.disconnect().get(10, TimeUnit.SECONDS) }
    }
  }

  @Test
  fun controlledMosquittoStopProducesEstablishedDisconnect() {
    assumeTrue(System.getenv("RUN_MQTT_LOCAL_TESTS") == "1")
    val disconnected = CountDownLatch(1)
    val callbacks = java.util.concurrent.atomic.AtomicInteger(0)
    val transport = PahoMqttTransport(
      ParsedMqttOptions(
        host = System.getenv("MQTT_TEST_HOST") ?: "127.0.0.1",
        port = 1883,
        protocolVersion = 5,
        transport = "tcp",
        clientId = "rn-network-down-${System.currentTimeMillis()}",
        reconnectEnabled = false,
      )
    )
    transport.setDisconnectedListener {
      callbacks.incrementAndGet()
      disconnected.countDown()
    }
    assertEquals(0, transport.connect().get(10, TimeUnit.SECONDS).reasonCode)
    val composeFile = System.getenv("MQTT_COMPOSE_FILE")
      ?: throw IllegalStateException("MQTT_COMPOSE_FILE is required for network-down injection")
    try {
      ProcessBuilder("docker", "compose", "-f", composeFile, "stop", "mosquitto")
        .inheritIO()
        .start()
        .also { assertTrue("Mosquitto stop command failed", it.waitFor(10, TimeUnit.SECONDS)) }
      assertTrue("stopping Mosquitto must notify an established disconnect", disconnected.await(10, TimeUnit.SECONDS))
      assertEquals(1, callbacks.get())
    } finally {
      ProcessBuilder("docker", "compose", "-f", composeFile, "start", "mosquitto")
        .inheritIO()
        .start()
        .also { assertTrue("Mosquitto start command failed", it.waitFor(10, TimeUnit.SECONDS)) }
    }
  }

  @Test
  fun samePahoTransportReconnectRestoresPersistentSubscriptionWithoutJsReplay() {
    assumeTrue(System.getenv("RUN_MQTT_LOCAL_TESTS") == "1")
    val composeFile = System.getenv("MQTT_COMPOSE_FILE")
      ?: throw IllegalStateException("MQTT_COMPOSE_FILE is required for reconnect injection")
    val suffix = System.currentTimeMillis()
    val topic = "rn/paho-reconnect/$suffix"
    val disconnected = CountDownLatch(1)
    val received = CountDownLatch(1)
    var receivedPayload: ByteArray? = null
    val transport = PahoMqttTransport(
      ParsedMqttOptions(
        host = System.getenv("MQTT_TEST_HOST") ?: "127.0.0.1",
        port = 1883,
        protocolVersion = 5,
        transport = "tcp",
        clientId = "rn-paho-reconnect-$suffix",
        cleanStart = false,
        sessionExpiryIntervalSeconds = 60,
      )
    ) { message ->
      receivedPayload = message.payload
      received.countDown()
    }
    transport.setDisconnectedListener { disconnected.countDown() }
    try {
      assertEquals(0, transport.connect().get(10, TimeUnit.SECONDS).reasonCode)
      assertEquals(1, transport.subscribe(topic, 1, false, false, 0).get(10, TimeUnit.SECONDS).grantedQos)
      ProcessBuilder("docker", "compose", "-f", composeFile, "stop", "mosquitto")
        .inheritIO()
        .start()
        .also { assertTrue("Mosquitto stop command failed", it.waitFor(10, TimeUnit.SECONDS)) }
      assertTrue("Paho must report the established connection loss", disconnected.await(10, TimeUnit.SECONDS))
      ProcessBuilder("docker", "compose", "-f", composeFile, "start", "mosquitto")
        .inheritIO()
        .start()
        .also { assertTrue("Mosquitto start command failed", it.waitFor(10, TimeUnit.SECONDS)) }
      Thread.sleep(1_000)
      val resumed = transport.connect().get(10, TimeUnit.SECONDS)
      assertTrue("same-client reconnect must report the persistent session", resumed.sessionPresent)
      transport.publish(topic, byteArrayOf(21, 22, 23), 1, false).get(10, TimeUnit.SECONDS)
      assertTrue("restored Paho subscription must receive a message", received.await(10, TimeUnit.SECONDS))
      assertEquals(listOf<Byte>(21, 22, 23), receivedPayload!!.toList())
    } finally {
      runCatching { transport.disconnect().get(10, TimeUnit.SECONDS) }
      runCatching {
        ProcessBuilder("docker", "compose", "-f", composeFile, "start", "mosquitto")
          .inheritIO()
          .start()
          .waitFor(10, TimeUnit.SECONDS)
      }
    }
  }

  @Test
  fun pahoRetransmitsAnInFlightPublishAfterReconnectWithoutApplicationResubmit() {
    assumeTrue(System.getenv("RUN_MQTT_LOCAL_TESTS") == "1")
    val proxy = InProcessRetransmissionProxy((System.getenv("MQTT_TEST_PORT") ?: "1883").toInt())
    val disconnected = CountDownLatch(1)
    val suffix = System.currentTimeMillis()
    val transport = PahoMqttTransport(
      ParsedMqttOptions(
        host = "127.0.0.1",
        port = proxy.port,
        protocolVersion = 5,
        transport = "tcp",
        clientId = "rn-paho-retransmit-$suffix",
        cleanStart = false,
        sessionExpiryIntervalSeconds = 60,
      )
    )
    transport.setDisconnectedListener { disconnected.countDown() }
    try {
      assertEquals(0, transport.connect().get(10, TimeUnit.SECONDS).reasonCode)
      val publish = transport.publish("rn/paho-retransmit/$suffix", byteArrayOf(31, 32, 33), 1, false)
      assertTrue("Paho must report the first connection loss", disconnected.await(10, TimeUnit.SECONDS))
      assertEquals(0, transport.connect().get(10, TimeUnit.SECONDS).reasonCode)
      // The public operation may already have an unknown/failed outcome when
      // the socket closes. Protocol retransmission is verified independently
      // by the packet sequence below; the application must not resubmit it.
      runCatching { publish.get(10, TimeUnit.SECONDS) }
      Thread.sleep(500)

      val packets = proxy.publishedPackets()
      assertTrue("Paho must retransmit the in-flight publish", packets.size >= 2)
      val first = packets[0]
      val retransmitted = packets[1]
      assertTrue("retransmitted PUBLISH must set DUP", retransmitted[0].toInt() and 0x08 != 0)
      assertEquals("retransmitted PUBLISH must retain packet id", mqttPacketId(first), mqttPacketId(retransmitted))
    } finally {
      runCatching { transport.disconnect().get(10, TimeUnit.SECONDS) }
      proxy.close()
    }
  }

  @Test
  fun controlledServerDisconnectPreservesReasonCode() {
    assumeTrue(System.getenv("RUN_MQTT_LOCAL_TESTS") == "1")
    val disconnected = CountDownLatch(1)
    var reason: MqttDisconnect? = null
    val transport = PahoMqttTransport(
      ParsedMqttOptions(
        host = System.getenv("MQTT_TEST_HOST") ?: "127.0.0.1",
        port = 2889,
        protocolVersion = 5,
        transport = "tcp",
        clientId = "rn-server-disconnect-${System.currentTimeMillis()}",
      )
    )
    transport.setDisconnectedListener {
      reason = it
      disconnected.countDown()
    }
    assertEquals(0, transport.connect().get(10, TimeUnit.SECONDS).reasonCode)
    try {
      transport.subscribe("rn/server-disconnect", 1, false, false, 0).get(10, TimeUnit.SECONDS)
      assertTrue("server DISCONNECT must be delivered to the transport", disconnected.await(10, TimeUnit.SECONDS))
      assertEquals(135, reason?.reasonCode)
    } finally {
      runCatching { transport.disconnect().get(10, TimeUnit.SECONDS) }
    }
  }

  @Test
  fun rejectsCanonicalBase64ThatIsNotAnX509CertificateBeforeNetworkIo() {
    val client = PahoMqttTransport(
      ParsedMqttOptions(
        host = "127.0.0.1",
        port = 8883,
        protocolVersion = 5,
        transport = "tls",
        tlsCaBase64 = Base64.getEncoder().encodeToString("not-a-certificate".toByteArray()),
      ),
    )
    var rejected = false
    try {
      client.connect().get(2, TimeUnit.SECONDS)
    } catch (_: Throwable) {
      // CertificateFactory failure is expected before a socket is opened.
      rejected = true
    }
    assertEquals(true, rejected)
  }

  @Test
  fun controlledTlsAcceptsConfiguredCaAndRejectsMismatchedHostname() {
    assumeTrue(System.getenv("RUN_MQTT_TLS_FIXTURE_TESTS") == "1")
    val ca = System.getenv("MQTT_TLS_FIXTURE_CA_BASE64")
      ?: throw IllegalStateException("MQTT_TLS_FIXTURE_CA_BASE64 is required")
    val port = (System.getenv("MQTT_TLS_FIXTURE_PORT") ?: "8883").toInt()
    val matching = PahoMqttTransport(
      ParsedMqttOptions(
        host = System.getenv("MQTT_TLS_FIXTURE_HOST") ?: "localhost",
        port = port,
        protocolVersion = 5,
        transport = "tls",
        clientId = "rn-tls-ca-${System.currentTimeMillis()}",
        tlsCaBase64 = ca,
      ),
    )
    assertEquals(0, matching.connect().get(10, TimeUnit.SECONDS).reasonCode)
    matching.disconnect().get(10, TimeUnit.SECONDS)

    val mismatched = PahoMqttTransport(
      ParsedMqttOptions(
        host = System.getenv("MQTT_TLS_FIXTURE_BAD_HOST") ?: "127.0.0.1",
        port = port,
        protocolVersion = 5,
        transport = "tls",
        clientId = "rn-tls-hostname-${System.currentTimeMillis()}",
        tlsCaBase64 = ca,
      ),
    )
    val failure = runCatching { mismatched.connect().get(10, TimeUnit.SECONDS) }.exceptionOrNull()
    assertTrue("custom CA must not disable hostname verification", failure != null)
    runCatching { mismatched.disconnect().get(2, TimeUnit.SECONDS) }
  }

  @Test
  fun controlledTlsAcceptsPasswordProtectedPkcs12ClientIdentity() {
    assumeTrue(System.getenv("RUN_MQTT_TLS_FIXTURE_TESTS") == "1")
    val ca = System.getenv("MQTT_TLS_FIXTURE_CA_BASE64")
      ?: throw IllegalStateException("MQTT_TLS_FIXTURE_CA_BASE64 is required")
    val p12 = System.getenv("MQTT_TLS_FIXTURE_P12_BASE64")
      ?: throw IllegalStateException("MQTT_TLS_FIXTURE_P12_BASE64 is required")
    val password = System.getenv("MQTT_TLS_FIXTURE_P12_PASSWORD")
      ?: throw IllegalStateException("MQTT_TLS_FIXTURE_P12_PASSWORD is required")
    val transport = PahoMqttTransport(
      ParsedMqttOptions(
        host = System.getenv("MQTT_TLS_FIXTURE_HOST") ?: "localhost",
        port = (System.getenv("MQTT_TLS_FIXTURE_MTLS_PORT") ?: "8884").toInt(),
        protocolVersion = 5,
        transport = "tls",
        clientId = "rn-tls-mtls-${System.currentTimeMillis()}",
        tlsCaBase64 = ca,
        tlsP12Base64 = p12,
        tlsP12Password = password,
      ),
    )
    assertEquals(0, transport.connect().get(10, TimeUnit.SECONDS).reasonCode)
    transport.disconnect().get(10, TimeUnit.SECONDS)
  }

  @Test
  fun connectsToPublicMosquittoTlsAndWebSocketListeners() {
    assumeTrue(System.getenv("RUN_MQTT_PUBLIC_TESTS") == "1")
    val listeners = listOf(
      Pair(1883, "tcp"),
      Pair(8883, "tls"),
      Pair(8886, "tls"),
      Pair(8080, "ws"),
      Pair(8081, "wss"),
    )
    listeners.forEachIndexed { index, (port, transport) ->
      val client = PahoMqttTransport(
        ParsedMqttOptions(
          host = "test.mosquitto.org",
          port = port,
          protocolVersion = 5,
          transport = transport,
          path = "/mqtt",
          clientId = "rn-public-smoke-${System.currentTimeMillis()}-$index",
          keepAliveSeconds = 30,
          cleanSession = true,
          cleanStart = true,
          // The public test listener's certificate chain is not trusted by every
          // host JVM. Certificate/hostname validation is covered separately.
          tlsInsecure = transport == "tls" || transport == "wss",
        )
      )
      val ack = client.connect().get(20, TimeUnit.SECONDS)
      assertEquals("$transport listener should accept MQTT 5", 0, ack.reasonCode)
      client.disconnect().get(10, TimeUnit.SECONDS)
    }
  }

  @Test
  fun connectsToPublicMosquittoMqtt311WebSocketListeners() {
    assumeTrue(System.getenv("RUN_MQTT_PUBLIC_TESTS") == "1")
    listOf("ws", "wss").forEachIndexed { index, transport ->
      val client = PahoMqttTransport(
        ParsedMqttOptions(
          host = "test.mosquitto.org",
          port = if (transport == "ws") 8080 else 8081,
          protocolVersion = 4,
          transport = transport,
          path = "/mqtt",
          clientId = "rn-public-mqtt311-$transport-${System.currentTimeMillis()}-$index",
          keepAliveSeconds = 30,
          cleanSession = true,
          tlsInsecure = transport == "wss",
        )
      )
      val ack = client.connect().get(20, TimeUnit.SECONDS)
      // MQTT 3.1.1 has no CONNACK reason-code field in the public adapter DTO.
      assertEquals("$transport listener should accept MQTT 3.1.1", null, ack.reasonCode)
      client.disconnect().get(10, TimeUnit.SECONDS)
    }
  }

  @Test
  fun publicMosquittoExpiredCertificateIsRejectedByDefaultTrust() {
    assumeTrue(System.getenv("RUN_MQTT_PUBLIC_TESTS") == "1")
    val transport = PahoMqttTransport(
      ParsedMqttOptions(
        host = "test.mosquitto.org",
        port = 8887,
        protocolVersion = 5,
        transport = "tls",
        clientId = "rn-public-expired-${System.currentTimeMillis()}",
        keepAliveSeconds = 30,
        cleanStart = true,
        tlsInsecure = false,
      )
    )
    val failure = runCatching { transport.connect().get(20, TimeUnit.SECONDS) }.exceptionOrNull()
    assertTrue("expired-certificate listener must reject default TLS trust", failure != null)
  }

  @Test
  fun publicMosquittoAuthenticatedListenersUseExplicitEnvironmentCredentials() {
    assumeTrue(System.getenv("RUN_MQTT_PUBLIC_TESTS") == "1")
    val username = System.getenv("MQTT_PUBLIC_USERNAME")
    val password = System.getenv("MQTT_PUBLIC_PASSWORD")
    assumeTrue(!username.isNullOrEmpty() && password != null)
    val insecure = System.getenv("MQTT_PUBLIC_TLS_INSECURE") == "1"
    listOf(
      Triple(1884, "tcp", ""),
      Triple(8885, "tls", ""),
      Triple(8090, "ws", "/mqtt"),
      Triple(8091, "wss", "/mqtt"),
    ).forEachIndexed { index, (port, transport, path) ->
      val client = PahoMqttTransport(
        ParsedMqttOptions(
          host = "test.mosquitto.org",
          port = port,
          protocolVersion = 5,
          transport = transport,
          path = path,
          clientId = "rn-public-auth-${System.currentTimeMillis()}-$index",
          username = username,
          password = password,
          keepAliveSeconds = 30,
          cleanStart = true,
          tlsInsecure = insecure && (transport == "tls" || transport == "wss"),
        )
      )
      val ack = client.connect().get(20, TimeUnit.SECONDS)
      assertEquals("$transport authenticated listener should accept MQTT 5", 0, ack.reasonCode)
      client.disconnect().get(10, TimeUnit.SECONDS)
    }
  }

  @Test
  fun connectsToControlledMosquittoWithMqtt5AndPreservesConnAck() {
    assumeTrue(System.getenv("RUN_MQTT_LOCAL_TESTS") == "1")
    val options = ParsedMqttOptions(
      host = System.getenv("MQTT_TEST_HOST") ?: "127.0.0.1",
      port = (System.getenv("MQTT_TEST_PORT") ?: "1883").toInt(),
      protocolVersion = 5,
      transport = "tcp",
      clientId = "rn-jvm-integration",
      username = null,
      password = null,
      keepAliveSeconds = 30,
      cleanSession = true,
      cleanStart = true,
      sessionExpiryIntervalSeconds = 0,
      receiveMaximum = 1,
    )
    val received = CountDownLatch(1)
    var receivedMessage: MqttMessageData? = null
    val transport = PahoMqttTransport(options) {
      receivedMessage = it
      received.countDown()
    }
    val ack = transport.connect().get(10, TimeUnit.SECONDS)
    assertEquals(0, ack.reasonCode)
    assertEquals(false, ack.sessionPresent)
    val subscription = transport.subscribe("rn/integration", 1, false, false, 0).get(10, TimeUnit.SECONDS)
    assertEquals(1, subscription.grantedQos)
    assertEquals(1, subscription.reasonCode)
    transport.publish(
      "rn/integration",
      byteArrayOf(0, 1, 2, 127, -1),
      1,
      false,
      MqttPublishProperties(
        userProperties = listOf("trace" to "integration", "trace" to "second"),
        contentType = "application/octet-stream",
        responseTopic = "rn/response",
        correlationData = byteArrayOf(9, 8, 7),
        messageExpiryIntervalSeconds = 120,
        payloadFormatIndicator = 0,
      ),
    ).get(10, TimeUnit.SECONDS)
    assertEquals(true, received.await(10, TimeUnit.SECONDS))
    assertEquals(listOf<Byte>(0, 1, 2, 127, -1), receivedMessage!!.payload.toList())
    assertEquals(
      listOf(
        mapOf("key" to "trace", "value" to "integration"),
        mapOf("key" to "trace", "value" to "second"),
      ),
      receivedMessage!!.userProperties,
    )
    assertEquals("application/octet-stream", receivedMessage!!.contentType)
    assertEquals("rn/response", receivedMessage!!.responseTopic)
    assertEquals(listOf<Byte>(9, 8, 7), receivedMessage!!.correlationData!!.toList())
    assertEquals(0, receivedMessage!!.payloadFormatIndicator)
    assertEquals(0, transport.unsubscribe("rn/integration").get(10, TimeUnit.SECONDS))
    transport.disconnect().get(10, TimeUnit.SECONDS)
  }

  @Test
  fun controlledMosquittoAssignsClientIdWhenClientIdIsOmitted() {
    assumeTrue(System.getenv("RUN_MQTT_LOCAL_TESTS") == "1")
    val transport = PahoMqttTransport(
      ParsedMqttOptions(
        host = System.getenv("MQTT_TEST_HOST") ?: "127.0.0.1",
        port = 1883,
        protocolVersion = 5,
        transport = "tcp",
        clientId = "",
        cleanStart = true,
        sessionExpiryIntervalSeconds = 0,
      ),
    )
    val ack = transport.connect().get(10, TimeUnit.SECONDS)
    try {
      assertEquals(0, ack.reasonCode)
      assertTrue("MQTT 5 broker should return an assigned client id", !ack.assignedClientId.isNullOrEmpty())
    } finally {
      transport.disconnect().get(10, TimeUnit.SECONDS)
    }
  }

  @Test
  fun connectsToControlledMosquittoWithMqtt311AndCompletesBasicOperations() {
    assumeTrue(System.getenv("RUN_MQTT_LOCAL_TESTS") == "1")
    val options = ParsedMqttOptions(
      host = System.getenv("MQTT_TEST_HOST") ?: "127.0.0.1",
      port = (System.getenv("MQTT_TEST_PORT") ?: "1883").toInt(),
      protocolVersion = 4,
      transport = "tcp",
      clientId = "rn-jvm-integration-v4",
      keepAliveSeconds = 30,
      cleanSession = true,
    )
    val transport = PahoMqttTransport(options)
    val ack = transport.connect().get(10, TimeUnit.SECONDS)
    assertEquals(null, ack.reasonCode)
    assertEquals(false, ack.sessionPresent)
    val subscription = transport.subscribe("rn/integration-v4", 1, false, false, 0).get(10, TimeUnit.SECONDS)
    assertEquals(1, subscription.grantedQos)
    transport.publish("rn/integration-v4", "payload".toByteArray(), 1, false).get(10, TimeUnit.SECONDS)
    assertEquals(0, transport.unsubscribe("rn/integration-v4").get(10, TimeUnit.SECONDS))
    transport.disconnect().get(10, TimeUnit.SECONDS)
  }

  private fun mqtt5ConnectProperty(packet: ByteArray, propertyId: Int): Int {
    for (offset in 0 until packet.size - 2) {
      if ((packet[offset].toInt() and 0xff) == propertyId) {
        return ((packet[offset + 1].toInt() and 0xff) shl 8) or (packet[offset + 2].toInt() and 0xff)
      }
    }
    error("CONNECT property 0x${propertyId.toString(16)} was not encoded")
  }

}
