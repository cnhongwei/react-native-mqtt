package com.cnhongwei.reactnativemqtt

import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import org.junit.Assert.assertEquals
import org.junit.Assert.assertThrows
import org.junit.Test

private class DelayedTransport : MqttTransport {
  var connectResult = CompletableFuture<MqttConnAck>()
  var connectCalls = 0
  val disconnectResult = CompletableFuture<Void>()
  val subscribeResult = CompletableFuture<MqttSubscriptionResult>()
  val publishResult = CompletableFuture<Void>()
  var onDisconnected: (MqttDisconnect?) -> Unit = {}
  val messageListeners = mutableListOf<(MqttMessageData) -> Unit>()

  override fun connect(): CompletableFuture<MqttConnAck> {
    connectCalls += 1
    return connectResult
  }
  override fun disconnect(): CompletableFuture<Void> = disconnectResult
  override fun subscribe(
    filter: String,
    qos: Int,
    noLocal: Boolean,
    retainAsPublished: Boolean,
    retainHandling: Int,
    onMessage: ((MqttMessageData) -> Unit)?,
  ) = subscribeResult
  override fun unsubscribe(filter: String) = CompletableFuture.completedFuture(0)
  override fun publish(topic: String, payload: ByteArray, qos: Int, retained: Boolean) = publishResult
  override fun setDisconnectedListener(listener: (MqttDisconnect?) -> Unit) {
    onDisconnected = listener
  }
}

private class VirtualScheduler : MqttScheduler {
  private data class Entry(val at: Long, val order: Long, val task: () -> Unit, var cancelled: Boolean = false, var done: Boolean = false)
  private var nowMs = 0L
  private var nextOrder = 0L
  private val entries = mutableListOf<Entry>()

  override fun schedule(delayMs: Long, task: () -> Unit): MqttTimer {
    val entry = Entry(nowMs + delayMs, nextOrder++, task)
    entries += entry
    return object : MqttTimer {
      override fun cancel() { entry.cancelled = true; entry.done = true }
      override fun isDone(): Boolean = entry.done
    }
  }

  fun advanceBy(deltaMs: Long) {
    require(deltaMs >= 0)
    val target = nowMs + deltaMs
    while (true) {
      val next = entries.filter { !it.cancelled && !it.done && it.at <= target }.minWithOrNull(compareBy<Entry> { it.at }.thenBy { it.order }) ?: break
      entries.remove(next)
      nowMs = next.at
      next.done = true
      next.task()
    }
    nowMs = target
  }

  override fun shutdown() { entries.clear() }
}

private fun testMessage(payload: ByteArray) = MqttMessageData("topic", payload, 1, false)

class MqttSessionDeadlineTest {
  @Test
  fun rawConnectFailureIsWrappedWithStructuredOperationMetadata() {
    val transport = DelayedTransport()
    val options = ParsedMqttOptions(host = "broker.example", port = 8887, protocolVersion = 5, transport = "tls")
    val registry = MqttSessionRegistry(transportFactory = { _, _ -> transport })
    registry.open("instance", options).join()

    val connect = registry.connect("instance")
    transport.connectResult.completeExceptionally(IllegalStateException("TLS handshake failed"))
    val error = assertThrows(CompletionException::class.java) { connect.join() }
    val cause = error.cause as MqttSessionException
    assertEquals("ERR_CONNECTION_REFUSED", cause.errorCode)
    assertEquals("connect", cause.operation)
    assertEquals(1L, cause.generation)
    assertEquals("unknown", cause.outcome)
    assertEquals("TLS handshake failed", cause.message)

    transport.disconnectResult.complete(null)
    registry.close("instance").join()
  }

  @Test
  fun firstConnectFailureTerminatesAttemptWithoutPoisoningTheNextConnect() {
    val transport = DelayedTransport()
    val options = ParsedMqttOptions(host = "broker.example", port = 1883, protocolVersion = 5, transport = "tcp")
    val registry = MqttSessionRegistry(transportFactory = { _, _ -> transport })
    registry.open("instance", options).join()

    val first = registry.connect("instance")
    val firstAttemptDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1)
    while (transport.connectCalls != 1) {
      if (System.nanoTime() > firstAttemptDeadline) throw AssertionError("First connect attempt was not started")
      Thread.sleep(1)
    }
    transport.connectResult.completeExceptionally(IllegalStateException("broker unavailable"))
    assertThrows(CompletionException::class.java) { first.join() }
    val disconnectedDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1)
    while (registry.snapshot("instance").join()["state"] != "disconnected") {
      if (System.nanoTime() > disconnectedDeadline) throw AssertionError("Failed connect did not terminate the attempt")
      Thread.sleep(1)
    }

    transport.connectResult = CompletableFuture()
    val second = registry.connect("instance")
    val secondAttemptDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1)
    while (transport.connectCalls != 2) {
      if (System.nanoTime() > secondAttemptDeadline) throw AssertionError("Second connect attempt was not started")
      Thread.sleep(1)
    }
    transport.connectResult.complete(MqttConnAck(false, 0))
    assertEquals("connected", second.join()["snapshot"].let { (it as Map<*, *>)["state"] })

    transport.disconnectResult.complete(null)
    registry.close("instance").join()
  }

  @Test
  fun awaitSubscriptionsWaitsForTheRequestedRevision() {
    val transport = DelayedTransport()
    val options = ParsedMqttOptions(host = "broker.example", port = 1883, protocolVersion = 5, transport = "tcp")
    val registry = MqttSessionRegistry(transportFactory = { _, _ -> transport })
    registry.open("instance", options).join()
    transport.connectResult.complete(MqttConnAck(false, 0))
    registry.connect("instance").join()
    val subscription = registry.setSubscription("instance", "topic", 1, false, false, 0, 1)
    val ready = registry.awaitSubscriptions("instance", 1, 1)
    Thread.sleep(10)
    assertEquals(false, ready.isDone)
    transport.subscribeResult.complete(MqttSubscriptionResult(1, 0))
    subscription.join()
    assertEquals("ready", ready.join()["subscriptionsStatus"])
    transport.disconnectResult.complete(null)
    registry.close("instance").join()
  }

  @Test
  fun disconnectCancelsConnectingAttemptAndLateAckCannotReconnect() {
    val transport = DelayedTransport()
    val options = ParsedMqttOptions(
      host = "broker.example",
      port = 1883,
      protocolVersion = 5,
      transport = "tcp",
      disconnectTimeoutMs = 100,
    )
    val registry = MqttSessionRegistry(transportFactory = { _, _ -> transport })
    registry.open("instance", options).join()
    val connect = registry.connect("instance")
    registry.disconnect("instance").also { transport.disconnectResult.complete(null); it.join() }
    transport.connectResult.complete(MqttConnAck(false, 0))
    assertEquals("disconnected", registry.snapshot("instance").join()["state"])
    assertThrows(ExecutionException::class.java) { connect.get(1, TimeUnit.SECONDS) }
    transport.disconnectResult.complete(null)
    registry.close("instance").join()
  }

  @Test
  fun delayedOperationsTimeoutOnceAndIgnoreLateCompletion() {
    val transport = DelayedTransport()
    val options = ParsedMqttOptions(
      host = "broker.example",
      port = 1883,
      protocolVersion = 5,
      transport = "tcp",
      operationTimeoutMs = 25,
      disconnectTimeoutMs = 25,
    )
    val registry = MqttSessionRegistry(transportFactory = { _, _ -> transport })
    registry.open("instance", options).join()
    transport.connectResult.complete(MqttConnAck(false, 0))
    registry.connect("instance").join()

    val subscription = registry.setSubscription("instance", "topic", 1, false, false, 0, 1)
    val subscriptionError = assertThrows(ExecutionException::class.java) {
      subscription.get(1, TimeUnit.SECONDS)
    }
    assertEquals("ERR_OPERATION_TIMEOUT", (subscriptionError.cause as MqttSessionException).errorCode)
    transport.subscribeResult.complete(MqttSubscriptionResult(1, 0))
    val lateSubscriptionError = subscription.handle { _, error -> error }.join()
    assertEquals("ERR_OPERATION_TIMEOUT", (lateSubscriptionError as MqttSessionException).errorCode)

    val publish = registry.publish("instance", "topic", byteArrayOf(1), 1, false)
    val publishError = assertThrows(ExecutionException::class.java) {
      publish.get(1, TimeUnit.SECONDS)
    }
    assertEquals("ERR_OPERATION_TIMEOUT", (publishError.cause as MqttSessionException).errorCode)
    transport.publishResult.complete(null)

    val disconnect = registry.disconnect("instance")
    val disconnectError = assertThrows(ExecutionException::class.java) {
      disconnect.get(1, TimeUnit.SECONDS)
    }
    assertEquals("ERR_OPERATION_TIMEOUT", (disconnectError.cause as MqttSessionException).errorCode)
    assertEquals("disconnected", registry.snapshot("instance").join()["state"])
    transport.disconnectResult.complete(null)
    registry.close("instance").join()
  }

  @Test
  fun publishTransportFailureBecomesStructuredUnknownOutcome() {
    val transport = DelayedTransport()
    val options = ParsedMqttOptions(host = "broker.example", port = 1883, protocolVersion = 5, transport = "tcp")
    val registry = MqttSessionRegistry(transportFactory = { _, _ -> transport })
    registry.open("instance", options).join()
    transport.connectResult.complete(MqttConnAck(false, 0))
    registry.connect("instance").join()

    val publish = registry.publish("instance", "topic", byteArrayOf(1), 2, false)
    transport.publishResult.completeExceptionally(IllegalStateException("PUBCOMP completion is not exposed"))
    val error = assertThrows(ExecutionException::class.java) { publish.get(1, TimeUnit.SECONDS) }
    val cause = error.cause as MqttSessionException
    assertEquals("ERR_PUBLISH_FAILED", cause.errorCode)
    assertEquals("publish", cause.operation)
    assertEquals(1L, cause.generation)
    assertEquals("unknown", cause.outcome)

    transport.disconnectResult.complete(null)
    registry.close("instance").join()
  }

  @Test
  fun disconnectSettlesPendingSubscribeAndPublishOperations() {
    val transport = DelayedTransport()
    val options = ParsedMqttOptions(
      host = "broker.example",
      port = 1883,
      protocolVersion = 5,
      transport = "tcp",
      operationTimeoutMs = 5_000,
      disconnectTimeoutMs = 100,
    )
    val registry = MqttSessionRegistry(transportFactory = { _, _ -> transport })
    registry.open("instance", options).join()
    transport.connectResult.complete(MqttConnAck(false, 0))
    registry.connect("instance").join()

    val subscribe = registry.setSubscription("instance", "topic", 1, false, false, 0, 1)
    val publish = registry.publish("instance", "topic", byteArrayOf(1), 1, false)
    val pendingDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1)
    while (registry.pendingOperations("instance") < 2) {
      if (System.nanoTime() > pendingDeadline) throw AssertionError("Pending operation metric did not include publish")
      Thread.sleep(1)
    }
    val disconnect = registry.disconnect("instance")
    transport.disconnectResult.complete(null)
    assertEquals("disconnected", disconnect.join()["state"])

    val subscribeError = assertThrows(ExecutionException::class.java) { subscribe.get(1, TimeUnit.SECONDS) }
    assertEquals("ERR_CONNECTION_LOST", (subscribeError.cause as MqttSessionException).errorCode)
    val publishError = assertThrows(ExecutionException::class.java) { publish.get(1, TimeUnit.SECONDS) }
    assertEquals("ERR_CONNECTION_LOST", (publishError.cause as MqttSessionException).errorCode)

    registry.close("instance").join()
  }

  @Test
  fun operationLedgerKeepsGenerationAndTopicUntilExactlyOneSettlement() {
    val transport = DelayedTransport()
    val options = ParsedMqttOptions(
      host = "broker.example",
      port = 1883,
      protocolVersion = 5,
      transport = "tcp",
      operationTimeoutMs = 25,
    )
    val registry = MqttSessionRegistry(transportFactory = { _, _ -> transport })
    registry.open("instance", options).join()
    transport.connectResult.complete(MqttConnAck(false, 0))
    registry.connect("instance").join()

    val publish = registry.publish("instance", "ledger/topic", byteArrayOf(7), 1, false)
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1)
    while (registry.operationLedger("instance").isEmpty()) {
      if (System.nanoTime() > deadline) throw AssertionError("operation ledger entry was not registered")
      Thread.sleep(1)
    }
    val entry = registry.operationLedger("instance").single()
    assertEquals("publish", entry["operation"])
    assertEquals("ledger/topic", entry["topic"])
    assertEquals(1L, entry["generation"])
    assertEquals(false, entry["settled"])

    assertEquals("ERR_OPERATION_TIMEOUT", (runCatching { publish.join() }.exceptionOrNull()?.cause as MqttSessionException).errorCode)
    transport.publishResult.complete(null)
    Thread.sleep(20)
    assertEquals(emptyList<Map<String, Any?>>(), registry.operationLedger("instance"))
    transport.disconnectResult.complete(null)
    registry.close("instance").join()
  }

  @Test
  fun closeSettlesPendingPublishAsDisposed() {
    val transport = DelayedTransport()
    val options = ParsedMqttOptions(
      host = "broker.example",
      port = 1883,
      protocolVersion = 5,
      transport = "tcp",
      operationTimeoutMs = 5_000,
      disconnectTimeoutMs = 100,
    )
    val registry = MqttSessionRegistry(transportFactory = { _, _ -> transport })
    registry.open("instance", options).join()
    transport.connectResult.complete(MqttConnAck(false, 0))
    registry.connect("instance").join()

    val publish = registry.publish("instance", "topic", byteArrayOf(1), 1, false)
    val close = registry.close("instance")
    transport.disconnectResult.complete(null)
    assertEquals("closed", close.join()["state"])

    val publishError = assertThrows(ExecutionException::class.java) { publish.get(1, TimeUnit.SECONDS) }
    assertEquals("ERR_DISPOSED", (publishError.cause as MqttSessionException).errorCode)
  }

  @Test
  fun establishedDisconnectUsesOneRegistryOwnedReconnectAttempt() {
    val transport = DelayedTransport()
    val options = ParsedMqttOptions(
      host = "broker.example",
      port = 1883,
      protocolVersion = 5,
      transport = "tcp",
      reconnectInitialDelayMs = 5,
      reconnectMaxDelayMs = 5,
      reconnectJitterRatio = 0.0,
    )
    val deliveredGenerations = mutableListOf<Long>()
    val registry = MqttSessionRegistry(
      onMessage = { _, generation, _ -> deliveredGenerations += generation },
      transportFactory = { _, onMessage ->
        transport.messageListeners += onMessage
        transport
      },
    )
    registry.open("instance", options).join()
    transport.connectResult.complete(MqttConnAck(false, 0))
    registry.connect("instance").join()

    transport.connectResult = CompletableFuture()
    transport.onDisconnected(null)
    val reconnectingDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1)
    while (registry.snapshot("instance").join()["state"] != "reconnecting") {
      if (System.nanoTime() > reconnectingDeadline) throw AssertionError("Session did not enter reconnecting")
      Thread.sleep(2)
    }
    val reconnectFuture = transport.connectResult
    val connectedDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1)
    while (registry.snapshot("instance").join()["state"] != "connecting") {
      if (System.nanoTime() > connectedDeadline) throw AssertionError("Registry did not schedule reconnect")
      Thread.sleep(2)
    }
    reconnectFuture.complete(MqttConnAck(false, 0))
    while (registry.snapshot("instance").join()["state"] != "connected") {
      if (System.nanoTime() > connectedDeadline) throw AssertionError("Reconnect did not complete")
      Thread.sleep(2)
    }
    assertEquals(2L, registry.snapshot("instance").join()["generation"])
    transport.messageListeners[0](testMessage(byteArrayOf(1)))
    transport.messageListeners[1](testMessage(byteArrayOf(2)))
    val messageDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1)
    while (deliveredGenerations.isEmpty()) {
      if (System.nanoTime() > messageDeadline) throw AssertionError("Current generation message was not delivered")
      Thread.sleep(2)
    }
    assertEquals(listOf(2L), deliveredGenerations)
    transport.disconnectResult.complete(null)
    registry.close("instance").join()
  }

  @Test
  fun connectDuringReconnectingWaitsForTheSingleScheduledAttempt() {
    val transport = DelayedTransport()
    val options = ParsedMqttOptions(
      host = "broker.example",
      port = 1883,
      protocolVersion = 5,
      transport = "tcp",
      connectTimeoutMs = 200,
      reconnectInitialDelayMs = 20,
      reconnectMaxDelayMs = 20,
      reconnectJitterRatio = 0.0,
    )
    val registry = MqttSessionRegistry(transportFactory = { _, _ -> transport })
    registry.open("instance", options).join()
    transport.connectResult.complete(MqttConnAck(false, 0))
    registry.connect("instance").join()
    assertEquals(1, transport.connectCalls)

    transport.connectResult = CompletableFuture()
    transport.onDisconnected(null)
    val reconnectingDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1)
    while (registry.snapshot("instance").join()["state"] != "reconnecting") {
      if (System.nanoTime() > reconnectingDeadline) throw AssertionError("Session did not enter reconnecting")
      Thread.sleep(2)
    }

    val waiter = registry.connect("instance")
    assertEquals(false, waiter.isDone)
    val connectingDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1)
    while (registry.snapshot("instance").join()["state"] != "connecting") {
      if (System.nanoTime() > connectingDeadline) throw AssertionError("Scheduled reconnect did not start")
      Thread.sleep(2)
    }
    assertEquals(2, transport.connectCalls)
    transport.connectResult.complete(MqttConnAck(false, 0))
    assertEquals(2L, waiter.join()["snapshot"].let { (it as Map<*, *>)["generation"] })
    transport.disconnectResult.complete(null)
    registry.close("instance").join()
  }

  @Test
  fun reconnectConnectTimeoutRejectsTheWaiterBeforeTheNextAttempt() {
    val transport = DelayedTransport()
    val options = ParsedMqttOptions(
      host = "broker.example",
      port = 1883,
      protocolVersion = 5,
      transport = "tcp",
      connectTimeoutMs = 25,
      reconnectInitialDelayMs = 1_000,
      reconnectMaxDelayMs = 1_000,
      reconnectJitterRatio = 0.0,
    )
    val registry = MqttSessionRegistry(transportFactory = { _, _ -> transport })
    registry.open("instance", options).join()
    transport.connectResult.complete(MqttConnAck(false, 0))
    registry.connect("instance").join()

    transport.connectResult = CompletableFuture()
    transport.onDisconnected(null)
    val reconnectingDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1)
    while (registry.snapshot("instance").join()["state"] != "reconnecting") {
      if (System.nanoTime() > reconnectingDeadline) throw AssertionError("Session did not enter reconnecting")
      Thread.sleep(2)
    }

    val waiter = registry.connect("instance")
    val error = assertThrows(ExecutionException::class.java) { waiter.get(1, TimeUnit.SECONDS) }
    assertEquals("ERR_CONNECT_TIMEOUT", (error.cause as MqttSessionException).errorCode)
    assertEquals(1, transport.connectCalls)
    transport.disconnectResult.complete(null)
    registry.close("instance").join()
  }

  @Test
  fun manualDisconnectDuringReconnectCancelsTheScheduledRetry() {
    val transport = DelayedTransport()
    val options = ParsedMqttOptions(
      host = "broker.example",
      port = 1883,
      protocolVersion = 5,
      transport = "tcp",
      reconnectInitialDelayMs = 50,
      reconnectMaxDelayMs = 50,
      reconnectJitterRatio = 0.0,
    )
    val registry = MqttSessionRegistry(transportFactory = { _, _ -> transport })
    registry.open("instance", options).join()
    transport.connectResult.complete(MqttConnAck(false, 0))
    registry.connect("instance").join()
    transport.onDisconnected(null)

    val reconnectingDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1)
    while (registry.snapshot("instance").join()["state"] != "reconnecting") {
      if (System.nanoTime() > reconnectingDeadline) throw AssertionError("Session did not enter reconnecting")
      Thread.sleep(2)
    }

    val disconnect = registry.disconnect("instance")
    transport.disconnectResult.complete(null)
    assertEquals("disconnected", disconnect.join()["state"])
    Thread.sleep(100)
    assertEquals(1, transport.connectCalls)

    registry.close("instance").join()
  }

  @Test
  fun virtualSchedulerSettlesConnectDeadlineWithoutWallClockWait() {
    val transport = DelayedTransport()
    val scheduler = VirtualScheduler()
    val options = ParsedMqttOptions(
      host = "broker.example",
      port = 1883,
      protocolVersion = 5,
      transport = "tcp",
      connectTimeoutMs = 25,
      reconnectEnabled = false,
    )
    val registry = MqttSessionRegistry(
      scheduler = scheduler,
      transportFactory = { _, _ -> transport },
    )
    registry.open("instance", options).join()
    val connect = registry.connect("instance")
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1)
    while (transport.connectCalls == 0) {
      if (System.nanoTime() > deadline) throw AssertionError("Connect was not submitted")
      Thread.sleep(1)
    }
    scheduler.advanceBy(25)
    val error = assertThrows(ExecutionException::class.java) { connect.get(1, TimeUnit.SECONDS) }
    assertEquals("ERR_CONNECT_TIMEOUT", (error.cause as MqttSessionException).errorCode)
    transport.connectResult.complete(MqttConnAck(false, 0))
    assertEquals("disconnected", registry.snapshot("instance").join()["state"])
    transport.disconnectResult.complete(null)
    registry.close("instance").join()
  }
}
