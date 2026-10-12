package com.cnhongwei.reactnativemqtt

import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import org.junit.Assert.assertEquals
import org.junit.Test

private class ReconnectTransport : MqttTransport {
  val connectResult = CompletableFuture<MqttConnAck>()
  val subscribedFilters = mutableListOf<String>()
  val batchRequests = mutableListOf<List<MqttSubscriptionRequest>>()
  var rejectedBatchIndex: Int? = null
  var omitLastBatchResult = false
  var delayedBatchResult: CompletableFuture<List<MqttSubscriptionResult>>? = null
  var unsubscribeCalls = 0
  var disconnected: (MqttDisconnect?) -> Unit = {}

  override fun connect() = connectResult
  override fun disconnect(): CompletableFuture<Void> = CompletableFuture.completedFuture(null)
  override fun subscribe(
    filter: String,
    qos: Int,
    noLocal: Boolean,
    retainAsPublished: Boolean,
    retainHandling: Int,
    onMessage: ((MqttMessageData) -> Unit)?,
  ): CompletableFuture<MqttSubscriptionResult> {
    subscribedFilters += filter
    return CompletableFuture.completedFuture(MqttSubscriptionResult(qos, qos))
  }
  override fun unsubscribe(filter: String): CompletableFuture<Int> {
    unsubscribeCalls += 1
    return CompletableFuture.completedFuture(0)
  }
  override fun subscribeBatch(requests: List<MqttSubscriptionRequest>): CompletableFuture<List<MqttSubscriptionResult>> {
    batchRequests += requests
    subscribedFilters += requests.map { it.filter }
    val results = requests.mapIndexed { index, request ->
      if (index == rejectedBatchIndex) MqttSubscriptionResult(-1, 128)
      else MqttSubscriptionResult(request.qos, request.qos)
    }
    return delayedBatchResult ?: CompletableFuture.completedFuture(if (omitLastBatchResult) results.dropLast(1) else results)
  }
  override fun publish(topic: String, payload: ByteArray, qos: Int, retained: Boolean): CompletableFuture<Void> = CompletableFuture.completedFuture(null)
  override fun setDisconnectedListener(listener: (MqttDisconnect?) -> Unit) { disconnected = listener }
}

private class IntentRaceTransport : MqttTransport {
  val connectResult = CompletableFuture<MqttConnAck>()
  val subscribeResult = CompletableFuture<MqttSubscriptionResult>()
  val unsubscribeResult = CompletableFuture<Int>()
  var unsubscribeCalls = 0

  override fun connect() = connectResult
  override fun disconnect(): CompletableFuture<Void> = CompletableFuture.completedFuture(null)
  override fun subscribe(
    filter: String,
    qos: Int,
    noLocal: Boolean,
    retainAsPublished: Boolean,
    retainHandling: Int,
    onMessage: ((MqttMessageData) -> Unit)?,
  ) = subscribeResult
  override fun unsubscribe(filter: String): CompletableFuture<Int> {
    unsubscribeCalls += 1
    return unsubscribeResult
  }
  override fun publish(topic: String, payload: ByteArray, qos: Int, retained: Boolean): CompletableFuture<Void> = CompletableFuture.completedFuture(null)
  override fun setDisconnectedListener(listener: (MqttDisconnect?) -> Unit) = Unit
}

class MqttSessionReconciliationTest {
  @Test
  fun offlineRemoveRecordsTombstoneAndClearsExistingBrokerSessionOnReconnect() {
    val options = ParsedMqttOptions(
      host = "broker.example", port = 1883, protocolVersion = 5, transport = "tcp",
      clientId = "offline-remove-test", reconnectEnabled = false,
    )
    val transports = mutableListOf<ReconnectTransport>()
    val registry = MqttSessionRegistry(transportFactory = { _, _ ->
      ReconnectTransport().also { transports += it }
    })
    registry.open("instance", options).join()
    val firstConnect = registry.connect("instance")
    while (transports.isEmpty()) Thread.sleep(1)
    transports[0].connectResult.complete(MqttConnAck(sessionPresent = true, reasonCode = 0))
    firstConnect.join()
    registry.setSubscription("instance", "offline/status", 1, false, false, 0, 1).join()
    registry.disconnect("instance").join()

    registry.removeSubscription("instance", "offline/status", 2).join()
    val secondConnect = registry.connect("instance")
    while (transports.size < 2) Thread.sleep(1)
    transports[1].connectResult.complete(MqttConnAck(sessionPresent = true, reasonCode = 0))
    secondConnect.join()
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1)
    while (transports[1].unsubscribeCalls == 0) {
      if (System.nanoTime() > deadline) throw AssertionError("offline tombstone was not restored")
      Thread.sleep(2)
    }
    assertEquals(1, transports[1].unsubscribeCalls)
    assertEquals("ready", registry.snapshot("instance").join()["subscriptionsStatus"])
    registry.close("instance").join()
  }

  @Test
  fun lateSetAckCannotResurrectSubscriptionAfterRemoveIntent() {
    val transport = IntentRaceTransport()
    val options = ParsedMqttOptions(host = "broker.example", port = 1883, protocolVersion = 5, transport = "tcp")
    val registry = MqttSessionRegistry(transportFactory = { _, _ -> transport })
    registry.open("instance", options).join()
    val connect = registry.connect("instance")
    transport.connectResult.complete(MqttConnAck(sessionPresent = true, reasonCode = 0))
    connect.join()

    val set = registry.setSubscription("instance", "race/status", 1, false, false, 0, 1)
    val remove = registry.removeSubscription("instance", "race/status", 2)
    assertEquals(0, transport.unsubscribeCalls)
    transport.subscribeResult.complete(MqttSubscriptionResult(grantedQos = 1, reasonCode = 1))
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1)
    while (transport.unsubscribeCalls == 0) {
      if (System.nanoTime() > deadline) throw AssertionError("same-filter unsubscribe was not queued")
      Thread.sleep(2)
    }
    val setError = runCatching { set.join() }.exceptionOrNull()
    var cause: Throwable? = setError
    while (cause != null && cause !is MqttSessionException) cause = cause.cause
    assertEquals("ERR_OPERATION_SUPERSEDED", (cause as MqttSessionException).errorCode)
    transport.unsubscribeResult.complete(0)
    remove.join()

    val snapshot = registry.snapshot("instance").join()
    assertEquals("ready", snapshot["subscriptionsStatus"])
    assertEquals(2L, snapshot["subscriptionsRevision"])
    registry.close("instance").join()
  }

  @Test
  fun lostBrokerSessionRestoresDesiredSubscriptionWithoutJsReplay() {
    val options = ParsedMqttOptions(
      host = "broker.example",
      port = 1883,
      protocolVersion = 5,
      transport = "tcp",
      clientId = "reconcile-test",
      reconnectInitialDelayMs = 1,
      reconnectMaxDelayMs = 1,
      reconnectJitterRatio = 0.0,
    )
    val transports = mutableListOf<ReconnectTransport>()
    val registry = MqttSessionRegistry(transportFactory = { _, _ ->
      ReconnectTransport().also { transports += it }
    })
    registry.open("instance", options).join()
    val connect = registry.connect("instance")
    while (transports.isEmpty()) Thread.sleep(1)
    val first = transports[0]
    first.connectResult.complete(MqttConnAck(sessionPresent = false, reasonCode = 0))
    connect.join()
    registry.setSubscription("instance", "device/+/status", 1, false, false, 0, 1).join()
    assertEquals(listOf("device/+/status"), first.subscribedFilters)

    first.disconnected(null)
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2)
    while (transports.size < 2) {
      if (System.nanoTime() > deadline) throw AssertionError("reconnect transport was not created")
      Thread.sleep(2)
    }
    val second = transports[1]
    second.connectResult.complete(MqttConnAck(sessionPresent = false, reasonCode = 0))
    while (second.subscribedFilters.isEmpty()) {
      if (System.nanoTime() > deadline) throw AssertionError("desired subscription was not restored")
      Thread.sleep(2)
    }
    assertEquals(listOf("device/+/status"), second.subscribedFilters)
    assertEquals("ready", registry.snapshot("instance").join()["subscriptionsStatus"])
    registry.close("instance").join()
  }

  @Test
  fun lostBrokerSessionRestoresTwentyFiltersInOneBatch() {
    val options = ParsedMqttOptions(
      host = "broker.example",
      port = 1883,
      protocolVersion = 5,
      transport = "tcp",
      clientId = "batch-reconcile-test",
      reconnectInitialDelayMs = 1,
      reconnectMaxDelayMs = 1,
      reconnectJitterRatio = 0.0,
    )
    val transports = mutableListOf<ReconnectTransport>()
    val registry = MqttSessionRegistry(transportFactory = { _, _ ->
      ReconnectTransport().also { transports += it }
    })
    registry.open("instance", options).join()
    val connect = registry.connect("instance")
    while (transports.isEmpty()) Thread.sleep(1)
    transports[0].connectResult.complete(MqttConnAck(sessionPresent = true, reasonCode = 0))
    connect.join()
    repeat(20) { index ->
      registry.setSubscription("instance", "devices/$index/status", 1, false, false, 0, index.toLong() + 1).join()
    }

    transports[0].disconnected(null)
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2)
    while (transports.size < 2) {
      if (System.nanoTime() > deadline) throw AssertionError("reconnect transport was not created")
      Thread.sleep(2)
    }
    val second = transports[1]
    second.connectResult.complete(MqttConnAck(sessionPresent = false, reasonCode = 0))
    while (second.batchRequests.isEmpty()) {
      if (System.nanoTime() > deadline) throw AssertionError("batched desired subscriptions were not restored")
      Thread.sleep(2)
    }
    assertEquals(1, second.batchRequests.size)
    assertEquals(20, second.batchRequests.single().size)
    while (registry.snapshot("instance").join()["subscriptionsStatus"] != "ready") {
      if (System.nanoTime() > deadline) throw AssertionError("batched restoration did not reach ready")
      Thread.sleep(2)
    }
    assertEquals("ready", registry.snapshot("instance").join()["subscriptionsStatus"])
    registry.close("instance").join()
  }

  @Test
  fun lostBrokerSessionSplitsMoreThanThirtyTwoFilters() {
    val options = ParsedMqttOptions(
      host = "broker.example",
      port = 1883,
      protocolVersion = 5,
      transport = "tcp",
      clientId = "split-reconcile-test",
      reconnectInitialDelayMs = 1,
      reconnectMaxDelayMs = 1,
      reconnectJitterRatio = 0.0,
    )
    val transports = mutableListOf<ReconnectTransport>()
    val registry = MqttSessionRegistry(transportFactory = { _, _ ->
      ReconnectTransport().also { transports += it }
    })
    registry.open("instance", options).join()
    val connect = registry.connect("instance")
    while (transports.isEmpty()) Thread.sleep(1)
    transports[0].connectResult.complete(MqttConnAck(sessionPresent = true, reasonCode = 0))
    connect.join()
    repeat(33) { index ->
      registry.setSubscription("instance", "split/$index", 1, false, false, 0, index.toLong() + 1).join()
    }

    transports[0].disconnected(null)
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2)
    while (transports.size < 2) {
      if (System.nanoTime() > deadline) throw AssertionError("reconnect transport was not created")
      Thread.sleep(2)
    }
    val second = transports[1]
    second.connectResult.complete(MqttConnAck(sessionPresent = false, reasonCode = 0))
    while (second.batchRequests.size < 2) {
      if (System.nanoTime() > deadline) throw AssertionError("restore batches were not submitted")
      Thread.sleep(2)
    }
    assertEquals(listOf(32, 1), second.batchRequests.map { it.size })
    while (registry.snapshot("instance").join()["subscriptionsStatus"] != "ready") {
      if (System.nanoTime() > deadline) throw AssertionError("split restoration did not reach ready")
      Thread.sleep(2)
    }
    registry.close("instance").join()
  }

  @Test
  fun partialRestoreRejectionFailsSynchronizationWithoutDroppingOtherIntent() {
    val options = ParsedMqttOptions(
      host = "broker.example",
      port = 1883,
      protocolVersion = 5,
      transport = "tcp",
      clientId = "partial-reconcile-test",
      reconnectInitialDelayMs = 1,
      reconnectMaxDelayMs = 1,
      reconnectJitterRatio = 0.0,
    )
    val transports = mutableListOf<ReconnectTransport>()
    val registry = MqttSessionRegistry(transportFactory = { _, _ ->
      ReconnectTransport().also { transport ->
        if (transports.isNotEmpty()) transport.rejectedBatchIndex = 1
        transports += transport
      }
    })
    registry.open("instance", options).join()
    val connect = registry.connect("instance")
    while (transports.isEmpty()) Thread.sleep(1)
    transports[0].connectResult.complete(MqttConnAck(sessionPresent = true, reasonCode = 0))
    connect.join()
    registry.setSubscription("instance", "partial/a", 1, false, false, 0, 1).join()
    registry.setSubscription("instance", "partial/b", 1, false, false, 0, 2).join()

    transports[0].disconnected(null)
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2)
    while (transports.size < 2) {
      if (System.nanoTime() > deadline) throw AssertionError("reconnect transport was not created")
      Thread.sleep(2)
    }
    val second = transports[1]
    second.connectResult.complete(MqttConnAck(sessionPresent = false, reasonCode = 0))
    while (registry.snapshot("instance").join()["subscriptionsStatus"] != "failed") {
      if (System.nanoTime() > deadline) throw AssertionError("partial restore did not fail synchronization")
      Thread.sleep(2)
    }
    assertEquals(2, second.batchRequests.single().size)
    registry.close("instance").join()
  }

  @Test
  fun missingRestoreAckFailsWithoutDiscardingSuccessfullyGrantedItems() {
    val options = ParsedMqttOptions(
      host = "broker.example",
      port = 1883,
      protocolVersion = 5,
      transport = "tcp",
      clientId = "missing-ack-reconcile-test",
      reconnectInitialDelayMs = 1,
      reconnectMaxDelayMs = 1,
      reconnectJitterRatio = 0.0,
    )
    val transports = mutableListOf<ReconnectTransport>()
    val registry = MqttSessionRegistry(transportFactory = { _, _ ->
      ReconnectTransport().also { transport ->
        transports += transport
      }
    })
    registry.open("instance", options).join()
    val connect = registry.connect("instance")
    while (transports.isEmpty()) Thread.sleep(1)
    transports[0].connectResult.complete(MqttConnAck(sessionPresent = true, reasonCode = 0))
    connect.join()
    registry.setSubscription("instance", "partial/ok", 1, false, false, 0, 1).join()
    registry.setSubscription("instance", "partial/missing", 1, false, false, 0, 2).join()

    transports[0].disconnected(null)
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2)
    while (transports.size < 2) {
      if (System.nanoTime() > deadline) throw AssertionError("reconnect transport was not created")
      Thread.sleep(2)
    }
    val second = transports[1]
    second.omitLastBatchResult = true
    second.connectResult.complete(MqttConnAck(sessionPresent = false, reasonCode = 0))
    while (second.batchRequests.isEmpty()) {
      if (System.nanoTime() > deadline) throw AssertionError("restore batch was not submitted")
      Thread.sleep(2)
    }
    while (registry.snapshot("instance").join()["subscriptionsStatus"] != "failed") {
      if (System.nanoTime() > deadline) throw AssertionError("missing restore ACK did not fail synchronization")
      Thread.sleep(2)
    }
    assertEquals("failed", registry.snapshot("instance").join()["subscriptionsStatus"])
    registry.close("instance").join()
  }

  @Test
  fun oversizedSingleRestoreItemFailsWithoutSendingAnInvalidBatch() {
    val options = ParsedMqttOptions(
      host = "broker.example",
      port = 1883,
      protocolVersion = 5,
      transport = "tcp",
      clientId = "oversized-reconcile-test",
      reconnectInitialDelayMs = 1,
      reconnectMaxDelayMs = 1,
      reconnectJitterRatio = 0.0,
    )
    val transports = mutableListOf<ReconnectTransport>()
    val registry = MqttSessionRegistry(transportFactory = { _, _ ->
      ReconnectTransport().also { transports += it }
    })
    registry.open("instance", options).join()
    val connect = registry.connect("instance")
    while (transports.isEmpty()) Thread.sleep(1)
    transports[0].connectResult.complete(MqttConnAck(sessionPresent = true, reasonCode = 0))
    connect.join()
    registry.setSubscription("instance", "x".repeat(17_000), 1, false, false, 0, 1).join()

    transports[0].disconnected(null)
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2)
    while (transports.size < 2) {
      if (System.nanoTime() > deadline) throw AssertionError("reconnect transport was not created")
      Thread.sleep(2)
    }
    val second = transports[1]
    second.connectResult.complete(MqttConnAck(sessionPresent = false, reasonCode = 0))
    while (registry.snapshot("instance").join()["subscriptionsStatus"] != "failed") {
      if (System.nanoTime() > deadline) throw AssertionError("oversized restore item did not fail")
      Thread.sleep(2)
    }
    assertEquals(0, second.batchRequests.size)
    registry.close("instance").join()
  }

  @Test
  fun restoreBatchDoesNotResurrectAnIntentRemovedWhileBatchIsInFlight() {
    val options = ParsedMqttOptions(
      host = "broker.example",
      port = 1883,
      protocolVersion = 5,
      transport = "tcp",
      clientId = "batch-intent-race-test",
      reconnectInitialDelayMs = 1,
      reconnectMaxDelayMs = 1,
      reconnectJitterRatio = 0.0,
    )
    val transports = mutableListOf<ReconnectTransport>()
    val registry = MqttSessionRegistry(transportFactory = { _, _ ->
      ReconnectTransport().also { transports += it }
    })
    registry.open("instance", options).join()
    val connect = registry.connect("instance")
    while (transports.isEmpty()) Thread.sleep(1)
    transports[0].connectResult.complete(MqttConnAck(sessionPresent = true, reasonCode = 0))
    connect.join()
    registry.setSubscription("instance", "race/a", 1, false, false, 0, 1).join()

    transports[0].disconnected(null)
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2)
    while (transports.size < 2) {
      if (System.nanoTime() > deadline) throw AssertionError("reconnect transport was not created")
      Thread.sleep(2)
    }
    val second = transports[1]
    val delayed = CompletableFuture<List<MqttSubscriptionResult>>()
    second.delayedBatchResult = delayed
    second.connectResult.complete(MqttConnAck(sessionPresent = false, reasonCode = 0))
    while (second.batchRequests.isEmpty()) {
      if (System.nanoTime() > deadline) throw AssertionError("restore batch was not submitted")
      Thread.sleep(2)
    }
    val remove = registry.removeSubscription("instance", "race/a", 2)
    delayed.complete(listOf(MqttSubscriptionResult(1, 1)))
    remove.join()
    while (registry.snapshot("instance").join()["subscriptionsStatus"] != "ready") {
      if (System.nanoTime() > deadline) throw AssertionError("remove did not settle after restore race")
      Thread.sleep(2)
    }
    assertEquals("ready", registry.snapshot("instance").join()["subscriptionsStatus"])
    registry.close("instance").join()
  }
}
