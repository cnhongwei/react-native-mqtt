package com.cnhongwei.reactnativemqtt

import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.ThreadLocalRandom

internal interface MqttTimer {
  fun cancel()
  fun isDone(): Boolean
}

internal interface MqttScheduler {
  fun schedule(delayMs: Long, task: () -> Unit): MqttTimer
  fun shutdown()
}

private class ExecutorMqttScheduler : MqttScheduler {
  private val executor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor { runnable ->
    Thread(runnable, "react-native-mqtt-deadlines").apply { isDaemon = true }
  }

  override fun schedule(delayMs: Long, task: () -> Unit): MqttTimer {
    val future = executor.schedule(task, delayMs, TimeUnit.MILLISECONDS)
    return object : MqttTimer {
      override fun cancel() { future.cancel(false) }
      override fun isDone(): Boolean = future.isDone
    }
  }

  override fun shutdown() { executor.shutdownNow() }
}

internal class MqttSession(
  private val instanceId: String,
  private val options: ParsedMqttOptions,
  private val onMessage: (Long, MqttMessageData) -> Unit = { _, _ -> },
  private val transportFactory: ((ParsedMqttOptions, (MqttMessageData) -> Unit) -> MqttTransport)? = null,
  private val onTransportLost: (Long, MqttDisconnect?) -> Unit = { _, _ -> },
) {
  private data class SubscriptionIntent(
    val qos: Int,
    val noLocal: Boolean,
    val retainAsPublished: Boolean,
    val retainHandling: Int,
    val revision: Long,
    var grantedQos: Int? = null,
  )
  private data class SubscriptionWaiter(
    val generation: Long,
    val desiredRevision: Long,
    val future: CompletableFuture<Map<String, Any?>>,
  )
  private data class OperationLedgerEntry(
    val operationId: Long,
    val generation: Long,
    val operation: String,
    val topic: String?,
    var settled: Boolean = false,
  )

  private var revision = 0L
  private var generation = 0L
  private var state = "idle"
  private var lastConnAck: Map<String, Any?>? = null
  private var subscriptionsRevision = 0L
  private var subscriptionsStatus = "ready"
  private var transport: MqttTransport? = null
  private var connecting: CompletableFuture<MqttConnAck>? = null
  private var attemptId = 0L
  private var stopRequested = false
  private val desiredSubscriptions = LinkedHashMap<String, SubscriptionIntent>()
  private val confirmedSubscriptions = LinkedHashMap<String, SubscriptionIntent>()
  private val tombstones = LinkedHashMap<String, Long>()
  private val subscriptionTails = mutableMapOf<String, CompletableFuture<Unit>>()
  private var pendingSubscriptionOperations = 0
  private val subscriptionWaiters = mutableListOf<SubscriptionWaiter>()
  private val reconnectWaiters = mutableListOf<CompletableFuture<Map<String, Any?>>>()
  private val pendingOperationFutures = mutableSetOf<CompletableFuture<*>>()
  private val operationLedger = LinkedHashMap<Long, OperationLedgerEntry>()
  private var nextOperationId = 0L

  fun snapshot(): Map<String, Any?> = mapOf(
    "instanceId" to instanceId,
    "revision" to revision,
    "generation" to generation,
    "state" to state,
    "lastConnAck" to lastConnAck,
    "subscriptionsRevision" to subscriptionsRevision,
    "subscriptionsStatus" to subscriptionsStatus,
    "deliveryStatus" to "active",
  )

  fun connect(serialExecutor: ExecutorService, forceAttempt: Boolean = false): CompletableFuture<Map<String, Any?>> {
    if (state == "closed") return failed(MqttSessionException("ERR_DISPOSED", "Session is closed"))
    if (state == "connected" && lastConnAck != null) return CompletableFuture.completedFuture(mapOf("snapshot" to snapshot(), "ack" to lastConnAck))
    val existing = connecting
    if (existing != null) return existing.thenApplyAsync({ mapOf("snapshot" to snapshot(), "ack" to lastConnAck) }, serialExecutor)
    if (state == "reconnecting" && !forceAttempt) {
      return CompletableFuture<Map<String, Any?>>().also { reconnectWaiters += it }
    }
    val reuseTransport = state == "reconnecting" && transport?.supportsInPlaceReconnect() == true
    stopRequested = false
    val currentAttempt = ++attemptId
    state = "connecting"
    revision += 1
    val callbackGeneration = generation + 1
    val currentTransport = if (reuseTransport) {
      transport!!
    } else {
      if (transportFactory == null) {
        // Paho owns the client callback across an in-place reconnect. Resolve
        // the generation at delivery time so messages after reconnect are
        // tagged with the current generation rather than the first one.
        PahoMqttTransport(options) { message ->
          if (state == "connected") onMessage(generation, message)
        }
      } else {
        transportFactory.invoke(options) { message ->
          if (state == "connected" && generation == callbackGeneration) onMessage(callbackGeneration, message)
        }
      }.also { transport = it }
    }
    currentTransport.setDisconnectedListener { reason ->
      try {
        serialExecutor.execute {
          if (state == "connected" && generation == callbackGeneration) {
            state = "reconnecting"
            revision += 1
            onTransportLost(callbackGeneration, reason)
          }
        }
      } catch (_: RejectedExecutionException) {
        // Registry teardown owns the terminal state.
      }
    }
    val current = currentTransport.connect()
    connecting = current
    return current.handleAsync({ ack, error ->
        if (error != null) {
          val existing = findMqttSessionException(error)
          if (existing != null) throw existing
          throw MqttSessionException(
            "ERR_CONNECTION_REFUSED",
            error.message ?: "Connection failed",
            error,
            operation = "connect",
            generation = callbackGeneration,
            outcome = "unknown",
          )
        }
        if (ack == null) throw MqttSessionException(
          "ERR_CONNECTION_REFUSED",
          "Connection completed without CONNACK",
          operation = "connect",
          generation = callbackGeneration,
          outcome = "unknown",
        )
        if (stopRequested || currentAttempt != attemptId) throw MqttSessionException("ERR_CONNECTION_LOST", "Connection attempt was superseded")
        state = "connected"
        generation += 1
        revision += 1
        reconnectAttempts = 0
        lastConnAck = mapOf("sessionPresent" to ack.sessionPresent, "reasonCode" to ack.reasonCode, "reasonString" to ack.reasonString, "assignedClientId" to ack.assignedClientId, "userProperties" to ack.userProperties).filterValues { it != null }
        connecting = null
        subscriptionsStatus = if (desiredSubscriptions.isEmpty()) "ready" else "pending"
        if (!ack.sessionPresent) {
          tombstones.clear()
          if (desiredSubscriptions.isNotEmpty()) restoreSubscriptions(serialExecutor)
        } else if (tombstones.isNotEmpty()) {
          restoreTombstones(serialExecutor)
        }
        settleSubscriptionWaiters()
        mapOf<String, Any?>("snapshot" to snapshot(), "ack" to lastConnAck).also { result ->
          reconnectWaiters.removeAll { waiter -> waiter.complete(result); true }
        }
      }, serialExecutor)
      .whenCompleteAsync({ _, _ -> if (currentAttempt == attemptId && state == "connecting") { state = "disconnected"; connecting = null } }, serialExecutor)
  }

  fun disconnect(serialExecutor: ExecutorService): CompletableFuture<Map<String, Any?>> {
    if (state == "idle" || state == "disconnected") return CompletableFuture.completedFuture(snapshot())
    stopRequested = true
    attemptId += 1
    rejectReconnectWaiters(MqttSessionException("ERR_CONNECTION_LOST", "Connection was stopped"))
    connecting?.cancel(true)
    connecting = null
    state = "disconnecting"
    revision += 1
    cancelPendingOperations(MqttSessionException("ERR_CONNECTION_LOST", "Operation was cancelled by disconnect"))
    settleSubscriptionWaiters()
    return (transport?.disconnect() ?: CompletableFuture.completedFuture(null)).thenApplyAsync({
        state = "disconnected"
        revision += 1
        settleSubscriptionWaiters()
        snapshot()
      }, serialExecutor)
  }

  fun timeoutConnect() {
    if (state != "connecting") return
    state = "disconnected"
    revision += 1
    stopRequested = true
    attemptId += 1
    settleSubscriptionWaiters()
    connecting?.cancel(true)
    connecting = null
    rejectReconnectWaiters(MqttSessionException("ERR_CONNECT_TIMEOUT", "Connection timed out"))
    transport?.disconnect()
  }

  fun setSubscription(
    filter: String,
    qos: Int,
    noLocal: Boolean,
    retainAsPublished: Boolean,
    retainHandling: Int,
    intentRevision: Long,
    serialExecutor: ExecutorService,
  ): CompletableFuture<Map<String, Any?>> {
    if (state != "connected") return failed(MqttSessionException("ERR_NOT_CONNECTED", "Session is not connected"))
    subscriptionsStatus = "pending"
    subscriptionsRevision = maxOf(subscriptionsRevision, intentRevision)
    tombstones.remove(filter)
    desiredSubscriptions[filter] = SubscriptionIntent(qos, noLocal, retainAsPublished, retainHandling, intentRevision)
    val currentTransport = transport
    if (currentTransport == null) {
      desiredSubscriptions.remove(filter)
      return failed(MqttSessionException("ERR_NOT_CONNECTED", "Session transport is not connected"))
    }
    val operationGeneration = generation
    pendingSubscriptionOperations += 1
    val operation = enqueueFilterOperation(filter, serialExecutor) {
      currentTransport.subscribe(filter, qos, noLocal, retainAsPublished, retainHandling)
    }.exceptionally { error -> throw mapProtocolOperationError(error, "subscribe", operationGeneration) }
    val result = operation.thenApplyAsync({ result ->
      if (state != "connected" || generation != operationGeneration) {
        throw MqttSessionException("ERR_CONNECTION_LOST", "Subscription generation changed")
      }
      val current = desiredSubscriptions[filter]
      if (current == null || current.revision != intentRevision) {
        throw MqttSessionException("ERR_OPERATION_SUPERSEDED", "Subscription intent was superseded")
      }
      if (result.grantedQos < 0) {
        desiredSubscriptions.remove(filter)
        subscriptionsStatus = "failed"
        throw MqttSessionException(
          "ERR_SUBSCRIBE_FAILED",
          "Broker rejected subscription",
          reasonCode = result.reasonCode,
          outcome = "rejected",
        )
      }
      current.grantedQos = result.grantedQos
      confirmedSubscriptions[filter] = current.copy()
      revision += 1
      mapOf<String, Any?>("generation" to generation, "intentRevision" to intentRevision, "grantedQos" to result.grantedQos, "reasonCode" to result.reasonCode)
    }, serialExecutor).whenCompleteAsync({ _, error ->
      pendingSubscriptionOperations = (pendingSubscriptionOperations - 1).coerceAtLeast(0)
      if (error != null) subscriptionsStatus = "failed"
      else if (pendingSubscriptionOperations == 0) subscriptionsStatus = "ready"
      settleSubscriptionWaiters()
    }, serialExecutor)
    return trackOperation(result, serialExecutor, "subscribe", filter, operationGeneration)
  }

  fun removeSubscription(filter: String, intentRevision: Long, serialExecutor: ExecutorService): CompletableFuture<Map<String, Any?>> {
    if (state == "closed") return failed(MqttSessionException("ERR_DISPOSED", "Session is closed"))
    subscriptionsRevision = maxOf(subscriptionsRevision, intentRevision)
    val previous = desiredSubscriptions.remove(filter)
    tombstones[filter] = intentRevision
    if (state != "connected") {
      subscriptionsStatus = "pending"
      return CompletableFuture.completedFuture(mapOf("intentRevision" to intentRevision))
    }
    subscriptionsStatus = "pending"
    val currentTransport = transport
    if (currentTransport == null) {
      if (previous != null) desiredSubscriptions[filter] = previous
      return CompletableFuture.completedFuture(mapOf("intentRevision" to intentRevision))
    }
    val operationGeneration = generation
    pendingSubscriptionOperations += 1
    val operation = enqueueFilterOperation(filter, serialExecutor) {
      currentTransport.unsubscribe(filter)
    }.exceptionally { error -> throw mapProtocolOperationError(error, "unsubscribe", operationGeneration) }
    val result = operation.thenApplyAsync({ reasonCode ->
      if (state != "connected" || generation != operationGeneration) {
        throw MqttSessionException("ERR_CONNECTION_LOST", "Unsubscription generation changed")
      }
      val current = desiredSubscriptions[filter]
      if (current != null && current.revision > intentRevision) {
        throw MqttSessionException("ERR_OPERATION_SUPERSEDED", "Unsubscription intent was superseded")
      }
      if (reasonCode >= 128) {
        if (previous != null) desiredSubscriptions[filter] = previous
        tombstones.remove(filter)
        subscriptionsStatus = "failed"
        throw MqttSessionException(
          "ERR_UNSUBSCRIBE_FAILED",
          "Broker rejected unsubscription",
          reasonCode = reasonCode,
          outcome = "rejected",
        )
      }
      tombstones.remove(filter)
      confirmedSubscriptions.remove(filter)
      revision += 1
      mapOf<String, Any?>("intentRevision" to intentRevision)
    }, serialExecutor).whenCompleteAsync({ _, error ->
      pendingSubscriptionOperations = (pendingSubscriptionOperations - 1).coerceAtLeast(0)
      if (error != null) subscriptionsStatus = "failed"
      else if (pendingSubscriptionOperations == 0) subscriptionsStatus = "ready"
      settleSubscriptionWaiters()
    }, serialExecutor)
    return trackOperation(result, serialExecutor, "unsubscribe", filter, operationGeneration)
  }

  private fun restoreSubscriptions(serialExecutor: ExecutorService) {
    val currentTransport = transport ?: return
    val intents = desiredSubscriptions.toMap().entries.toList()
    val batches = mutableListOf<List<Map.Entry<String, SubscriptionIntent>>>()
    var currentBatch = mutableListOf<Map.Entry<String, SubscriptionIntent>>()
    var currentBytes = 0
    var restoreFailed = false
    intents.forEach { entry ->
      val estimatedBytes = entry.key.toByteArray(Charsets.UTF_8).size + 8
      if (estimatedBytes > RESTORE_MAX_PACKET_BYTES) {
        restoreFailed = true
        return@forEach
      }
      if (currentBatch.isNotEmpty() &&
        (currentBatch.size >= RESTORE_MAX_FILTERS || currentBytes + estimatedBytes > RESTORE_MAX_PACKET_BYTES)
      ) {
        batches += currentBatch
        currentBatch = mutableListOf()
        currentBytes = 0
      }
      currentBatch += entry
      currentBytes += estimatedBytes
    }
    if (currentBatch.isNotEmpty()) batches += currentBatch

    var chain = CompletableFuture.completedFuture(Unit)
    if (batches.isEmpty()) {
      subscriptionsStatus = "failed"
      revision += 1
      settleSubscriptionWaiters()
      return
    }
    batches.chunked(RESTORE_MAX_IN_FLIGHT).forEach { wave ->
      chain = chain.handleAsync({ _, error ->
        if (error != null) restoreFailed = true
        Unit
      }, serialExecutor).thenComposeAsync({
        val operations = wave.map { restoreSubscriptionBatch(it, currentTransport, serialExecutor) }
        CompletableFuture.allOf(*operations.toTypedArray()).thenApply { Unit }
      }, serialExecutor)
    }
    chain.handleAsync({ _, error ->
      if (error != null) restoreFailed = true
      if (!restoreFailed && pendingSubscriptionOperations == 0) subscriptionsStatus = "ready"
      else subscriptionsStatus = "failed"
      revision += 1
      settleSubscriptionWaiters()
      Unit
    }, serialExecutor)
  }

  private fun restoreTombstones(serialExecutor: ExecutorService) {
    val currentTransport = transport ?: return
    val removals = tombstones.toMap().entries.toList()
    subscriptionsStatus = "pending"
    pendingSubscriptionOperations += removals.size
    removals.forEach { (filter, intentRevision) ->
      val operationGeneration = generation
      enqueueFilterOperation(filter, serialExecutor) { currentTransport.unsubscribe(filter) }
        .thenApplyAsync({ reasonCode ->
          if (state != "connected" || generation != operationGeneration) {
            throw MqttSessionException("ERR_CONNECTION_LOST", "Tombstone restoration was interrupted")
          }
          if (reasonCode >= 128) {
            throw MqttSessionException("ERR_UNSUBSCRIBE_FAILED", "Broker rejected tombstone", reasonCode = reasonCode, outcome = "rejected")
          }
          if (tombstones[filter] == intentRevision) {
            tombstones.remove(filter)
            confirmedSubscriptions.remove(filter)
          }
          Unit
        }, serialExecutor)
        .whenCompleteAsync({ _, error ->
          pendingSubscriptionOperations = (pendingSubscriptionOperations - 1).coerceAtLeast(0)
          if (error != null) subscriptionsStatus = "failed"
          else if (pendingSubscriptionOperations == 0) subscriptionsStatus = "ready"
          settleSubscriptionWaiters()
        }, serialExecutor)
    }
  }

  /** Serialize one filter's protocol operations so late ACKs cannot cross requests. */
  private fun <T> enqueueFilterOperation(
    filter: String,
    serialExecutor: ExecutorService,
    operation: () -> CompletableFuture<T>,
  ): CompletableFuture<T> {
    val previous = subscriptionTails[filter] ?: CompletableFuture.completedFuture(Unit)
    val next = previous
      .handleAsync({ _, _ -> Unit }, serialExecutor)
      .thenComposeAsync({ operation() }, serialExecutor)
    val tail = next.handleAsync({ _, _ -> Unit }, serialExecutor)
    subscriptionTails[filter] = tail
    tail.whenCompleteAsync({ _, _ ->
      if (subscriptionTails[filter] === tail) subscriptionTails.remove(filter)
    }, serialExecutor)
    return next
  }

  private fun restoreSubscriptionBatch(
    batch: List<Map.Entry<String, SubscriptionIntent>>,
    currentTransport: MqttTransport,
    serialExecutor: ExecutorService,
  ): CompletableFuture<Unit> {
    pendingSubscriptionOperations += batch.size
    val requests = batch.map { (filter, intent) ->
      MqttSubscriptionRequest(filter, intent.qos, intent.noLocal, intent.retainAsPublished, intent.retainHandling)
    }
    return currentTransport.subscribeBatch(requests)
      .thenApplyAsync({ results ->
        if (state != "connected") throw MqttSessionException("ERR_CONNECTION_LOST", "Subscription restore was interrupted")
        var failed = results.size != batch.size
        results.take(batch.size).forEachIndexed { index, result ->
          val (filter, intent) = batch[index]
          val current = desiredSubscriptions[filter]
          if (current == null || current.revision != intent.revision) return@forEachIndexed
          if (result.grantedQos < 0) {
            failed = true
          } else {
            current.grantedQos = result.grantedQos
            confirmedSubscriptions[filter] = current.copy()
          }
        }
        if (failed) throw MqttSessionException("ERR_SUBSCRIBE_FAILED", "Broker returned a partial or rejected SUBACK")
        Unit
      }, serialExecutor)
      .whenCompleteAsync({ _, error ->
        pendingSubscriptionOperations = (pendingSubscriptionOperations - batch.size).coerceAtLeast(0)
        if (error != null) subscriptionsStatus = "failed"
        settleSubscriptionWaiters()
      }, serialExecutor)
  }

  private companion object {
    const val RESTORE_MAX_FILTERS = 32
    const val RESTORE_MAX_PACKET_BYTES = 16 * 1024
    const val RESTORE_MAX_IN_FLIGHT = 4
  }

  fun awaitSubscriptions(generation: Long, desiredRevision: Long, serialExecutor: ExecutorService): CompletableFuture<Map<String, Any?>> {
    if (state != "connected") return failed(MqttSessionException("ERR_NOT_CONNECTED", "Session is not connected"))
    if (this.generation != generation) return failed(MqttSessionException("ERR_CONNECTION_LOST", "Connection generation changed"))
    if (subscriptionsStatus == "failed") return failed(MqttSessionException("ERR_SUBSCRIBE_FAILED", "Subscription synchronization failed"))
    if (subscriptionsRevision >= desiredRevision && subscriptionsStatus == "ready") return CompletableFuture.completedFuture(snapshot())
    val waiter = CompletableFuture<Map<String, Any?>>().also {
      subscriptionWaiters += SubscriptionWaiter(generation, desiredRevision, it)
    }
    return trackOperation(waiter, serialExecutor, "ready", null, generation)
  }

  private fun settleSubscriptionWaiters() {
    val iterator = subscriptionWaiters.iterator()
    while (iterator.hasNext()) {
      val waiter = iterator.next()
      when {
        state != "connected" || generation != waiter.generation -> {
          waiter.future.completeExceptionally(MqttSessionException("ERR_CONNECTION_LOST", "Connection generation changed"))
          iterator.remove()
        }
        subscriptionsStatus == "failed" -> {
          waiter.future.completeExceptionally(MqttSessionException("ERR_SUBSCRIBE_FAILED", "Subscription synchronization failed"))
          iterator.remove()
        }
        subscriptionsStatus == "ready" && subscriptionsRevision >= waiter.desiredRevision -> {
          waiter.future.complete(snapshot())
          iterator.remove()
        }
      }
    }
  }

  fun publish(topic: String, payload: ByteArray, qos: Int, retained: Boolean, properties: MqttPublishProperties = MqttPublishProperties(), serialExecutor: ExecutorService): CompletableFuture<Void> {
    if (state != "connected") return failed(MqttSessionException("ERR_NOT_CONNECTED", "Session is not connected"))
    val currentTransport = transport
      ?: return failed(MqttSessionException("ERR_NOT_CONNECTED", "Session transport is not connected"))
    val operationGeneration = generation
    val result: CompletableFuture<Void> = currentTransport.publish(topic, payload, qos, retained, properties)
      .handle<Void> { _, error ->
        if (error != null) {
          throw mapProtocolOperationError(error, "publish", operationGeneration)
        }
        null
      }
    return trackOperation(result, serialExecutor, "publish", topic, operationGeneration)
  }

  fun close(): Map<String, Any?> {
    if (state != "closed") {
      stopRequested = true
      attemptId += 1
      connecting?.cancel(true)
      connecting = null
      rejectReconnectWaiters(MqttSessionException("ERR_DISPOSED", "Session was closed"))
      cancelPendingOperations(MqttSessionException("ERR_DISPOSED", "Session was closed"))
      transport?.disconnect()
      state = "closed"
      revision += 1
      settleSubscriptionWaiters()
    }
    return snapshot()
  }

  fun close(serialExecutor: ExecutorService): CompletableFuture<Map<String, Any?>> {
    if (state == "closed") return CompletableFuture.completedFuture(snapshot())
    stopRequested = true
    attemptId += 1
    connecting?.cancel(true)
    connecting = null
    state = "disconnecting"
    revision += 1
    cancelPendingOperations(MqttSessionException("ERR_DISPOSED", "Session was closed"))
    settleSubscriptionWaiters()
    val disconnect = transport?.disconnect() ?: CompletableFuture.completedFuture(null)
    return disconnect.thenApplyAsync({
      state = "closed"
      revision += 1
      settleSubscriptionWaiters()
      snapshot()
    }, serialExecutor)
  }

  private fun <T> failed(error: Throwable): CompletableFuture<T> = CompletableFuture<T>().also { it.completeExceptionally(error) }

  private fun mapProtocolOperationError(error: Throwable, operation: String, operationGeneration: Long): MqttSessionException {
    val cause = findMqttSessionException(error) ?: (error.cause ?: error)
    val protocolFailure = when (cause) {
      is org.eclipse.paho.mqttv5.common.MqttException -> Triple("ERR_${operation.uppercase()}_FAILED", cause.reasonCode, null)
      is org.eclipse.paho.client.mqttv3.MqttException -> Triple("ERR_${operation.uppercase()}_FAILED", cause.reasonCode, null)
      else -> null
    }
    if (cause is MqttSessionException) return cause
    return MqttSessionException(
      protocolFailure?.first ?: when (operation) {
        "subscribe" -> "ERR_SUBSCRIBE_FAILED"
        "unsubscribe" -> "ERR_UNSUBSCRIBE_FAILED"
        else -> "ERR_PUBLISH_FAILED"
      },
      cause.message ?: "MQTT $operation failed",
      cause = cause,
      operation = operation,
      generation = operationGeneration,
      reasonCode = protocolFailure?.second,
      reasonString = protocolFailure?.third,
      outcome = if (protocolFailure != null) "rejected" else "unknown",
    )
  }

  @Synchronized
  private fun <T> trackOperation(
    operation: CompletableFuture<T>,
    serialExecutor: ExecutorService,
    operationType: String,
    topic: String?,
    operationGeneration: Long,
  ): CompletableFuture<T> {
    val operationId = ++nextOperationId
    val entry = OperationLedgerEntry(operationId, operationGeneration, operationType, topic)
    operationLedger[operationId] = entry
    pendingOperationFutures += operation
    operation.whenCompleteAsync({ _, _ ->
      synchronized(this) {
        // Completion, timeout, disconnect and dispose all converge here. The
        // settled bit makes the ledger's removal explicitly idempotent even
        // when a source future invokes its callback more than once.
        if (!entry.settled) {
          entry.settled = true
          operationLedger.remove(operationId)
        }
        pendingOperationFutures.remove(operation)
      }
    }, serialExecutor)
    return operation
  }

  @Synchronized
  private fun cancelPendingOperations(error: Throwable) {
    pendingOperationFutures.toList().forEach { it.completeExceptionally(error) }
  }

  fun sameOptions(other: ParsedMqttOptions): Boolean = options == other

  fun connectTimeoutMs(): Long = options.connectTimeoutMs

  fun operationTimeoutMs(): Long = options.operationTimeoutMs

  fun disconnectTimeoutMs(): Long = options.disconnectTimeoutMs

  fun reconnectEnabled(): Boolean = options.reconnectEnabled

  @Synchronized
  fun pendingOperations(): Int = pendingOperationFutures.size

  @Synchronized
  fun operationLedgerSnapshot(): List<Map<String, Any?>> = operationLedger.values.map { entry ->
    mapOf(
      "operationId" to entry.operationId,
      "generation" to entry.generation,
      "operation" to entry.operation,
      "topic" to entry.topic,
      "settled" to entry.settled,
    )
  }

  fun reconnectAttemptsCount(): Long = reconnectAttempts

  fun reconnectDelayMs(): Long {
    reconnectAttempts += 1
    val exponent = (reconnectAttempts - 1).coerceAtMost(30).toInt()
    val base = (options.reconnectInitialDelayMs * (1L shl exponent)).coerceAtMost(options.reconnectMaxDelayMs)
    val spread = (base.toDouble() * options.reconnectJitterRatio).toLong()
    return if (spread == 0L) base else (base + ThreadLocalRandom.current().nextLong(-spread, spread + 1)).coerceAtLeast(1L)
  }

  fun stopReconnecting() {
    if (state == "reconnecting") {
      state = "disconnected"
      revision += 1
      settleSubscriptionWaiters()
    }
    rejectReconnectWaiters(MqttSessionException("ERR_CONNECTION_LOST", "Automatic reconnect is disabled"))
    reconnectAttempts = 0
  }

  fun cancelReconnectWaiter(waiter: CompletableFuture<Map<String, Any?>>, error: Throwable) {
    if (reconnectWaiters.remove(waiter)) waiter.completeExceptionally(error)
  }

  fun forceDisconnect() {
    if (state != "closed" && state != "disconnected") {
      state = "disconnected"
      revision += 1
      settleSubscriptionWaiters()
    }
    stopRequested = true
    attemptId += 1
    connecting?.cancel(true)
    connecting = null
    rejectReconnectWaiters(MqttSessionException("ERR_CONNECTION_LOST", "Connection was stopped"))
  }

  fun forceClose() {
    stopRequested = true
    attemptId += 1
    connecting?.cancel(true)
    connecting = null
    rejectReconnectWaiters(MqttSessionException("ERR_DISPOSED", "Session was closed"))
    if (state != "closed") {
      state = "closed"
      revision += 1
      settleSubscriptionWaiters()
    }
  }

  private fun rejectReconnectWaiters(error: Throwable) {
    reconnectWaiters.forEach { it.completeExceptionally(error) }
    reconnectWaiters.clear()
  }

  fun isReconnecting(): Boolean = state == "reconnecting"

  private var reconnectAttempts = 0L
}

internal class MqttSessionRegistry {
  constructor(
    onMessage: (String, Long, MqttMessageData) -> Unit = { _, _, _ -> },
    transportFactory: ((ParsedMqttOptions, (MqttMessageData) -> Unit) -> MqttTransport)? = null,
    onTransportLost: (String, Long, MqttDisconnect?) -> Unit = { _, _, _ -> },
    onSessionSnapshot: (Map<String, Any?>) -> Unit = {},
    scheduler: MqttScheduler = ExecutorMqttScheduler(),
  ) {
    this.onMessage = onMessage
    this.transportFactory = transportFactory
    this.onTransportLost = onTransportLost
    this.onSessionSnapshot = onSessionSnapshot
    this.scheduler = scheduler
  }

  private val onMessage: (String, Long, MqttMessageData) -> Unit
  private val transportFactory: ((ParsedMqttOptions, (MqttMessageData) -> Unit) -> MqttTransport)?
  private val onTransportLost: (String, Long, MqttDisconnect?) -> Unit
  private val onSessionSnapshot: (Map<String, Any?>) -> Unit
  private val executor: ExecutorService = Executors.newSingleThreadExecutor { runnable ->
    Thread(runnable, "react-native-mqtt-registry").apply { isDaemon = true }
  }
  private val scheduler: MqttScheduler
  private val sessions = ConcurrentHashMap<String, MqttSession>()
  private val reconnectTasks = ConcurrentHashMap<String, MqttTimer>()
  private val invalidated = AtomicBoolean(false)

  fun open(instanceId: String, options: ParsedMqttOptions): CompletableFuture<Map<String, Any?>> {
    val result = CompletableFuture<Map<String, Any?>>()
    try {
      executor.execute {
      if (invalidated.get()) {
        result.completeExceptionally(MqttSessionException("ERR_DISPOSED", "Native module is invalidated"))
      } else {
        val existing = sessions[instanceId]
        if (existing != null && !existing.sameOptions(options)) {
          result.completeExceptionally(MqttSessionException("ERR_OPERATION_IN_PROGRESS", "Session options cannot change"))
        } else {
          val session = existing ?: MqttSession(instanceId, options, onMessage = { generation, message ->
            try {
              executor.execute { if (!invalidated.get() && sessions.containsKey(instanceId)) onMessage(instanceId, generation, message) }
            } catch (_: RejectedExecutionException) {
              // Invalidation owns teardown; late SDK callbacks are ignored.
            }
          }, transportFactory = transportFactory, onTransportLost = { generation, reason ->
            onTransportLost(instanceId, generation, reason)
            try {
              executor.execute { scheduleReconnect(instanceId) }
            } catch (_: RejectedExecutionException) {
              // Registry teardown owns the terminal state.
            }
          }).also { sessions[instanceId] = it }
          result.complete(session.snapshot())
        }
      }
      }
    } catch (error: RejectedExecutionException) {
      result.completeExceptionally(MqttSessionException("ERR_DISPOSED", "Native module is invalidated", error))
    }
    return result
  }

  fun snapshot(instanceId: String): CompletableFuture<Map<String, Any?>> = submit {
    sessions[instanceId]?.snapshot() ?: closedSnapshot(instanceId)
  }

  fun pendingOperations(instanceId: String): Int = sessions[instanceId]?.pendingOperations() ?: 0

  fun operationLedger(instanceId: String): List<Map<String, Any?>> = sessions[instanceId]?.operationLedgerSnapshot() ?: emptyList()

  fun reconnectAttempts(instanceId: String): Long = sessions[instanceId]?.reconnectAttemptsCount() ?: 0

  fun close(instanceId: String): CompletableFuture<Map<String, Any?>> = submitFuture {
    val session = sessions[instanceId] ?: return@submitFuture CompletableFuture.completedFuture(closedSnapshot(instanceId))
    reconnectTasks.remove(instanceId)?.cancel()
    val result = withDeadline(
      session.close(executor),
      session.disconnectTimeoutMs(),
      "ERR_OPERATION_TIMEOUT",
      "Close timed out",
      onTimeout = { session.forceClose() },
    )
    result.whenComplete { _, _ ->
      sessions.remove(instanceId, session)
      reconnectTasks.remove(instanceId)?.cancel()
    }
    result
  }

  fun connect(instanceId: String): CompletableFuture<Map<String, Any?>> {
    val result = CompletableFuture<Map<String, Any?>>()
    try {
      executor.execute {
        if (invalidated.get()) {
          result.completeExceptionally(MqttSessionException("ERR_DISPOSED", "Native module is invalidated"))
          return@execute
        }
        val session = sessions[instanceId]
        if (session == null) {
          result.completeExceptionally(MqttSessionException("ERR_NOT_CONNECTED", "Session is not open"))
          return@execute
        }
        if (!session.isReconnecting()) reconnectTasks.remove(instanceId)?.cancel()
        var timeout: MqttTimer? = null
        try {
          val connection = session.connect(executor)
          timeout = scheduler.schedule(session.connectTimeoutMs()) {
            executor.execute {
              if (!result.isDone) {
                val timeoutError = MqttSessionException("ERR_CONNECT_TIMEOUT", "Connection timed out")
                session.timeoutConnect()
                session.cancelReconnectWaiter(connection, timeoutError)
                result.completeExceptionally(timeoutError)
              }
            }
          }
          connection.whenComplete { value, error ->
            executor.execute {
              timeout?.cancel()
              if (error == null) {
                result.complete(value)
              } else {
                onSessionSnapshot(session.snapshot())
                result.completeExceptionally(error)
              }
            }
          }
        } catch (error: Throwable) {
          timeout?.cancel()
          onSessionSnapshot(session.snapshot())
          result.completeExceptionally(error)
        }
      }
    } catch (error: RejectedExecutionException) {
      result.completeExceptionally(MqttSessionException("ERR_DISPOSED", "Native module is invalidated", error))
    }
    return result
  }

  fun disconnect(instanceId: String): CompletableFuture<Map<String, Any?>> = submitFuture {
    reconnectTasks.remove(instanceId)?.cancel()
    val session = sessions[instanceId]
    if (session == null) CompletableFuture.completedFuture(closedSnapshot(instanceId))
    else withDeadline(
      session.disconnect(executor),
      session.disconnectTimeoutMs(),
      "ERR_OPERATION_TIMEOUT",
      "Disconnect timed out",
      onTimeout = { session.forceDisconnect() },
    )
  }

  fun setSubscription(instanceId: String, filter: String, qos: Int, noLocal: Boolean, retainAsPublished: Boolean, retainHandling: Int, intentRevision: Long): CompletableFuture<Map<String, Any?>> =
    submitFuture {
      val session = sessions[instanceId]
      if (session == null) failedFuture(MqttSessionException("ERR_NOT_CONNECTED", "Session is not open"))
      else withDeadline(
        session.setSubscription(filter, qos, noLocal, retainAsPublished, retainHandling, intentRevision, executor),
        session.operationTimeoutMs(),
        "ERR_OPERATION_TIMEOUT",
        "Subscription operation timed out",
      )
    }

  fun removeSubscription(instanceId: String, filter: String, intentRevision: Long): CompletableFuture<Map<String, Any?>> =
    submitFuture {
      val session = sessions[instanceId]
      if (session == null) failedFuture(MqttSessionException("ERR_NOT_CONNECTED", "Session is not open"))
      else withDeadline(
        session.removeSubscription(filter, intentRevision, executor),
        session.operationTimeoutMs(),
        "ERR_OPERATION_TIMEOUT",
        "Unsubscription operation timed out",
      )
    }

  fun awaitSubscriptions(instanceId: String, generation: Long, desiredRevision: Long): CompletableFuture<Map<String, Any?>> =
    submitFuture {
      val session = sessions[instanceId]
      if (session == null) failedFuture(MqttSessionException("ERR_NOT_CONNECTED", "Session is not open"))
      else withDeadline(
        session.awaitSubscriptions(generation, desiredRevision, executor),
        session.operationTimeoutMs(),
        "ERR_OPERATION_TIMEOUT",
        "Subscription readiness timed out",
      )
    }

  fun publish(instanceId: String, topic: String, payload: ByteArray, qos: Int, retained: Boolean, properties: MqttPublishProperties = MqttPublishProperties()): CompletableFuture<Void> {
    val result = CompletableFuture<Void>()
    try {
      executor.execute {
        if (invalidated.get()) result.completeExceptionally(MqttSessionException("ERR_DISPOSED", "Native module is invalidated"))
        else {
          val session = sessions[instanceId]
          if (session == null) result.completeExceptionally(MqttSessionException("ERR_NOT_CONNECTED", "Session is not open"))
          else withDeadline(
            session.publish(topic, payload, qos, retained, properties, executor),
            session.operationTimeoutMs(),
            "ERR_OPERATION_TIMEOUT",
            "Publish operation timed out",
          ).whenComplete { value, error ->
            executor.execute { if (error == null) result.complete(value) else result.completeExceptionally(error) }
          }
        }
      }
    } catch (error: RejectedExecutionException) {
      result.completeExceptionally(MqttSessionException("ERR_DISPOSED", "Native module is invalidated", error))
    }
    return result
  }

  fun invalidate() {
    if (!invalidated.compareAndSet(false, true)) return
    executor.execute {
      reconnectTasks.values.forEach { it.cancel() }
      reconnectTasks.clear()
      sessions.values.forEach { it.close() }
      sessions.clear()
      executor.shutdown()
      scheduler.shutdown()
    }
  }

  private fun scheduleReconnect(instanceId: String) {
    val session = sessions[instanceId] ?: return
    if (!session.reconnectEnabled()) {
      session.stopReconnecting()
      return
    }
    val existing = reconnectTasks[instanceId]
    if (existing != null && !existing.isDone()) return
    val delay = session.reconnectDelayMs()
    reconnectTasks[instanceId] = scheduler.schedule(delay) {
      try {
        executor.execute {
          reconnectTasks.remove(instanceId)
          val current = sessions[instanceId] ?: return@execute
          current.connect(executor, forceAttempt = true).whenComplete { _, error ->
            executor.execute {
              if (error != null && sessions[instanceId] === current && current.reconnectEnabled()) scheduleReconnect(instanceId)
            }
          }
        }
      } catch (_: RejectedExecutionException) {
        // Registry teardown owns the terminal state.
      }
    }
  }

  private fun submit(block: () -> Map<String, Any?>): CompletableFuture<Map<String, Any?>> {
    val result = CompletableFuture<Map<String, Any?>>()
    try {
      executor.execute {
        if (invalidated.get()) result.completeExceptionally(MqttSessionException("ERR_DISPOSED", "Native module is invalidated"))
        else runCatching(block).onSuccess(result::complete).onFailure(result::completeExceptionally)
      }
    } catch (error: RejectedExecutionException) {
      result.completeExceptionally(MqttSessionException("ERR_DISPOSED", "Native module is invalidated", error))
    }
    return result
  }

  private fun submitFuture(block: () -> CompletableFuture<Map<String, Any?>>): CompletableFuture<Map<String, Any?>> {
    val result = CompletableFuture<Map<String, Any?>>()
    try {
      executor.execute {
        if (invalidated.get()) result.completeExceptionally(MqttSessionException("ERR_DISPOSED", "Native module is invalidated"))
        else try { block().whenComplete { value, error -> if (error == null) result.complete(value) else result.completeExceptionally(error) } } catch (error: Throwable) { result.completeExceptionally(error) }
      }
    } catch (error: RejectedExecutionException) { result.completeExceptionally(MqttSessionException("ERR_DISPOSED", "Native module is invalidated", error)) }
    return result
  }

  private fun <T> withDeadline(
    source: CompletableFuture<T>,
    timeoutMs: Long,
    errorCode: String,
    message: String,
    onTimeout: () -> Unit = {},
  ): CompletableFuture<T> {
    val result = CompletableFuture<T>()
    val timeout = scheduler.schedule(timeoutMs) {
      if (result.completeExceptionally(MqttSessionException(errorCode, message))) {
        onTimeout()
        source.cancel(true)
      }
    }
    source.whenComplete { value, error ->
      timeout.cancel()
      if (error == null) result.complete(value) else result.completeExceptionally(error)
    }
    return result
  }

  private fun closedSnapshot(instanceId: String): Map<String, Any?> = mapOf(
    "instanceId" to instanceId,
    "revision" to 0L,
    "generation" to 0L,
    "state" to "closed",
    "lastConnAck" to null,
    "subscriptionsRevision" to 0L,
    "subscriptionsStatus" to "ready",
    "deliveryStatus" to "closed",
  )

  private fun <T> failedFuture(error: Throwable): CompletableFuture<T> = CompletableFuture<T>().also { it.completeExceptionally(error) }
}

internal class MqttSessionException(
  val errorCode: String,
  message: String,
  cause: Throwable? = null,
  val operation: String? = null,
  val generation: Long? = null,
  val reasonCode: Int? = null,
  val reasonString: String? = null,
  val outcome: String? = null,
) : RuntimeException(message, cause)
