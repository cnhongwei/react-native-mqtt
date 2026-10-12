package com.cnhongwei.reactnativemqtt

import java.util.concurrent.CompletableFuture

/** Deterministic monotonic clock for native-session scenario tests. */
internal class NativeScenarioClock {
  private data class Timer(val at: Long, val order: Long, val task: () -> Unit, var cancelled: Boolean = false)

  var nowMs: Long = 0
    private set
  private var nextOrder = 0L
  private val timers = mutableListOf<Timer>()

  fun schedule(delayMs: Long, task: () -> Unit): NativeScenarioTimer {
    require(delayMs >= 0) { "scenario timer delay must be non-negative" }
    val timer = Timer(nowMs + delayMs, nextOrder++, task)
    timers += timer
    return NativeScenarioTimer { timer.cancelled = true }
  }

  fun advanceBy(deltaMs: Long) {
    require(deltaMs >= 0) { "scenario clock cannot move backwards" }
    advanceTo(nowMs + deltaMs)
  }

  fun advanceTo(targetMs: Long) {
    require(targetMs >= nowMs) { "scenario clock cannot move backwards" }
    while (true) {
      val next = timers
        .filter { !it.cancelled && it.at <= targetMs }
        .minWithOrNull(compareBy<Timer> { it.at }.thenBy { it.order })
        ?: break
      timers.remove(next)
      nowMs = next.at
      next.task()
    }
    nowMs = targetMs
  }
}

internal class NativeScenarioTimer(private val cancelAction: () -> Unit) {
  fun cancel() = cancelAction()
}

/** Callback-controlled transport; tests explicitly decide completion and order. */
internal class NativeScenarioTransport : MqttTransport {
  var connectAttempts = 0
    private set
  var disconnectAttempts = 0
    private set
  val publishAttempts = mutableListOf<Pair<String, ByteArray>>()
  val connectPending = mutableListOf<CompletableFuture<MqttConnAck>>()
  val publishPending = mutableListOf<CompletableFuture<Void>>()
  val disconnectPending = mutableListOf<CompletableFuture<Void>>()
  var messageHandler: ((MqttMessageData) -> Unit)? = null
  var disconnectHandler: ((MqttDisconnect?) -> Unit)? = null

  override fun connect(): CompletableFuture<MqttConnAck> {
    connectAttempts += 1
    return CompletableFuture<MqttConnAck>().also { connectPending += it }
  }

  override fun disconnect(): CompletableFuture<Void> {
    disconnectAttempts += 1
    return CompletableFuture<Void>().also { disconnectPending += it }
  }

  override fun subscribe(
    filter: String,
    qos: Int,
    noLocal: Boolean,
    retainAsPublished: Boolean,
    retainHandling: Int,
    onMessage: ((MqttMessageData) -> Unit)?,
  ): CompletableFuture<MqttSubscriptionResult> = CompletableFuture.completedFuture(MqttSubscriptionResult(qos, 0))

  override fun unsubscribe(filter: String): CompletableFuture<Int> = CompletableFuture.completedFuture(0)

  override fun publish(topic: String, payload: ByteArray, qos: Int, retained: Boolean): CompletableFuture<Void> {
    publishAttempts += topic to payload.copyOf()
    return CompletableFuture<Void>().also { publishPending += it }
  }

  override fun setDisconnectedListener(listener: (MqttDisconnect?) -> Unit) {
    disconnectHandler = listener
  }

  fun repeatConnectCallback(index: Int, ack: MqttConnAck = MqttConnAck(false, 0)) {
    connectPending[index].complete(ack)
  }

  fun completeConnect(index: Int = 0, ack: MqttConnAck = MqttConnAck(false, 0)) {
    connectPending.removeAt(index).complete(ack)
  }

  fun failConnect(index: Int = 0, error: Throwable = IllegalStateException("connect failed")) {
    connectPending.removeAt(index).completeExceptionally(error)
  }

  fun completePublish(index: Int = 0) {
    publishPending.removeAt(index).complete(null)
  }

  fun failPublish(index: Int = 0, error: Throwable = IllegalStateException("publish failed")) {
    publishPending.removeAt(index).completeExceptionally(error)
  }

  fun completeDisconnect(index: Int = 0) {
    disconnectPending.removeAt(index).complete(null)
  }

  fun emitMessageAfter(clock: NativeScenarioClock, delayMs: Long, message: MqttMessageData) {
    clock.schedule(delayMs) { messageHandler?.invoke(message) }
  }

  fun emitDisconnectAfter(clock: NativeScenarioClock, delayMs: Long, reason: MqttDisconnect?) {
    clock.schedule(delayMs) { disconnectHandler?.invoke(reason) }
  }
}

internal sealed interface NativeScenarioAction {
  data object Connect : NativeScenarioAction
  data class CompleteConnect(val index: Int = 0, val ack: MqttConnAck = MqttConnAck(false, 0)) : NativeScenarioAction
  data class FailConnect(val index: Int = 0, val error: Throwable = IllegalStateException("connect failed")) : NativeScenarioAction
  data class DelayedMessage(val delayMs: Long, val message: MqttMessageData) : NativeScenarioAction
  data class DelayedDisconnect(val delayMs: Long, val reason: MqttDisconnect?) : NativeScenarioAction
  data class Advance(val deltaMs: Long) : NativeScenarioAction
}

/** Applies the same transport-only action vocabulary as the iOS runner. */
internal class NativeScenarioRunner(
  val clock: NativeScenarioClock = NativeScenarioClock(),
  val transport: NativeScenarioTransport = NativeScenarioTransport(),
) {
  fun run(actions: List<NativeScenarioAction>): NativeScenarioRunner {
    actions.forEach { action ->
      when (action) {
        NativeScenarioAction.Connect -> transport.connect()
        is NativeScenarioAction.CompleteConnect -> transport.completeConnect(action.index, action.ack)
        is NativeScenarioAction.FailConnect -> transport.failConnect(action.index, action.error)
        is NativeScenarioAction.DelayedMessage -> transport.emitMessageAfter(clock, action.delayMs, action.message)
        is NativeScenarioAction.DelayedDisconnect -> transport.emitDisconnectAfter(clock, action.delayMs, action.reason)
        is NativeScenarioAction.Advance -> clock.advanceBy(action.deltaMs)
      }
    }
    return this
  }
}
