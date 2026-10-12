package com.cnhongwei.reactnativemqtt

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class NativeSessionScenarioSupportTest {
  @Test
  fun virtualClockOrdersSameTimeCallbacksAndHonorsCancellation() {
    val clock = NativeScenarioClock()
    val transport = NativeScenarioTransport()
    val events = mutableListOf<String>()
    val cancelled = clock.schedule(10) { events += "cancelled" }
    transport.messageHandler = { events += "message:${it.topic}" }
    transport.disconnectHandler = { events += "disconnect" }
    transport.emitMessageAfter(clock, 5, MqttMessageData("late", byteArrayOf(1), 1, false))
    transport.emitDisconnectAfter(clock, 5, null)
    cancelled.cancel()

    clock.advanceBy(5)
    assertEquals(listOf("message:late", "disconnect"), events)
    clock.advanceBy(5)
    assertEquals(listOf("message:late", "disconnect"), events)
    assertEquals(10L, clock.nowMs)
  }

  @Test
  fun transportAllowsOutOfOrderAndRepeatedConnectCallbacks() {
    val transport = NativeScenarioTransport()
    val results = mutableListOf<String>()
    transport.connect().whenComplete { _, error -> results += if (error == null) "first" else "failed" }
    transport.connect().whenComplete { _, error -> results += if (error == null) "second" else "failed" }

    transport.completeConnect(1)
    transport.failConnect(0)
    assertEquals(listOf("second", "failed"), results)

    val stale = transport.connect()
    transport.repeatConnectCallback(0)
    assertTrue(stale.isDone)
    assertEquals(3, transport.connectAttempts)
  }

  @Test
  fun transportSupportsImmediatePublishFailureAndDelayedCompletion() {
    val transport = NativeScenarioTransport()
    val delayed = transport.publish("topic", byteArrayOf(1, 2), 1, false)
    val immediate = java.util.concurrent.CompletableFuture<Void>().also {
      it.completeExceptionally(IllegalStateException("send failed"))
    }
    assertTrue(immediate.isCompletedExceptionally)
    assertTrue(!delayed.isDone)
    transport.completePublish()
    assertTrue(delayed.isDone)
    assertEquals(byteArrayOf(1, 2).toList(), transport.publishAttempts.single().second.toList())
  }

  @Test
  fun runnerAppliesSharedActionVocabulary() {
    val runner = NativeScenarioRunner()
    val events = mutableListOf<String>()
    runner.transport.messageHandler = { events += it.topic }
    runner.run(
      listOf(
        NativeScenarioAction.DelayedMessage(20, MqttMessageData("late", byteArrayOf(1), 1, false)),
        NativeScenarioAction.DelayedMessage(10, MqttMessageData("early", byteArrayOf(2), 1, false)),
        NativeScenarioAction.Advance(10),
      ),
    )
    assertEquals(listOf("early"), events)
    runner.run(listOf(NativeScenarioAction.Advance(10)))
    assertEquals(listOf("early", "late"), events)
  }
}
