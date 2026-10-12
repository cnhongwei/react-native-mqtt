package com.cnhongwei.reactnativemqtt

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertTrue
import org.junit.Test

class DeliveryStateTest {
  private val options = ParsedMqttOptions(
    host = "broker.example",
    port = 1883,
    protocolVersion = 5,
    transport = "tcp",
    maxQueuedMessages = 2,
    maxQueuedBytes = 8,
    maxMessageBytes = 4,
    overflowPolicy = "drop-newest",
  )

  private fun message(payload: ByteArray) = MqttMessageData("topic", payload, 1, false)

  @Test
  fun usesBridgeBatchDefaultsDistinctFromRestoreBatchLimits() {
    val defaults = ParsedMqttOptions(host = "broker.example", port = 1883, protocolVersion = 5, transport = "tcp")
    assertEquals(64, defaults.batchMaxMessages)
    assertEquals(256 * 1024, defaults.batchMaxBytes)
    assertEquals("disconnect", defaults.overflowPolicy)
  }

  @Test
  fun coalescesOverflowDiagnosticsUntilTheInFlightBatchIsAcknowledged() {
    val state = DeliveryState(options.copy(maxQueuedMessages = 1))
    assertTrue(state.enqueue(1, message(byteArrayOf(1))))
    assertFalse(state.enqueue(1, message(byteArrayOf(2))))
    assertTrue(state.takeOverflowDiagnostic())
    assertFalse(state.takeOverflowDiagnostic())
    assertEquals(1L, state.recordDrop())
    assertEquals(2L, state.recordDrop())

    state.acknowledge()
    assertTrue(state.takeOverflowDiagnostic())
  }

  @Test
  fun clearResetsQueueBytesDropsAndDiagnosticCoalescing() {
    val state = DeliveryState(options.copy(maxQueuedMessages = 1))
    assertTrue(state.enqueue(1, message(byteArrayOf(1, 2))))
    assertFalse(state.enqueue(1, message(byteArrayOf(3))))
    assertEquals(1L, state.recordDrop())
    assertTrue(state.takeOverflowDiagnostic())

    state.clear()

    assertEquals(0, state.queuedMessages())
    assertEquals(0L, state.queuedBytes())
    assertEquals(1L, state.recordDrop())
    assertTrue(state.takeOverflowDiagnostic())
  }

  @Test
  fun keepsOneInFlightAndBoundsQueuedMessagesAndBytes() {
    val state = DeliveryState(options)
    assertTrue(state.enqueue(1, message(byteArrayOf(1, 2))))
    assertTrue(state.enqueue(1, message(byteArrayOf(3))))
    assertFalse(state.enqueue(1, message(byteArrayOf(4, 5))))

    val first = state.startNext()
    assertNotNull(first)
    assertEquals(2, first!!.size)
    assertEquals(2, state.queuedMessages())
    assertEquals(3L, state.queuedBytes())
    assertTrue(state.startNext() == null)

    state.acknowledge()
    assertTrue(state.startNext() == null)
  }

  @Test
  fun countsOversizeDropsWithoutRetainingPayload() {
    val state = DeliveryState(options)
    assertFalse(state.enqueue(1, message(byteArrayOf(1, 2, 3, 4, 5))))
    assertEquals(1L, state.recordDrop())
    assertTrue(state.takeOverflowDiagnostic())
    assertEquals(2L, state.recordDrop())
    assertFalse(state.takeOverflowDiagnostic())
    assertEquals(0, state.queuedMessages())
    assertEquals(0L, state.queuedBytes())
    state.acknowledge()
    assertEquals(3L, state.recordDrop())
    assertTrue(state.takeOverflowDiagnostic())
  }

  @Test
  fun keepsInFlightBytesInsideTheConfiguredMemoryBudget() {
    val state = DeliveryState(options.copy(maxQueuedBytes = 5))
    assertTrue(state.enqueue(1, message(byteArrayOf(1, 2))))
    val first = state.startNext()!!
    assertEquals(listOf<Byte>(1, 2), first.single().second.payload.toList())
    assertFalse(state.enqueue(1, message(byteArrayOf(3, 4, 5, 6))))
    assertEquals(1, state.queuedMessages())
    assertEquals(2L, state.queuedBytes())
    state.acknowledge()
    assertEquals(0, state.queuedMessages())
    assertEquals(0L, state.queuedBytes())
  }

  @Test
  fun splitsBatchesByBytesWithoutReorderingMessages() {
    val state = DeliveryState(
      options.copy(
        maxQueuedMessages = 4,
        maxQueuedBytes = 16,
        batchMaxMessages = 4,
        batchMaxBytes = 3,
      ),
    )
    assertTrue(state.enqueue(7, message(byteArrayOf(1, 2))))
    assertTrue(state.enqueue(7, message(byteArrayOf(3))))
    assertTrue(state.enqueue(7, message(byteArrayOf(4, 5))))

    val first = state.startNext()!!
    assertEquals(listOf<Byte>(1, 2, 3), first.flatMap { it.second.payload.toList() })
    assertEquals(listOf(7L, 7L), first.map { it.first })
    state.acknowledge()

    val second = state.startNext()!!
    assertEquals(listOf<Byte>(4, 5), second.single().second.payload.toList())
    state.acknowledge()
    assertEquals(0, state.queuedMessages())
  }

  @Test
  fun allowsOneMessageAboveBatchByteLimitAsItsOwnBatch() {
    val state = DeliveryState(
      options.copy(
        maxQueuedMessages = 2,
        maxQueuedBytes = 8,
        maxMessageBytes = 4,
        batchMaxMessages = 2,
        batchMaxBytes = 2,
      ),
    )
    assertTrue(state.enqueue(3, message(byteArrayOf(1, 2, 3))))
    assertTrue(state.enqueue(3, message(byteArrayOf(4))))

    val first = state.startNext()!!
    assertEquals(1, first.size)
    assertEquals(listOf<Byte>(1, 2, 3), first.single().second.payload.toList())
    assertEquals(4L, state.queuedBytes())
    state.acknowledge()

    val second = state.startNext()!!
    assertEquals(listOf<Byte>(4), second.single().second.payload.toList())
    state.acknowledge()
    assertEquals(0L, state.queuedBytes())
  }

  @Test
  fun dropNewestPreservesExistingOrderAndExposesBoundedMetrics() {
    val state = DeliveryState(
      options.copy(
        maxQueuedMessages = 2,
        maxQueuedBytes = 8,
        maxMessageBytes = 4,
        overflowPolicy = "drop-newest",
      ),
    )
    assertTrue(state.enqueue(9, message(byteArrayOf(1))))
    assertTrue(state.enqueue(9, message(byteArrayOf(2))))
    assertFalse(state.enqueue(9, message(byteArrayOf(3))))

    assertEquals(1L, state.recordDrop())
    assertTrue(state.takeOverflowDiagnostic())
    assertEquals(2, state.queuedMessages())
    assertEquals(2L, state.queuedBytes())

    val batch = state.startNext()!!
    assertEquals(listOf<Byte>(1, 2), batch.flatMap { it.second.payload.toList() })
    state.acknowledge()
    assertEquals(0, state.queuedMessages())
    assertEquals(0L, state.queuedBytes())
  }
}
