package com.cnhongwei.reactnativemqtt

import org.junit.Assert.assertEquals
import org.junit.Assert.assertThrows
import org.junit.Test

class MqttSessionRegistryTest {
  private val options = ParsedMqttOptions(
    host = "broker.example",
    port = 1883,
    protocolVersion = 5,
    transport = "tcp",
    clientId = "registry-test",
    username = null,
    password = null,
    keepAliveSeconds = 60,
    cleanSession = true,
    cleanStart = true,
    sessionExpiryIntervalSeconds = 0,
  )

  @Test
  fun repeatedOpenIsIdempotentAndOptionChangesAreRejected() {
    val registry = MqttSessionRegistry()
    val first = registry.open("instance", options).join()
    val second = registry.open("instance", options).join()
    assertEquals(first, second)
    val error = assertThrows(java.util.concurrent.CompletionException::class.java) {
      registry.open("instance", options.copy(port = 1884)).join()
    }
    assertEquals("ERR_OPERATION_IN_PROGRESS", (error.cause as MqttSessionException).errorCode)
    registry.close("instance").join()
  }

  @Test
  fun closeIsTerminalAndInvalidateRejectsLaterWork() {
    val registry = MqttSessionRegistry()
    registry.open("instance", options).join()
    assertEquals("closed", registry.close("instance").join()["state"])
    registry.invalidate()
    val error = assertThrows(java.util.concurrent.CompletionException::class.java) {
      registry.snapshot("instance").join()
    }
    assertEquals("ERR_DISPOSED", (error.cause as MqttSessionException).errorCode)
  }

  @Test
  fun failedConnectEmitsTheTerminalSnapshotBeforeRejecting() {
    val transport = NativeScenarioTransport()
    val snapshots = mutableListOf<Map<String, Any?>>()
    val registry = MqttSessionRegistry(
      transportFactory = { _, _ -> transport },
      onSessionSnapshot = { snapshots += it },
    )
    registry.open("instance", options).join()
    val error = assertThrows(java.util.concurrent.CompletionException::class.java) {
      val connect = registry.connect("instance")
      val deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(1)
      while (transport.connectPending.isEmpty() && System.nanoTime() < deadline) {
        Thread.yield()
      }
      check(transport.connectPending.isNotEmpty()) { "connect was not dispatched" }
      transport.failConnect(error = IllegalStateException("TLS failed"))
      connect.join()
    }

    assertEquals("TLS failed", error.cause?.cause?.message ?: error.cause?.message)
    assertEquals("disconnected", snapshots.last()["state"])
    val close = registry.close("instance")
    val closeDeadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(1)
    while (transport.disconnectPending.isEmpty() && System.nanoTime() < closeDeadline) {
      Thread.yield()
    }
    check(transport.disconnectPending.isNotEmpty()) { "close was not dispatched" }
    transport.completeDisconnect()
    close.join()
  }
}
