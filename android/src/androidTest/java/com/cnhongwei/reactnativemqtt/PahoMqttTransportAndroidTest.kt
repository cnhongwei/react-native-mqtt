package com.cnhongwei.reactnativemqtt

import androidx.test.ext.junit.runners.AndroidJUnit4
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

@RunWith(AndroidJUnit4::class)
class PahoMqttTransportAndroidTest {
  @Test
  fun mqtt5WebSocketRoundTripRunsThroughSessionRegistry() {
    val registry = MqttSessionRegistry()
    val instanceId = "android-ws-registry-${System.currentTimeMillis()}"
    val options = ParsedMqttOptions(
      host = "10.0.2.2",
      port = 8080,
      protocolVersion = 5,
      transport = "ws",
      path = "/mqtt",
      clientId = "rn-android-registry-ws-${System.currentTimeMillis()}",
      keepAliveSeconds = 30,
      cleanStart = true,
      sessionExpiryIntervalSeconds = 0,
    )

    try {
      registry.open(instanceId, options).get(5, TimeUnit.SECONDS)
      assertEquals(0, registry.connect(instanceId).get(15, TimeUnit.SECONDS)["ack"].let { ack ->
        @Suppress("UNCHECKED_CAST")
        (ack as Map<String, Any?>)["reasonCode"] as Int
      })
    } finally {
      registry.close(instanceId).get(10, TimeUnit.SECONDS)
      registry.invalidate()
    }
  }

  @Test
  fun mqtt5WebSocketRoundTripRunsOnAndroidRuntime() {
    val received = CountDownLatch(1)
    val transport = PahoMqttTransport(
      ParsedMqttOptions(
        host = "10.0.2.2",
        port = 8080,
        protocolVersion = 5,
        transport = "ws",
        path = "/mqtt",
        clientId = "rn-android-ws-${System.currentTimeMillis()}",
        keepAliveSeconds = 30,
        cleanStart = true,
        sessionExpiryIntervalSeconds = 0,
      ),
    ) { message ->
      assertEquals("rn/android-ws", message.topic)
      assertEquals(listOf<Byte>(13, 21, 34), message.payload.toList())
      received.countDown()
    }

    try {
      assertEquals(0, transport.connect().get(15, TimeUnit.SECONDS).reasonCode)
      assertEquals(1, transport.subscribe("rn/android-ws", 1, false, false, 0).get(10, TimeUnit.SECONDS).grantedQos)
      transport.publish("rn/android-ws", byteArrayOf(13, 21, 34), 1, false).get(10, TimeUnit.SECONDS)
      assertTrue(received.await(10, TimeUnit.SECONDS))
    } finally {
      transport.disconnect().get(10, TimeUnit.SECONDS)
    }
  }

  @Test
  fun mqtt5TcpRoundTripRunsOnAndroidRuntime() {
    val received = CountDownLatch(1)
    val transport = PahoMqttTransport(
      ParsedMqttOptions(
        host = "10.0.2.2",
        port = 1883,
        protocolVersion = 5,
        transport = "tcp",
        clientId = "rn-android-runtime-${System.currentTimeMillis()}",
        keepAliveSeconds = 30,
        cleanStart = true,
        sessionExpiryIntervalSeconds = 0,
      ),
    ) { message ->
      assertEquals("rn/android-runtime", message.topic)
      assertEquals(listOf<Byte>(0, 1, 2, 127, -1), message.payload.toList())
      received.countDown()
    }

    try {
      assertEquals(0, transport.connect().get(15, TimeUnit.SECONDS).reasonCode)
      assertEquals(1, transport.subscribe("rn/android-runtime", 1, false, false, 0).get(10, TimeUnit.SECONDS).grantedQos)
      transport.publish("rn/android-runtime", byteArrayOf(0, 1, 2, 127, -1), 1, false).get(10, TimeUnit.SECONDS)
      assertTrue(received.await(10, TimeUnit.SECONDS))
    } finally {
      transport.disconnect().get(10, TimeUnit.SECONDS)
    }
  }

  @Test
  fun mqtt311TcpRoundTripRunsOnAndroidRuntime() {
    val received = CountDownLatch(1)
    val transport = PahoMqttTransport(
      ParsedMqttOptions(
        host = "10.0.2.2",
        port = 1883,
        protocolVersion = 4,
        transport = "tcp",
        clientId = "rn-android-runtime-v4-${System.currentTimeMillis()}",
        keepAliveSeconds = 30,
        cleanSession = true,
      ),
    ) { message ->
      assertEquals("rn/android-runtime-v4", message.topic)
      assertEquals(listOf<Byte>(3, 5, 8), message.payload.toList())
      received.countDown()
    }

    try {
      assertEquals(0, transport.connect().get(15, TimeUnit.SECONDS).reasonCode)
      assertEquals(1, transport.subscribe("rn/android-runtime-v4", 1, false, false, 0).get(10, TimeUnit.SECONDS).grantedQos)
      transport.publish("rn/android-runtime-v4", byteArrayOf(3, 5, 8), 1, false).get(10, TimeUnit.SECONDS)
      assertTrue(received.await(10, TimeUnit.SECONDS))
    } finally {
      transport.disconnect().get(10, TimeUnit.SECONDS)
    }
  }

}
