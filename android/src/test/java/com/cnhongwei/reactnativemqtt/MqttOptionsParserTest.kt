package com.cnhongwei.reactnativemqtt

import com.facebook.react.bridge.JavaOnlyMap
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class MqttOptionsParserTest {
  @Test
  fun preservesZeroFalseUint32AndWillFields() {
    val options = JavaOnlyMap().apply {
      putString("host", "broker.example")
      putInt("port", 1883)
      putInt("protocolVersion", 5)
      putInt("keepAliveSeconds", 0)
      putBoolean("cleanStart", false)
      putDouble("sessionExpiryIntervalSeconds", 4294967295.0)
      putInt("receiveMaximum", 17)
      putString("willTopic", "last/will")
      putString("willPayloadBase64", "")
      putInt("willQos", 2)
      putBoolean("willRetained", true)
      putArray("userProperties", com.facebook.react.bridge.JavaOnlyArray.of(
        JavaOnlyMap.of("key", "trace", "value", "one"),
        JavaOnlyMap.of("key", "trace", "value", "two"),
      ))
    }

    val parsed = MqttOptionsParser.parse(options).getOrThrow()
    assertEquals(0, parsed.keepAliveSeconds)
    assertEquals(false, parsed.cleanStart)
    assertEquals(4294967295L, parsed.sessionExpiryIntervalSeconds)
    assertEquals(17, parsed.receiveMaximum)
    assertEquals("last/will", parsed.willTopic)
    assertEquals("", parsed.willPayloadBase64)
    assertEquals(2, parsed.willQos)
    assertEquals(true, parsed.willRetained)
    assertEquals(listOf("trace" to "one", "trace" to "two"), parsed.userProperties)
  }

  @Test
  fun rejectsFractionalAndUnpairedWillFieldsBeforeTransport() {
    val options = JavaOnlyMap().apply {
      putString("host", "broker.example")
      putInt("port", 1883)
      putDouble("keepAliveSeconds", 1.5)
    }
    assertTrue(MqttOptionsParser.parse(options).isFailure)

    val incompleteWill = JavaOnlyMap().apply {
      putString("host", "broker.example")
      putInt("port", 1883)
      putString("willTopic", "last/will")
    }
    assertTrue(MqttOptionsParser.parse(incompleteWill).isFailure)
  }
}
