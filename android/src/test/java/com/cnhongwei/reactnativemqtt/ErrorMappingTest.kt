package com.cnhongwei.reactnativemqtt

import org.junit.Assert.assertSame
import org.junit.Assert.assertNull
import org.junit.Test

class ErrorMappingTest {
  @Test
  fun findsSessionExceptionAtAnySupportedWrapperDepth() {
    val expected = MqttSessionException("ERR_PUBLISH_FAILED", "publish failed")
    val wrapped = IllegalStateException("outer", IllegalArgumentException("middle", expected))

    assertSame(expected, findMqttSessionException(expected))
    assertSame(expected, findMqttSessionException(wrapped))
  }

  @Test
  fun returnsNullWhenWrapperChainHasNoSessionException() {
    assertNull(findMqttSessionException(IllegalStateException("unrelated")))
  }
}
