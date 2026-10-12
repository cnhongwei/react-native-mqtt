package com.cnhongwei.reactnativemqtt

import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class StrictBase64Test {
  @Test
  fun acceptsCanonicalBase64Shape() {
    assertTrue(isStrictBase64Shape("AA=="))
    assertTrue(isStrictBase64Shape("YWJj"))
  }

  @Test
  fun rejectsWhitespaceNonCanonicalBitsAndBadPadding() {
    assertFalse(isStrictBase64Shape("YQ==\n"))
    assertFalse(isStrictBase64Shape("YR=="))
    assertFalse(isStrictBase64Shape("YQ="))
  }
}
