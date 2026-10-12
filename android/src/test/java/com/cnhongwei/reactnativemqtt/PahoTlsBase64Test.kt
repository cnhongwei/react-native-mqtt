package com.cnhongwei.reactnativemqtt

import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertThrows
import org.junit.Test
import java.util.Base64

class PahoTlsBase64Test {
  @Test
  fun acceptsCanonicalBase64WithDoublePadding() {
    val encoded = Base64.getEncoder().encodeToString(byteArrayOf(0x0a))

    assertArrayEquals(byteArrayOf(0x0a), decodeTlsBase64(encoded))
  }

  @Test
  fun rejectsNonZeroTrailingBitsWithDoublePadding() {
    assertThrows(IllegalArgumentException::class.java) {
      decodeTlsBase64("Ch==")
    }
  }
}
