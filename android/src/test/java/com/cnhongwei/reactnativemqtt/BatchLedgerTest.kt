package com.cnhongwei.reactnativemqtt

import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class BatchLedgerTest {
  @Test
  fun acceptsOnlyOneOutstandingBatchAndAcknowledgesExactTokenOnce() {
    val ledger = BatchLedger()
    val first = BatchToken(3, 7)
    val stale = BatchToken(2, 7)
    val second = BatchToken(3, 8)

    assertTrue(ledger.reserve("instance", first))
    assertFalse(ledger.reserve("instance", second))
    assertFalse(ledger.acknowledge("instance", stale))
    assertTrue(ledger.acknowledge("instance", first))
    assertFalse(ledger.acknowledge("instance", first))
    assertTrue(ledger.reserve("instance", second))
  }
}
