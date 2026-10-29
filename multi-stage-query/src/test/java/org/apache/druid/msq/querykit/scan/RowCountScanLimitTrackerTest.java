/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.msq.querykit.scan;

import org.apache.druid.java.util.common.Intervals;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class RowCountScanLimitTrackerTest
{
  @Test
  public void test_gate_bufferedRowsReachLimit_stops()
  {
    final RowCountScanLimitTracker tracker = new RowCountScanLimitTracker(3);
    final RowGate gate = newGate(tracker);

    writeRows(gate, 3);
    Assertions.assertEquals(RowGate.Decision.STOP, gate.check());
  }

  @Test
  public void test_gate_afterFlush_doesNotDoubleCountRows()
  {
    final RowCountScanLimitTracker tracker = new RowCountScanLimitTracker(3);
    final RowGate gate = newGate(tracker);

    writeRows(gate, 2);
    gate.onFrameFlushed();
    Assertions.assertEquals(RowGate.Decision.WRITE, gate.check());

    writeRows(gate, 1);
    Assertions.assertEquals(RowGate.Decision.STOP, gate.check());
  }

  @Test
  public void test_gate_otherGateFlushesRows_stops()
  {
    final RowCountScanLimitTracker tracker = new RowCountScanLimitTracker(3);
    final RowGate gateA = newGate(tracker);
    final RowGate gateB = newGate(tracker);

    writeRows(gateA, 2);
    writeRows(gateB, 1);
    Assertions.assertEquals(RowGate.Decision.WRITE, gateB.check(), "other gate's unflushed rows do not count");

    gateA.onFrameFlushed();
    Assertions.assertEquals(RowGate.Decision.STOP, gateB.check());
  }

  @Test
  public void test_isSatisfied_limitFlushed_skipsAllInputs()
  {
    final RowCountScanLimitTracker tracker = new RowCountScanLimitTracker(3);
    final RowGate gate = newGate(tracker);

    writeRows(gate, 3);
    Assertions.assertFalse(tracker.isSatisfied(), "unflushed rows are not output yet");
    Assertions.assertFalse(tracker.canSkip(Intervals.ETERNITY));

    gate.onFrameFlushed();
    Assertions.assertTrue(tracker.isSatisfied());
    Assertions.assertTrue(tracker.canSkip(Intervals.ETERNITY));
    Assertions.assertEquals(RowGate.Decision.STOP, newGate(tracker).check());
  }

  /**
   * The row-count gate ignores the row's values and the cursor's ordering, so neither is provided.
   */
  private static RowGate newGate(final RowCountScanLimitTracker tracker)
  {
    return tracker.forCursor(null, List.of());
  }

  /**
   * Writes {@code numRows} rows through {@code gate}, checking each one first as the processor does.
   */
  private static void writeRows(final RowGate gate, final int numRows)
  {
    for (int i = 0; i < numRows; i++) {
      Assertions.assertEquals(RowGate.Decision.WRITE, gate.check());
      gate.onRowWritten();
    }
  }
}
