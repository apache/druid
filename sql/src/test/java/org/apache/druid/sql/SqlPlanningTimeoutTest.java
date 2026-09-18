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

package org.apache.druid.sql;

import org.apache.calcite.util.CancelFlag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SqlPlanningTimeoutTest
{
  private static CancelFlag newCancelFlag()
  {
    return new CancelFlag(new AtomicBoolean(false));
  }

  @Test
  public void testDisabledWhenTimeoutNotPositive()
  {
    final CancelFlag cancelFlag = newCancelFlag();
    try (SqlPlanningTimeout timeout = SqlPlanningTimeout.arm(0, cancelFlag, Thread.currentThread())) {
      assertFalse(timeout.isTimedOut());
      assertFalse(cancelFlag.isCancelRequested());
    }
    assertFalse(cancelFlag.isCancelRequested());
    assertFalse(Thread.currentThread().isInterrupted());
  }

  @Test
  public void testDisabledWhenTimeoutNegative()
  {
    final CancelFlag cancelFlag = newCancelFlag();
    try (SqlPlanningTimeout timeout = SqlPlanningTimeout.arm(-100, cancelFlag, Thread.currentThread())) {
      assertFalse(timeout.isTimedOut());
    }
    assertFalse(cancelFlag.isCancelRequested());
  }

  /**
   * Simulate slow planning: the "planning" thread spins until either the Calcite cancel flag trips (mimicking
   * Calcite's {@code checkCancel()}) or the thread is interrupted. The watchdog should fire, trip the flag, interrupt
   * the thread, and report a timeout; {@link SqlPlanningTimeout#close()} should then clear the interrupt.
   */
  @Test
  @Timeout(30)
  public void testTimeoutTripsCancelFlagAndInterrupts()
  {
    final CancelFlag cancelFlag = newCancelFlag();
    boolean sawInterruptOrCancel;
    try (SqlPlanningTimeout timeout = SqlPlanningTimeout.arm(50, cancelFlag, Thread.currentThread())) {
      // Busy-wait to mimic CPU-bound Calcite planning that periodically checks the cancel flag.
      while (!cancelFlag.isCancelRequested() && !Thread.currentThread().isInterrupted()) {
        // spin
      }
      sawInterruptOrCancel = true;
      assertTrue(timeout.isTimedOut());
      assertTrue(cancelFlag.isCancelRequested());
    }
    assertTrue(sawInterruptOrCancel);
    // close() must clear the interrupt so it does not leak to a pooled request thread.
    assertFalse(Thread.currentThread().isInterrupted());
  }

  /**
   * When planning finishes before the deadline, the watchdog must not trip the cancel flag or leave the thread
   * interrupted.
   */
  @Test
  @Timeout(30)
  public void testNoTimeoutWhenPlanningFinishesEarly() throws InterruptedException
  {
    final CancelFlag cancelFlag = newCancelFlag();
    try (SqlPlanningTimeout timeout = SqlPlanningTimeout.arm(5000, cancelFlag, Thread.currentThread())) {
      // "Planning" completes quickly.
      Thread.sleep(10);
      assertFalse(timeout.isTimedOut());
      assertFalse(cancelFlag.isCancelRequested());
    }
    assertFalse(cancelFlag.isCancelRequested());
    assertFalse(Thread.currentThread().isInterrupted());
  }
}
