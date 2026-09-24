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
import org.apache.druid.java.util.common.concurrent.Execs;

import java.io.Closeable;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Bounds the wall-clock time spent planning a single SQL query (see
 * {@link org.apache.druid.sql.calcite.planner.PlannerConfig#getMaxPlanningTimeMs()}). On deadline it trips the query's
 * Calcite {@link CancelFlag} and interrupts the planning thread; {@link #close()} cancels the watchdog and clears any
 * leaked interrupt. Check {@link #isTimedOut()} to translate a planning failure into a timeout.
 */
public class SqlPlanningTimeout implements Closeable
{
  private static final ScheduledThreadPoolExecutor SCHEDULER = createScheduler();

  private static ScheduledThreadPoolExecutor createScheduler()
  {
    // One daemon thread suffices; remove cancelled tasks (the common case) from the queue so it can't grow under load.
    final ScheduledThreadPoolExecutor scheduler =
        new ScheduledThreadPoolExecutor(1, Execs.makeThreadFactory("sql-planning-timeout-%d"));
    scheduler.setRemoveOnCancelPolicy(true);
    return scheduler;
  }

  // No-op instance returned when no timeout is configured, so callers need no null checks.
  private static final SqlPlanningTimeout DISABLED = new SqlPlanningTimeout();

  private final Object lock = new Object();
  private final ScheduledFuture<?> future;
  private volatile boolean timedOut;
  private boolean closed;

  private SqlPlanningTimeout()
  {
    this.future = null;
  }

  private SqlPlanningTimeout(long maxPlanningTimeMs, CancelFlag cancelFlag, Thread planningThread)
  {
    this.future = SCHEDULER.schedule(
        () -> fire(cancelFlag, planningThread),
        maxPlanningTimeMs,
        TimeUnit.MILLISECONDS
    );
  }

  /**
   * Arm a watchdog for {@code planningThread}. A non-positive {@code maxPlanningTimeMs} returns a no-op instance.
   */
  public static SqlPlanningTimeout arm(long maxPlanningTimeMs, CancelFlag cancelFlag, Thread planningThread)
  {
    if (maxPlanningTimeMs <= 0) {
      return DISABLED;
    }
    return new SqlPlanningTimeout(maxPlanningTimeMs, cancelFlag, planningThread);
  }

  private void fire(CancelFlag cancelFlag, Thread planningThread)
  {
    synchronized (lock) {
      if (closed) {
        // Planning already finished; do not interrupt a possibly-recycled thread.
        return;
      }
      timedOut = true;
      cancelFlag.requestCancel();
      planningThread.interrupt();
    }
  }

  public boolean isTimedOut()
  {
    return timedOut;
  }

  @Override
  public void close()
  {
    if (future == null) {
      return;
    }
    boolean wasTimedOut;
    synchronized (lock) {
      closed = true;
      future.cancel(false);
      wasTimedOut = timedOut;
    }
    if (wasTimedOut) {
      // Clear the watchdog's interrupt so it isn't leaked to a pooled request thread (close() runs on that thread).
      Thread.interrupted();
    }
  }
}
