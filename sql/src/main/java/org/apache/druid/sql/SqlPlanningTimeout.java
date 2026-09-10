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
 * Bounds the wall-clock time spent planning a single SQL query. See
 * {@link org.apache.druid.sql.calcite.planner.PlannerConfig#getMaxPlanningTimeMs()}.
 *
 * <p>When {@link #arm} is called with a positive timeout, a task is scheduled that, on deadline, trips the query's
 * Calcite {@link CancelFlag} (so the planner aborts at its next cancellation checkpoint) and interrupts the planning
 * thread. The caller plans on its own thread and then calls {@link #close()} (ideally in a {@code finally}), which
 * cancels the pending task and, if the watchdog fired, clears the interrupt so it is not leaked to a pooled request
 * thread. Check {@link #isTimedOut()} when planning throws to decide whether to translate the failure into a timeout.
 */
public class SqlPlanningTimeout implements Closeable
{
  // Single daemon thread suffices: each task only flips a flag and interrupts a thread, and is usually cancelled first.
  private static final ScheduledThreadPoolExecutor SCHEDULER = createScheduler();

  private static ScheduledThreadPoolExecutor createScheduler()
  {
    final ScheduledThreadPoolExecutor scheduler =
        new ScheduledThreadPoolExecutor(1, Execs.makeThreadFactory("sql-planning-timeout-%d"));
    // Planning usually finishes before the deadline, so most tasks are cancelled. Remove them from the queue on
    // cancellation instead of letting them linger until their delay elapses, so the queue does not grow under load.
    scheduler.setRemoveOnCancelPolicy(true);
    return scheduler;
  }

  // No-op instance returned when no timeout is configured, so callers need no null checks.
  private static final SqlPlanningTimeout DISABLED = new SqlPlanningTimeout();

  private final Object lock = new Object();
  private final ScheduledFuture<?> future;

  // Whether the deadline was reached. Written under lock; read via isTimedOut().
  private volatile boolean timedOut;

  // Whether close() has been called. Once closed, a still-running watchdog task must not interrupt the thread.
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
        // Planning already finished; do not interrupt a thread that may have been recycled.
        return;
      }
      timedOut = true;
      cancelFlag.requestCancel();
      planningThread.interrupt();
    }
  }

  /**
   * Whether the planning deadline was reached before {@link #close()}.
   */
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
      // Clear the interrupt the watchdog set on this thread so it is not leaked to a pooled request thread.
      // Safe because close() runs on the planning thread once planning has finished.
      Thread.interrupted();
    }
  }
}
