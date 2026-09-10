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

import com.google.common.annotations.VisibleForTesting;
import org.apache.calcite.util.CancelFlag;
import org.apache.druid.java.util.common.concurrent.Execs;

import java.io.Closeable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * A watchdog that bounds the wall-clock time spent planning a single SQL query. SQL planning happens synchronously on
 * the request thread; a pathological query (for example one with an enormous {@code IN} clause) can spend tens of
 * seconds in Calcite's planner, and a flood of such queries can exhaust the Broker's request threads and freeze the
 * process. See {@link org.apache.druid.sql.calcite.planner.PlannerConfig#getMaxPlanningTimeMs()}.
 *
 * <p>When {@link #arm} is called with a positive timeout, a background task is scheduled that, once the deadline is
 * reached, (1) trips the Calcite {@link CancelFlag} for the query so that the planner aborts at its next cancellation
 * checkpoint, and (2) interrupts the planning thread so that any interruptible work also unwinds. The caller runs
 * planning on its own thread as usual and then calls {@link #close()} (ideally in a {@code finally} block), which
 * cancels the pending task and, if the watchdog had already fired, clears the interrupt status of the (current)
 * planning thread so the interrupt is not leaked back to a pooled request thread.
 *
 * <p>Callers should check {@link #isTimedOut()} when planning throws to decide whether to translate the failure into a
 * timeout error.
 */
public class SqlPlanningTimeout implements Closeable
{
  /**
   * Shared, lazily-started scheduler used to fire planning-timeout tasks. A single daemon thread is sufficient because
   * each task does only a tiny amount of work (flip a flag and interrupt a thread) and, in the common case, is
   * cancelled well before it ever runs.
   */
  private static final ScheduledExecutorService SCHEDULER =
      Execs.scheduledSingleThreaded("sql-planning-timeout-%d");

  /**
   * A shared no-op instance returned when no planning timeout is configured, so callers need no null checks.
   */
  private static final SqlPlanningTimeout DISABLED = new SqlPlanningTimeout();

  private final Object lock = new Object();
  private final ScheduledFuture<?> future;

  /**
   * Whether the watchdog fired (i.e. the planning deadline was reached). Guarded by {@link #lock} for writes; read via
   * {@link #isTimedOut()}.
   */
  private volatile boolean timedOut;

  /**
   * Whether {@link #close()} has been called. Once closed, a concurrently-running watchdog task must not interrupt the
   * planning thread. Guarded by {@link #lock}.
   */
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
   * Arm a planning-timeout watchdog for the current thread. When {@code maxPlanningTimeMs} is not positive the returned
   * instance is a no-op.
   *
   * @param maxPlanningTimeMs the planning budget in milliseconds; values &lt;= 0 disable the timeout
   * @param cancelFlag        the Calcite cancellation flag for the query, tripped on timeout
   * @param planningThread    the thread performing planning, which will be interrupted on timeout
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
      // Ask Calcite's planner to abort at its next cancellation checkpoint.
      cancelFlag.requestCancel();
      // Also interrupt the planning thread so any interruptible work unwinds.
      planningThread.interrupt();
    }
  }

  /**
   * Whether the planning deadline was reached before {@link #close()} was called.
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
      // The watchdog interrupted this (the planning) thread. Clear the interrupt status so it is not leaked to a
      // pooled request thread. Safe because close() is called on the planning thread once planning has finished.
      Thread.interrupted();
    }
  }

  @VisibleForTesting
  static ScheduledExecutorService sharedScheduler()
  {
    return SCHEDULER;
  }
}
