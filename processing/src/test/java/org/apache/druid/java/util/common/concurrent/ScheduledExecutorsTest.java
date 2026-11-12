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

package org.apache.druid.java.util.common.concurrent;

import org.apache.druid.testing.junit.LoggerCaptureExtension;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.joda.time.Duration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ScheduledExecutorsTest
{
  @Test
  public void testscheduleWithFixedDelay() throws Exception
  {
    Duration initialDelay = Duration.millis(100);
    Duration delay = Duration.millis(500);
    int taskDuration = 100; // ms
    ScheduledExecutorService exec = Execs.scheduledSingleThreaded("testFixedDelay--%d");

    List<Long> taskStartTimes = new ArrayList<>();
    AtomicInteger executionCount = new AtomicInteger(0);
    CountDownLatch latch = new CountDownLatch(1);
    final long startTime = System.nanoTime();

    ScheduledExecutors.scheduleWithFixedDelay(
        exec,
        initialDelay,
        delay,
        () -> {
          try {
            final long taskStart = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
            int count = executionCount.getAndIncrement();
            synchronized (taskStartTimes) {
              taskStartTimes.add(taskStart);
            }

            // Each task takes 100ms
            Thread.sleep(taskDuration);

            if (count == 3) {
              latch.countDown();
            }
          }
          catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          }
        }
    );

    boolean completed = latch.await(5, TimeUnit.SECONDS);
    exec.shutdown();

    Assertions.assertTrue(completed, "Should complete within timeout");
    Assertions.assertEquals(4, executionCount.get(), "Should have exactly 4 executions");

    // Verify first task starts at approximately the initial delay.
    // It can fire exactly on time, so the 100ms lower bound is inclusive.
    final long firstTaskStart = taskStartTimes.get(0);
    Assertions.assertTrue(
        firstTaskStart >= 100 && firstTaskStart < 500,
        "First task should start at approximately initial delay (100ms), was: " + firstTaskStart
    );

    // Verify subsequent tasks wait for delay from previous task end
    // Task ends at: taskStart + taskDuration
    for (int i = 1; i < taskStartTimes.size(); i++) {
      long previousTaskEnd = taskStartTimes.get(i - 1) + taskDuration;
      long delayBetweenTasks = taskStartTimes.get(i) - previousTaskEnd;
      // Should be approximately 500ms (the delay between task end and next task start)
      Assertions.assertTrue(
          delayBetweenTasks >= 450 && delayBetweenTasks <= 650,
          "Delay from task " + (i - 1) + " end to task " + i + " start should be ~500ms, was: " + delayBetweenTasks
      );
    }
  }

  @Test
  public void testScheduleWithFixedDelayContinuesAfterException() throws Exception
  {
    final Duration initialDelay = Duration.millis(0);
    final Duration delay = Duration.millis(100);
    final ScheduledExecutorService exec = Execs.scheduledSingleThreaded("testFixedDelayException-%d");

    final AtomicInteger executionCount = new AtomicInteger(0);
    final CountDownLatch latch = new CountDownLatch(1);

    ScheduledExecutors.scheduleWithFixedDelay(
        exec,
        initialDelay,
        delay,
        () -> {
          final int count = executionCount.incrementAndGet();
          if (count == 1) {
            throw new RuntimeException("test exception");
          }
          if (count >= 3) {
            latch.countDown();
            return ScheduledExecutors.Signal.STOP;
          }
          return ScheduledExecutors.Signal.REPEAT;
        }
    );

    final boolean completed = latch.await(5, TimeUnit.SECONDS);
    exec.shutdown();

    Assertions.assertTrue(completed, "Should continue executing after exception");
    Assertions.assertEquals(3, executionCount.get(), "Should have exactly 3 executions");
  }

  @Test
  public void testScheduleWithFixedDelayStopsOnSignalStop() throws Exception
  {
    final Duration initialDelay = Duration.millis(0);
    final Duration delay = Duration.millis(100);
    final ScheduledExecutorService exec = Execs.scheduledSingleThreaded("testFixedDelayStop-%d");

    final AtomicInteger executionCount = new AtomicInteger(0);

    ScheduledExecutors.scheduleWithFixedDelay(
        exec,
        initialDelay,
        delay,
        () -> {
          executionCount.incrementAndGet();
          return ScheduledExecutors.Signal.STOP;
        }
    );

    Thread.sleep(500);
    exec.shutdown();

    Assertions.assertEquals(1, executionCount.get(), "Should execute exactly once before stopping");
  }

  @Test
  public void testScheduleAtFixedRateWithLongRunningTask() throws Exception
  {
    Duration initialDelay = Duration.millis(100);
    Duration period = Duration.millis(500);
    ScheduledExecutorService exec = Execs.scheduledSingleThreaded("testScheduleAtFixedRate-%d");

    List<Long> executionStartTimes = new ArrayList<>();
    AtomicInteger executionCount = new AtomicInteger(0);
    CountDownLatch latch = new CountDownLatch(1);

    ScheduledExecutors.scheduleAtFixedRate(
        exec,
        initialDelay,
        period,
        () -> {
          try {
            int count = executionCount.getAndIncrement();
            long startTime = System.currentTimeMillis();
            synchronized (executionStartTimes) {
              executionStartTimes.add(startTime);
            }

            if (count == 0) {
              // First task takes longer than the period (800ms > 500ms period)
              Thread.sleep(800);
            } else {
              // Subsequent tasks finish quickly
              Thread.sleep(100);
            }

            // Stop after 4 executions
            if (count == 3) {
              latch.countDown();
            }
          }
          catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          }
        }
    );

    // Wait for completion
    boolean completed = latch.await(10, TimeUnit.SECONDS);
    exec.shutdown();

    Assertions.assertTrue(completed, "Should complete within timeout");
    Assertions.assertEquals(4, executionStartTimes.size(), "Should have exactly 4 executions");

    // Verify first task took longer than period (no pileup should occur)
    // Second task should start immediately after first task finishes (not during)
    long timeBetweenFirstAndSecond = executionStartTimes.get(1) - executionStartTimes.get(0);
    // Should be at least 800ms (first task duration), but not much more since it schedules immediately
    Assertions.assertTrue(
        timeBetweenFirstAndSecond >= 750 && timeBetweenFirstAndSecond <= 950,
        "Second task should start after first task completes (~800ms), was: " + timeBetweenFirstAndSecond
    );

    // Verify subsequent tasks maintain the period (no catch-up burst)
    long timeBetweenSecondAndThird = executionStartTimes.get(2) - executionStartTimes.get(1);
    // Should be approximately 500ms (the period), since tasks now finish quickly
    Assertions.assertTrue(
        timeBetweenSecondAndThird >= 450 && timeBetweenSecondAndThird <= 600,
        "Subsequent tasks should maintain period (~500ms), was: " + timeBetweenSecondAndThird
    );
  }

  @Test
  public void testScheduleAtFixedRateNoConcurrentExecutionWithMultipleThreads() throws Exception
  {
    final Duration initialDelay = Duration.millis(0);
    final Duration period = Duration.millis(100);
    // Use 4 threads to verify that even with available threads, only one execution runs at a time
    final ScheduledExecutorService exec = ScheduledExecutors.fixed(4, "testNoConcurrency-%d");

    final AtomicInteger concurrentCount = new AtomicInteger(0);
    final AtomicInteger maxConcurrentCount = new AtomicInteger(0);
    final AtomicInteger executionCount = new AtomicInteger(0);
    final CountDownLatch latch = new CountDownLatch(1);

    ScheduledExecutors.scheduleAtFixedRate(
        exec,
        initialDelay,
        period,
        () -> {
          try {
            final int concurrent = concurrentCount.incrementAndGet();

            // Track the maximum number of concurrent executions
            int currentMax;
            do {
              currentMax = maxConcurrentCount.get();
            } while (concurrent > currentMax && !maxConcurrentCount.compareAndSet(currentMax, concurrent));

            // Task takes 3x the period — would cause overlap if scheduling were broken
            Thread.sleep(period.getMillis() * 3);

            concurrentCount.decrementAndGet();

            if (executionCount.incrementAndGet() >= 4) {
              latch.countDown();
            }
          }
          catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          }
        }
    );

    final boolean completed = latch.await(10, TimeUnit.SECONDS);
    exec.shutdown();

    Assertions.assertTrue(completed, "Should complete within timeout");
    Assertions.assertEquals(1, maxConcurrentCount.get(), "Should never have more than 1 concurrent execution");
  }

  @Test
  public void testScheduleAtFixedRateStopsOnSignalStop() throws Exception
  {
    final Duration initialDelay = Duration.millis(0);
    final Duration period = Duration.millis(100);
    final ScheduledExecutorService exec = ScheduledExecutors.fixed(4, "testStop-%d");

    final AtomicInteger executionCount = new AtomicInteger(0);

    ScheduledExecutors.scheduleAtFixedRate(
        exec,
        initialDelay,
        period,
        () -> {
          executionCount.incrementAndGet();
          return ScheduledExecutors.Signal.STOP;
        }
    );

    // Wait enough time for multiple executions to have occurred if stopping were broken
    Thread.sleep(500);
    exec.shutdown();

    Assertions.assertEquals(1, executionCount.get(), "Should execute exactly once before stopping");
  }

  @Test
  public void testScheduleAtFixedRateContinuesAfterException() throws Exception
  {
    final Duration initialDelay = Duration.millis(0);
    final Duration period = Duration.millis(100);
    final ScheduledExecutorService exec = Execs.scheduledSingleThreaded("testException-%d");

    final AtomicInteger executionCount = new AtomicInteger(0);
    final CountDownLatch latch = new CountDownLatch(1);

    ScheduledExecutors.scheduleAtFixedRate(
        exec,
        initialDelay,
        period,
        () -> {
          final int count = executionCount.incrementAndGet();
          if (count == 1) {
            throw new RuntimeException("test exception");
          }
          if (count >= 3) {
            latch.countDown();
            return ScheduledExecutors.Signal.STOP;
          }
          return ScheduledExecutors.Signal.REPEAT;
        }
    );

    final boolean completed = latch.await(5, TimeUnit.SECONDS);
    exec.shutdown();

    Assertions.assertTrue(completed, "Should continue executing after exception");
    Assertions.assertEquals(3, executionCount.get(), "Should have exactly 3 executions");
  }

  @Test
  public void testFixedWithKeepAliveTimeReclaimsIdleThreads() throws Exception
  {
    final ScheduledThreadPoolExecutor exec = ScheduledExecutors.fixedWithKeepAliveTime(8, "testKeepAlive-%d", 100);
    try {
      // Core threads must be eligible for timeout, otherwise a large corePoolSize would pin that many
      // threads for the lifetime of the pool even when idle.
      Assertions.assertTrue(exec.allowsCoreThreadTimeOut(), "Core threads should be allowed to time out");

      final CountDownLatch latch = new CountDownLatch(4);
      for (int i = 0; i < 4; i++) {
        exec.submit(latch::countDown);
      }
      Assertions.assertTrue(latch.await(5, TimeUnit.SECONDS), "Submitted tasks should run");

      // Once idle past the keep-alive, all workers should be reclaimed and the pool shrink back to zero.
      final long deadline = System.currentTimeMillis() + 5000;
      while (exec.getPoolSize() > 0 && System.currentTimeMillis() < deadline) {
        Thread.sleep(50);
      }
      Assertions.assertEquals(0, exec.getPoolSize(), "Idle core threads should be reclaimed");
    }
    finally {
      exec.shutdownNow();
    }
  }

  @RegisterExtension
  public final LoggerCaptureExtension logCaptureExtension = new LoggerCaptureExtension(ScheduledExecutors.class);

  @Test
  public void testScheduleAtFixedRateLogsQuietlyWhenExecutorAlreadyShutdown() throws Exception
  {
    final Duration initialDelay = Duration.millis(0);
    final Duration period = Duration.millis(1000);
    final ScheduledExecutorService delegate = Execs.scheduledSingleThreaded("testQuietShutdown-%d");
    final ScheduledExecutorService alreadyShutdownExec = new ShutdownReportingExecutor(delegate);

    try {
      ScheduledExecutors.scheduleAtFixedRate(
          alreadyShutdownExec,
          initialDelay,
          period,
          () -> {
            throw new RuntimeException("simulated shutdown-ordering failure");
          }
      );

      logCaptureExtension.awaitLogEvents();
    }
    finally {
      delegate.shutdownNow();
    }

    final List<LogEvent> events = logCaptureExtension.getLogEvents();
    Assertions.assertFalse(
        events.stream().anyMatch(e -> e.getLevel().equals(Level.WARN)),
        "no WARN-level stack trace should be logged when the executor is already shut down"
    );
  }

  /**
   * Minimal {@link ScheduledExecutorService} that delegates scheduling to a real executor
   * but always reports {@link #isShutdown()} as {@code true}, for testing shutdown-path
   * behavior without needing to win a real shutdown race.
   */
  private static class ShutdownReportingExecutor extends java.util.concurrent.AbstractExecutorService
      implements ScheduledExecutorService
  {
    private final ScheduledExecutorService delegate;

    ShutdownReportingExecutor(ScheduledExecutorService delegate)
    {
      this.delegate = delegate;
    }

    @Override
    public boolean isShutdown()
    {
      return true;
    }

    @Override
    public java.util.concurrent.ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit)
    {
      return delegate.schedule(command, delay, unit);
    }

    @Override
    public <V> java.util.concurrent.ScheduledFuture<V> schedule(java.util.concurrent.Callable<V> callable, long delay, TimeUnit unit)
    {
      throw new UnsupportedOperationException("Not used by this test.");
    }

    @Override
    public java.util.concurrent.ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit)
    {
      throw new UnsupportedOperationException("Not used by this test.");
    }

    @Override
    public java.util.concurrent.ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit)
    {
      throw new UnsupportedOperationException("Not used by this test.");
    }

    @Override
    public void shutdown()
    {
      delegate.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow()
    {
      return delegate.shutdownNow();
    }

    @Override
    public boolean isTerminated()
    {
      return delegate.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
    {
      return delegate.awaitTermination(timeout, unit);
    }

    @Override
    public void execute(Runnable command)
    {
      delegate.execute(command);
    }
  }
}

