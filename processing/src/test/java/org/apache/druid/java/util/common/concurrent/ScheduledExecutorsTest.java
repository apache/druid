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

import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
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
    ScheduledExecutorService exec = Execs.scheduledSingleThreaded("BasicAuthenticatorCacheManager-Exec--%d");

    List<Long> taskStartTimes = new ArrayList<>();
    AtomicInteger executionCount = new AtomicInteger(0);
    CountDownLatch latch = new CountDownLatch(1);
    long startTime = System.currentTimeMillis();

    ScheduledExecutors.scheduleWithFixedDelay(
        exec,
        initialDelay,
        delay,
        () -> {
          try {
            long taskStart = System.currentTimeMillis();
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

    Assert.assertTrue("Should complete within timeout", completed);
    Assert.assertEquals("Should have exactly 4 executions", 4, executionCount.get());

    // Verify first task starts at approximately the initial delay, in real life this is greater than 100ms due to overhead.
    long firstTaskStart = taskStartTimes.get(0) - startTime;
    Assert.assertTrue(
        "First task should start at approximately initial delay (100ms), was: " + firstTaskStart,
        firstTaskStart > 100 && firstTaskStart < 500
    );

    // Verify subsequent tasks wait for delay from previous task end
    // Task ends at: taskStart + taskDuration
    for (int i = 1; i < taskStartTimes.size(); i++) {
      long previousTaskEnd = taskStartTimes.get(i - 1) + taskDuration;
      long delayBetweenTasks = taskStartTimes.get(i) - previousTaskEnd;
      // Should be approximately 500ms (the delay between task end and next task start)
      Assert.assertTrue(
          "Delay from task " + (i - 1) + " end to task " + i + " start should be ~500ms, was: " + delayBetweenTasks,
          delayBetweenTasks >= 450 && delayBetweenTasks <= 650
      );
    }
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

    Assert.assertTrue("Should complete within timeout", completed);
    Assert.assertEquals("Should have exactly 4 executions", 4, executionStartTimes.size());

    // Verify first task took longer than period (no pileup should occur)
    // Second task should start immediately after first task finishes (not during)
    long timeBetweenFirstAndSecond = executionStartTimes.get(1) - executionStartTimes.get(0);
    // Should be at least 800ms (first task duration), but not much more since it schedules immediately
    Assert.assertTrue(
        "Second task should start after first task completes (~800ms), was: " + timeBetweenFirstAndSecond,
        timeBetweenFirstAndSecond >= 750 && timeBetweenFirstAndSecond <= 900
    );

    // Verify subsequent tasks maintain the period
    long timeBetweenSecondAndThird = executionStartTimes.get(2) - executionStartTimes.get(1);
    // Should be approximately 500ms (the period), since tasks now finish quickly
    Assert.assertTrue(
        "Subsequent tasks should maintain period (~500ms), was: " + timeBetweenSecondAndThird,
        timeBetweenSecondAndThird >= 450 && timeBetweenSecondAndThird <= 600
    );
  }
}
