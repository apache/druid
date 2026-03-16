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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class DelayMetricEmittingScheduledExecutorServiceTest
{
  private static final String METRIC_NAME = "schedule/lag";

  @Test
  public void testScheduleRunnableEmitsLagMetric() throws Exception
  {
    final StubServiceEmitter emitter = new StubServiceEmitter();
    final ScheduledExecutorService exec = ScheduledExecutors.emittingDelayMetric(
        Execs.scheduledSingleThreaded("test-%d"),
        emitter,
        METRIC_NAME,
        ImmutableMap.of("component", "testComponent")
    );

    final CountDownLatch latch = new CountDownLatch(1);
    exec.schedule(latch::countDown, 10, TimeUnit.MILLISECONDS);

    Assert.assertTrue("Task should complete", latch.await(5, TimeUnit.SECONDS));
    exec.shutdown();

    final List<ServiceMetricEvent> events = emitter.getMetricEvents(METRIC_NAME);
    Assert.assertEquals("Should emit exactly one metric", 1, events.size());

    final ServiceMetricEvent event = events.get(0);
    Assert.assertTrue("Lag should be >= 0", event.getValue().longValue() >= 0);
    Assert.assertEquals("testComponent", event.getUserDims().get("component"));
  }

  @Test
  public void testScheduleCallableEmitsLagMetric() throws Exception
  {
    final StubServiceEmitter emitter = new StubServiceEmitter();
    final ScheduledExecutorService exec = ScheduledExecutors.emittingDelayMetric(
        Execs.scheduledSingleThreaded("test-%d"),
        emitter,
        METRIC_NAME,
        ImmutableMap.of("component", "testComponent")
    );

    final String result = exec.schedule(() -> "hello", 10, TimeUnit.MILLISECONDS).get(5, TimeUnit.SECONDS);
    exec.shutdown();

    Assert.assertEquals("hello", result);

    final List<ServiceMetricEvent> events = emitter.getMetricEvents(METRIC_NAME);
    Assert.assertEquals("Should emit exactly one metric", 1, events.size());
    Assert.assertTrue("Lag should be >= 0", events.get(0).getValue().longValue() >= 0);
  }

  @Test
  public void testLagIsSmallWhenExecutorIsNotOverloaded() throws Exception
  {
    final StubServiceEmitter emitter = new StubServiceEmitter();
    final ScheduledExecutorService exec = ScheduledExecutors.emittingDelayMetric(
        Execs.scheduledSingleThreaded("test-%d"),
        emitter,
        METRIC_NAME,
        ImmutableMap.of()
    );

    // Schedule with 200ms delay — lag should be small (not 200ms)
    final CountDownLatch latch = new CountDownLatch(1);
    exec.schedule(latch::countDown, 200, TimeUnit.MILLISECONDS);

    Assert.assertTrue("Task should complete", latch.await(5, TimeUnit.SECONDS));
    exec.shutdown();

    final List<ServiceMetricEvent> events = emitter.getMetricEvents(METRIC_NAME);
    Assert.assertEquals(1, events.size());
    Assert.assertTrue(
        "Lag should be less than 100ms, was: " + events.get(0).getValue().longValue(),
        events.get(0).getValue().longValue() < 100
    );
  }

  @Test
  public void testEmitsOnRepeatedScheduleFromScheduledExecutors() throws Exception
  {
    final StubServiceEmitter emitter = new StubServiceEmitter();
    final ScheduledExecutorService exec = ScheduledExecutors.emittingDelayMetric(
        Execs.scheduledSingleThreaded("test-%d"),
        emitter,
        METRIC_NAME,
        ImmutableMap.of("scheduler", "test")
    );

    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicInteger count = new AtomicInteger(0);

    // Use ScheduledExecutors.scheduleAtFixedRate which calls exec.schedule internally
    ScheduledExecutors.scheduleAtFixedRate(
        exec,
        Duration.millis(0),
        Duration.millis(100),
        () -> {
          if (count.incrementAndGet() >= 3) {
            latch.countDown();
            return ScheduledExecutors.Signal.STOP;
          }
          return ScheduledExecutors.Signal.REPEAT;
        }
    );

    Assert.assertTrue("Should complete", latch.await(5, TimeUnit.SECONDS));
    exec.shutdown();

    final List<ServiceMetricEvent> events = emitter.getMetricEvents(METRIC_NAME);
    // 3 executions = 3 schedule calls that ran = 3 metrics
    Assert.assertEquals(
        "Should emit one metric per schedule call",
        3,
        events.size()
    );

    for (final ServiceMetricEvent event : events) {
      Assert.assertEquals("test", event.getUserDims().get("scheduler"));
    }
  }

  @Test
  public void testExecuteEmitsLagMetric() throws Exception
  {
    final StubServiceEmitter emitter = new StubServiceEmitter();
    final ScheduledExecutorService exec = ScheduledExecutors.emittingDelayMetric(
        Execs.scheduledSingleThreaded("test-%d"),
        emitter,
        METRIC_NAME,
        ImmutableMap.of("component", "testComponent")
    );

    final CountDownLatch latch = new CountDownLatch(1);
    exec.execute(latch::countDown);

    Assert.assertTrue("Task should complete", latch.await(5, TimeUnit.SECONDS));
    exec.shutdown();

    final List<ServiceMetricEvent> events = emitter.getMetricEvents(METRIC_NAME);
    Assert.assertEquals("Should emit exactly one metric", 1, events.size());
    Assert.assertTrue("Lag should be >= 0", events.get(0).getValue().longValue() >= 0);
    Assert.assertEquals("testComponent", events.get(0).getUserDims().get("component"));
  }

  @Test
  public void testSubmitRunnableEmitsLagMetric() throws Exception
  {
    final StubServiceEmitter emitter = new StubServiceEmitter();
    final ScheduledExecutorService exec = ScheduledExecutors.emittingDelayMetric(
        Execs.scheduledSingleThreaded("test-%d"),
        emitter,
        METRIC_NAME,
        ImmutableMap.of()
    );

    final CountDownLatch latch = new CountDownLatch(1);
    final Future<?> future = exec.submit(latch::countDown);

    Assert.assertTrue("Task should complete", latch.await(5, TimeUnit.SECONDS));
    future.get(5, TimeUnit.SECONDS);
    exec.shutdown();

    final List<ServiceMetricEvent> events = emitter.getMetricEvents(METRIC_NAME);
    Assert.assertEquals("Should emit exactly one metric", 1, events.size());
    Assert.assertTrue("Lag should be >= 0", events.get(0).getValue().longValue() >= 0);
  }

  @Test
  public void testSubmitRunnableWithResultEmitsLagMetric() throws Exception
  {
    final StubServiceEmitter emitter = new StubServiceEmitter();
    final ScheduledExecutorService exec = ScheduledExecutors.emittingDelayMetric(
        Execs.scheduledSingleThreaded("test-%d"),
        emitter,
        METRIC_NAME,
        ImmutableMap.of()
    );

    final Future<String> future = exec.submit(() -> {}, "result");

    Assert.assertEquals("result", future.get(5, TimeUnit.SECONDS));
    exec.shutdown();

    final List<ServiceMetricEvent> events = emitter.getMetricEvents(METRIC_NAME);
    Assert.assertEquals("Should emit exactly one metric", 1, events.size());
    Assert.assertTrue("Lag should be >= 0", events.get(0).getValue().longValue() >= 0);
  }

  @Test
  public void testSubmitCallableEmitsLagMetric() throws Exception
  {
    final StubServiceEmitter emitter = new StubServiceEmitter();
    final ScheduledExecutorService exec = ScheduledExecutors.emittingDelayMetric(
        Execs.scheduledSingleThreaded("test-%d"),
        emitter,
        METRIC_NAME,
        ImmutableMap.of()
    );

    final Future<String> future = exec.submit(() -> "hello");

    Assert.assertEquals("hello", future.get(5, TimeUnit.SECONDS));
    exec.shutdown();

    final List<ServiceMetricEvent> events = emitter.getMetricEvents(METRIC_NAME);
    Assert.assertEquals("Should emit exactly one metric", 1, events.size());
    Assert.assertTrue("Lag should be >= 0", events.get(0).getValue().longValue() >= 0);
  }

  @Test
  public void testScheduleAtFixedRateThrows()
  {
    final ScheduledExecutorService exec = ScheduledExecutors.emittingDelayMetric(
        Execs.scheduledSingleThreaded("test-%d"),
        new StubServiceEmitter(),
        METRIC_NAME,
        ImmutableMap.of()
    );

    try {
      Assert.assertThrows(
          DruidException.class,
          () -> exec.scheduleAtFixedRate(() -> {}, 0, 100, TimeUnit.MILLISECONDS)
      );
    }
    finally {
      exec.shutdown();
    }
  }

  @Test
  public void testScheduleWithFixedDelayThrows()
  {
    final ScheduledExecutorService exec = ScheduledExecutors.emittingDelayMetric(
        Execs.scheduledSingleThreaded("test-%d"),
        new StubServiceEmitter(),
        METRIC_NAME,
        ImmutableMap.of()
    );

    try {
      Assert.assertThrows(
          DruidException.class,
          () -> exec.scheduleWithFixedDelay(() -> {}, 0, 100, TimeUnit.MILLISECONDS)
      );
    }
    finally {
      exec.shutdown();
    }
  }
}
