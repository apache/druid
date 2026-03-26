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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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

    Assertions.assertTrue(latch.await(5, TimeUnit.SECONDS), "Task should complete");
    exec.shutdown();

    final List<ServiceMetricEvent> events = emitter.getMetricEvents(METRIC_NAME);
    Assertions.assertEquals(1, events.size(), "Should emit exactly one metric");

    final ServiceMetricEvent event = events.get(0);
    Assertions.assertTrue(event.getValue().longValue() >= 0, "Lag should be >= 0");
    Assertions.assertEquals("testComponent", event.getUserDims().get("component"));
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

    Assertions.assertEquals("hello", result);

    final List<ServiceMetricEvent> events = emitter.getMetricEvents(METRIC_NAME);
    Assertions.assertEquals(1, events.size(), "Should emit exactly one metric");
    Assertions.assertTrue(events.get(0).getValue().longValue() >= 0, "Lag should be >= 0");
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

    Assertions.assertTrue(latch.await(5, TimeUnit.SECONDS), "Task should complete");
    exec.shutdown();

    final List<ServiceMetricEvent> events = emitter.getMetricEvents(METRIC_NAME);
    Assertions.assertEquals(1, events.size());
    Assertions.assertTrue(
        events.get(0).getValue().longValue() < 100,
        "Lag should be less than 100ms, was: " + events.get(0).getValue().longValue()
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

    Assertions.assertTrue(latch.await(5, TimeUnit.SECONDS), "Should complete");
    exec.shutdown();

    final List<ServiceMetricEvent> events = emitter.getMetricEvents(METRIC_NAME);
    // 3 executions = 3 schedule calls that ran = 3 metrics
    Assertions.assertEquals(3, events.size(), "Should emit one metric per schedule call");

    for (final ServiceMetricEvent event : events) {
      Assertions.assertEquals("test", event.getUserDims().get("scheduler"));
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

    Assertions.assertTrue(latch.await(5, TimeUnit.SECONDS), "Task should complete");
    exec.shutdown();

    final List<ServiceMetricEvent> events = emitter.getMetricEvents(METRIC_NAME);
    Assertions.assertEquals(1, events.size(), "Should emit exactly one metric");
    Assertions.assertTrue(events.get(0).getValue().longValue() >= 0, "Lag should be >= 0");
    Assertions.assertEquals("testComponent", events.get(0).getUserDims().get("component"));
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

    Assertions.assertTrue(latch.await(5, TimeUnit.SECONDS), "Task should complete");
    future.get(5, TimeUnit.SECONDS);
    exec.shutdown();

    final List<ServiceMetricEvent> events = emitter.getMetricEvents(METRIC_NAME);
    Assertions.assertEquals(1, events.size(), "Should emit exactly one metric");
    Assertions.assertTrue(events.get(0).getValue().longValue() >= 0, "Lag should be >= 0");
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

    Assertions.assertEquals("result", future.get(5, TimeUnit.SECONDS));
    exec.shutdown();

    final List<ServiceMetricEvent> events = emitter.getMetricEvents(METRIC_NAME);
    Assertions.assertEquals(1, events.size(), "Should emit exactly one metric");
    Assertions.assertTrue(events.get(0).getValue().longValue() >= 0, "Lag should be >= 0");
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

    Assertions.assertEquals("hello", future.get(5, TimeUnit.SECONDS));
    exec.shutdown();

    final List<ServiceMetricEvent> events = emitter.getMetricEvents(METRIC_NAME);
    Assertions.assertEquals(1, events.size(), "Should emit exactly one metric");
    Assertions.assertTrue(events.get(0).getValue().longValue() >= 0, "Lag should be >= 0");
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
      Assertions.assertThrows(
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
      Assertions.assertThrows(
          DruidException.class,
          () -> exec.scheduleWithFixedDelay(() -> {}, 0, 100, TimeUnit.MILLISECONDS)
      );
    }
    finally {
      exec.shutdown();
    }
  }
}
