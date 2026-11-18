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

package org.apache.druid.query.context;

import org.apache.druid.guice.StartupInjectorBuilder;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class RequestHeadersTest
{
  private final RequestHeaderContextConfig config = new RequestHeaderContextConfig(Map.of("X-Trace", "traceId"));

  @Test
  public void testDisabledPropagationBypassesCaptureAndWrapping() throws Exception
  {
    final boolean previous = RequestHeaders.isEnabled();
    final Properties properties = new Properties();
    properties.setProperty("druid.request.headers.headerToContextKey", "{}");
    try {
      new StartupInjectorBuilder().withProperties(properties).forTests().build();
      Assertions.assertFalse(RequestHeaders.isEnabled());
      Assertions.assertTrue(config.capture(header -> {
        throw new AssertionError("Disabled capture must not read headers");
      }).isEmpty());
      final Map<String, Object> context = Map.of("traceId", "existing-trace");
      Assertions.assertTrue(config.fromContext(context).isEmpty());
      final Runnable runnable = () -> {};
      final Callable<Integer> callable = () -> 1;
      final Executor direct = Runnable::run;
      final Sequence<Integer> sequence = Sequences.simple(List.of(1));
      try (ExecutorService worker = Executors.newSingleThreadExecutor();
           ScheduledThreadPoolExecutor scheduled = RequestHeaders.scheduled(1, Execs.makeThreadFactory("disabled-%d"));
           ExecutorService fixed = Execs.multiThreaded(1, "disabled-fixed-%d");
           ExecutorService blocking = Execs.newBlockingSingleThreaded("disabled-blocking-%d", 1);
           RequestHeaders.Scope ignored = RequestHeaders.open(config, context)) {
        Assertions.assertTrue(RequestHeaders.current().isEmpty());
        Assertions.assertTrue(RequestHeaders.currentContext().isEmpty());
        Assertions.assertSame(runnable, RequestHeaders.wrap(runnable));
        Assertions.assertSame(callable, RequestHeaders.wrap(callable));
        Assertions.assertSame(direct, RequestHeaders.bind(direct));
        Assertions.assertSame(worker, RequestHeaders.wrap(worker));
        Assertions.assertSame(sequence, RequestHeaders.wrap(sequence));
        Assertions.assertEquals(ScheduledThreadPoolExecutor.class, scheduled.getClass());
        Assertions.assertEquals(ThreadPoolExecutor.class, fixed.getClass());
        Assertions.assertEquals(ThreadPoolExecutor.class, blocking.getClass());
      }
    }
    finally {
      if (previous) {
        properties.remove("druid.request.headers.headerToContextKey");
      }
      new StartupInjectorBuilder().withProperties(properties).forTests().build();
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"single", "fixed", "blocking", "scheduled", "scheduledKeepAlive"})
  public void testQueuedRequestsAndFailuresDoNotLeakBetweenWorkerUses(final String pool) throws Exception
  {
    final ExecutorService executor = switch (pool) {
      case "fixed" -> Execs.multiThreaded(1, "trace-worker-%d");
      case "blocking" -> Execs.newBlockingSingleThreaded("trace-worker-%d", 10);
      case "scheduled" -> ScheduledExecutors.fixed(1, "trace-worker-%d");
      case "scheduledKeepAlive" -> ScheduledExecutors.fixedWithKeepAliveTime(1, "trace-worker-%d", 1000);
      default -> Execs.singleThreaded("trace-worker-%d");
    };
    final CountDownLatch release = new CountDownLatch(1);
    try {
      Assertions.assertThrows(NullPointerException.class, () -> executor.execute(null));
      Assertions.assertThrows(NullPointerException.class, () -> executor.submit((Runnable) null));
      Assertions.assertThrows(NullPointerException.class, () -> executor.submit((Callable<Void>) null));
      executor.submit(() -> {
        release.await();
        return null;
      });
      final Future<Map<String, String>> first;
      final Future<Map<String, String>> second;
      final Future<?> failing;
      try (RequestHeaders.Scope ignored = RequestHeaders.open(config, Map.of("traceId", "first"))) {
        first = executor.submit(RequestHeaders::current);
        failing = executor.submit(() -> {
          Assertions.assertEquals("first", RequestHeaders.current().get("X-Trace"));
          throw new IllegalStateException("expected");
        });
      }
      try (RequestHeaders.Scope ignored = RequestHeaders.open(config, Map.of("traceId", "second"))) {
        second = executor.submit(RequestHeaders::current);
      }
      final Future<Map<String, String>> empty = executor.submit(RequestHeaders::current);
      release.countDown();
      Assertions.assertEquals(Map.of("X-Trace", "first"), first.get(10, TimeUnit.SECONDS));
      Assertions.assertThrows(ExecutionException.class, () -> failing.get(10, TimeUnit.SECONDS));
      Assertions.assertEquals(Map.of("X-Trace", "second"), second.get(10, TimeUnit.SECONDS));
      Assertions.assertTrue(empty.get(10, TimeUnit.SECONDS).isEmpty());
      Assertions.assertTrue(RequestHeaders.current().isEmpty());
    }
    finally {
      release.countDown();
      executor.shutdownNow();
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"fixed", "blocking"})
  public void testCanceledFuturesCanBePurged(final String pool) throws Exception
  {
    final ThreadPoolExecutor executor = (ThreadPoolExecutor) (
        "fixed".equals(pool) ? Execs.multiThreaded(1, "trace-purge-%d") : Execs.newBlockingSingleThreaded("trace-purge-%d", 10)
    );
    final CountDownLatch started = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    try {
      executor.submit(() -> {
        started.countDown();
        release.await();
        return null;
      });
      Assertions.assertTrue(started.await(10, TimeUnit.SECONDS));
      final Future<?> canceled = executor.submit(() -> {});
      Assertions.assertTrue(canceled.cancel(false));
      executor.purge();
      Assertions.assertTrue(executor.getQueue().isEmpty());
    }
    finally {
      release.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  public void testScheduledWorkCapturesAndRestoresHeaders() throws Exception
  {
    final ScheduledExecutorService executor = Execs.scheduledSingleThreaded("trace-scheduled-%d");
    try {
      Assertions.assertThrows(NullPointerException.class, () -> executor.schedule((Runnable) null, 1, TimeUnit.SECONDS));
      Assertions.assertThrows(NullPointerException.class, () -> executor.schedule((Callable<Void>) null, 1, TimeUnit.SECONDS));
      Assertions.assertThrows(NullPointerException.class, () -> executor.scheduleAtFixedRate(null, 0, 1, TimeUnit.SECONDS));
      Assertions.assertThrows(NullPointerException.class, () -> executor.scheduleWithFixedDelay(null, 0, 1, TimeUnit.SECONDS));
      final Future<Map<String, String>> future;
      try (RequestHeaders.Scope ignored = RequestHeaders.open(config, Map.of("traceId", "scheduled"))) {
        future = executor.schedule(RequestHeaders::current, 1, TimeUnit.MILLISECONDS);
      }
      Assertions.assertEquals(Map.of("X-Trace", "scheduled"), future.get(10, TimeUnit.SECONDS));
      Assertions.assertTrue(executor.submit(RequestHeaders::current).get(10, TimeUnit.SECONDS).isEmpty());
    }
    finally {
      executor.shutdownNow();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testPeriodicWorkRetainsHeadersAndCancellation(final boolean fixedRate) throws Exception
  {
    final ScheduledExecutorService executor = Execs.scheduledSingleThreaded("trace-periodic-%d");
    final CountDownLatch repeated = new CountDownLatch(2);
    try {
      final ScheduledFuture<?> future;
      try (RequestHeaders.Scope ignored = RequestHeaders.open(config, Map.of("traceId", "periodic"))) {
        final Runnable task = () -> {
          Assertions.assertEquals("periodic", RequestHeaders.current().get("X-Trace"));
          repeated.countDown();
        };
        future = fixedRate
                 ? executor.scheduleAtFixedRate(task, 0, 1, TimeUnit.MILLISECONDS)
                 : executor.scheduleWithFixedDelay(task, 0, 1, TimeUnit.MILLISECONDS);
      }
      Assertions.assertTrue(repeated.await(10, TimeUnit.SECONDS));
      Assertions.assertTrue(future.cancel(false));
      Assertions.assertTrue(future.isCancelled());
      Assertions.assertTrue(executor.submit(RequestHeaders::current).get(10, TimeUnit.SECONDS).isEmpty());
    }
    finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void testLazyIterationAndCloseRestoreTheirOperationContext() throws Exception
  {
    final Sequence<Integer> sequence;
    try (RequestHeaders.Scope ignored = RequestHeaders.open(config, Map.of("traceId", "query"))) {
      sequence = RequestHeaders.wrap(Sequences.simple(List.of(1, 2)).map(value -> {
        Assertions.assertEquals("query", RequestHeaders.current().get("X-Trace"));
        return value;
      }).withBaggage(() -> Assertions.assertEquals("query", RequestHeaders.current().get("X-Trace"))));
    }
    try (RequestHeaders.Scope ignored = RequestHeaders.open(config, Map.of("traceId", "other"))) {
      try (Yielder<Integer> yielder = Yielders.each(sequence)) {
        Assertions.assertEquals(1, yielder.get());
        Assertions.assertEquals(2, yielder.next(null).get());
      }
      Assertions.assertEquals("other", RequestHeaders.current().get("X-Trace"));
      Assertions.assertEquals(List.of(1, 2), sequence.toList());
      Assertions.assertEquals("other", RequestHeaders.current().get("X-Trace"));
    }
    Assertions.assertTrue(RequestHeaders.current().isEmpty());
  }
}
