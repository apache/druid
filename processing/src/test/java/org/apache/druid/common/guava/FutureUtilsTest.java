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

package org.apache.druid.common.guava;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.java.util.common.Either;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class FutureUtilsTest
{
  private ExecutorService exec;

  @BeforeEach
  public void setUp()
  {
    exec = Execs.singleThreaded(StringUtils.encodeForFormat(getClass().getName()) + "-%d");
  }

  @AfterEach
  public void tearDown()
  {
    if (exec != null) {
      exec.shutdownNow();
      exec = null;
    }
  }

  @Test
  public void test_get_ok() throws Exception
  {
    final String s = FutureUtils.get(Futures.immediateFuture("x"), true);
    Assertions.assertEquals("x", s);
  }

  @Test
  public void test_get_failed()
  {
    final ExecutionException e = Assertions.assertThrows(
        ExecutionException.class,
        () -> FutureUtils.get(Futures.immediateFailedFuture(new ISE("oh no")), true)
    );

    Assertions.assertInstanceOf(IllegalStateException.class, e.getCause());
    Assertions.assertTrue(e.getCause().getMessage().contains("oh no"));
  }

  @Test
  public void test_getUnchecked_interrupted_cancelOnInterrupt() throws InterruptedException
  {
    final SettableFuture<String> neverGoingToResolve = SettableFuture.create();
    final AtomicReference<Throwable> exceptionFromOtherThread = new AtomicReference<>();
    final CountDownLatch runningLatch = new CountDownLatch(1);

    final Future<?> execResult = exec.submit(() -> {
      runningLatch.countDown();

      try {
        FutureUtils.getUnchecked(neverGoingToResolve, true);
      }
      catch (Throwable t) {
        exceptionFromOtherThread.set(t);
      }
    });

    runningLatch.await();
    Assertions.assertTrue(execResult.cancel(true));
    exec.shutdown();

    Assertions.assertTrue(exec.awaitTermination(1, TimeUnit.MINUTES));
    exec = null;

    Assertions.assertTrue(neverGoingToResolve.isCancelled());

    final Throwable e = exceptionFromOtherThread.get();
    Assertions.assertInstanceOf(RuntimeException.class, e);
    Assertions.assertInstanceOf(InterruptedException.class, e.getCause());
  }

  @Test
  public void test_getUnchecked_interrupted_dontCancelOnInterrupt() throws InterruptedException
  {
    final SettableFuture<String> neverGoingToResolve = SettableFuture.create();
    final AtomicReference<Throwable> exceptionFromOtherThread = new AtomicReference<>();
    final CountDownLatch runningLatch = new CountDownLatch(1);

    final Future<?> execResult = exec.submit(() -> {
      runningLatch.countDown();

      try {
        FutureUtils.getUnchecked(neverGoingToResolve, false);
      }
      catch (Throwable t) {
        exceptionFromOtherThread.set(t);
      }
    });

    runningLatch.await();
    Assertions.assertTrue(execResult.cancel(true));
    exec.shutdown();

    Assertions.assertTrue(exec.awaitTermination(1, TimeUnit.MINUTES));
    exec = null;

    Assertions.assertFalse(neverGoingToResolve.isCancelled());
    Assertions.assertFalse(neverGoingToResolve.isDone());

    final Throwable e = exceptionFromOtherThread.get();
    Assertions.assertInstanceOf(RuntimeException.class, e);
    Assertions.assertInstanceOf(InterruptedException.class, e.getCause());
  }

  @Test
  public void test_getUnchecked_ok()
  {
    final String s = FutureUtils.getUnchecked(Futures.immediateFuture("x"), true);
    Assertions.assertEquals("x", s);
  }

  @Test
  public void test_getUnchecked_failed()
  {
    final RuntimeException e = Assertions.assertThrows(
        RuntimeException.class,
        () -> FutureUtils.getUnchecked(Futures.immediateFailedFuture(new ISE("oh no")), true)
    );

    Assertions.assertInstanceOf(IllegalStateException.class, e.getCause());
    Assertions.assertTrue(e.getCause().getMessage().contains("oh no"));
  }

  @Test
  public void test_getUncheckedImmediately_ok()
  {
    final String s = FutureUtils.getUncheckedImmediately(Futures.immediateFuture("x"));
    Assertions.assertEquals("x", s);
  }

  @Test
  public void test_getUncheckedImmediately_failed()
  {
    final RuntimeException e = Assertions.assertThrows(
        RuntimeException.class,
        () -> FutureUtils.getUncheckedImmediately(Futures.immediateFailedFuture(new ISE("oh no")))
    );

    Assertions.assertInstanceOf(IllegalStateException.class, e.getCause());
    Assertions.assertTrue(e.getCause().getMessage().contains("oh no"));
  }

  @Test
  public void test_getUncheckedImmediately_notResolved()
  {
    Assertions.assertThrows(
        IllegalStateException.class,
        () -> FutureUtils.getUncheckedImmediately(SettableFuture.create())
    );
  }

  @Test
  public void test_transform() throws Exception
  {
    Assertions.assertEquals(
        "xy",
        FutureUtils.transform(Futures.immediateFuture("x"), s -> s + "y").get()
    );
  }

  @Test
  public void test_transform_error()
  {
    final ListenableFuture<String> future = FutureUtils.transform(
        Futures.immediateFuture("x"),
        s -> {
          throw new ISE("oops");
        }
    );

    Assertions.assertTrue(future.isDone());
    final ExecutionException e = Assertions.assertThrows(
        ExecutionException.class,
        future::get
    );

    final Throwable cause = Assertions.assertInstanceOf(IllegalStateException.class, e.getCause());
    Assertions.assertTrue(cause.getMessage().startsWith("oops"));
  }

  @Test
  public void test_transformAsync() throws Exception
  {
    Assertions.assertEquals(
        "xy",
        FutureUtils.transformAsync(Futures.immediateFuture("x"), s -> Futures.immediateFuture(s + "y")).get()
    );
  }

  @Test
  public void test_transformAsync_exceptionInFunction()
  {
    final ListenableFuture<Object> f = FutureUtils.transformAsync(
        Futures.immediateFuture("x"),
        s -> {
          throw new ISE("error!");
        }
    );

    final ExecutionException e = Assertions.assertThrows(
        ExecutionException.class,
        f::get
    );

    final Throwable cause = Assertions.assertInstanceOf(ISE.class, e.getCause());
    Assertions.assertTrue(cause.getMessage().contains("error!"));
  }

  @Test
  public void test_coalesce_allOk() throws Exception
  {
    final List<ListenableFuture<String>> futures = new ArrayList<>();

    futures.add(Futures.immediateFuture("foo"));
    futures.add(Futures.immediateFuture("bar"));
    futures.add(Futures.immediateFuture(null));

    Assertions.assertEquals(
        ImmutableList.of(Either.value("foo"), Either.value("bar"), Either.value(null)),
        FutureUtils.coalesce(futures).get()
    );
  }

  @Test
  public void test_coalesce_inputError() throws Exception
  {
    final List<ListenableFuture<String>> futures = new ArrayList<>();

    final ISE e = new ISE("oops");
    futures.add(Futures.immediateFuture("foo"));
    futures.add(Futures.immediateFailedFuture(e));
    futures.add(Futures.immediateFuture(null));

    Assertions.assertEquals(
        ImmutableList.of(Either.value("foo"), Either.error(e), Either.value(null)),
        FutureUtils.coalesce(futures).get()
    );
  }

  @Test
  public void test_coalesce_inputCanceled() throws Exception
  {
    final List<ListenableFuture<String>> futures = new ArrayList<>();

    futures.add(Futures.immediateFuture("foo"));
    futures.add(Futures.immediateCancelledFuture());
    futures.add(Futures.immediateFuture(null));

    final List<Either<Throwable, String>> results = FutureUtils.coalesce(futures).get();
    Assertions.assertEquals(3, results.size());
    Assertions.assertEquals(Either.value("foo"), results.get(0));
    Assertions.assertTrue(results.get(1).isError());
    Assertions.assertEquals(Either.value(null), results.get(2));

    Assertions.assertInstanceOf(CancellationException.class, results.get(1).error());
  }

  @Test
  public void test_coalesce_timeout()
  {
    final List<ListenableFuture<String>> futures = new ArrayList<>();
    final SettableFuture<String> unresolvedFuture = SettableFuture.create();

    futures.add(Futures.immediateFuture("foo"));
    futures.add(unresolvedFuture);
    futures.add(Futures.immediateFuture(null));

    final ListenableFuture<List<Either<Throwable, String>>> coalesced = FutureUtils.coalesce(futures);

    Assertions.assertThrows(
        TimeoutException.class,
        () -> coalesced.get(10, TimeUnit.MILLISECONDS)
    );
  }

  @Test
  public void test_coalesce_cancel()
  {
    final List<ListenableFuture<String>> futures = new ArrayList<>();
    final SettableFuture<String> unresolvedFuture = SettableFuture.create();

    futures.add(Futures.immediateFuture("foo"));
    futures.add(unresolvedFuture);
    futures.add(Futures.immediateFuture(null));

    final ListenableFuture<List<Either<Throwable, String>>> coalesced = FutureUtils.coalesce(futures);
    coalesced.cancel(true);

    Assertions.assertTrue(coalesced.isCancelled());

    // All input futures are canceled too.
    Assertions.assertTrue(unresolvedFuture.isCancelled());
  }

  @Test
  public void test_futureWithBaggage_ok() throws ExecutionException, InterruptedException
  {
    final AtomicLong baggageHandled = new AtomicLong(0);
    final SettableFuture<Long> future = SettableFuture.create();
    final ListenableFuture<Long> futureWithBaggage = FutureUtils.futureWithBaggage(
        future,
        baggageHandled::incrementAndGet
    );
    future.set(3L);
    Assertions.assertEquals(3L, (long) futureWithBaggage.get());
    Assertions.assertEquals(1, baggageHandled.get());
  }

  @Test
  public void test_futureWithBaggage_failure()
  {
    final AtomicLong baggageHandled = new AtomicLong(0);
    final SettableFuture<Long> future = SettableFuture.create();
    final ListenableFuture<Long> futureWithBaggage = FutureUtils.futureWithBaggage(
        future,
        baggageHandled::incrementAndGet
    );
    future.setException(new ISE("error!"));

    final ExecutionException e = Assertions.assertThrows(ExecutionException.class, futureWithBaggage::get);
    Assertions.assertInstanceOf(IllegalStateException.class, e.getCause());
    Assertions.assertEquals("error!", e.getCause().getMessage());
    Assertions.assertEquals(1, baggageHandled.get());
  }
}
