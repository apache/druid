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

package org.apache.druid.query;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class ForwardingQueryProcessingPoolTest
{
  private ExecutorService exec;
  private ScheduledExecutorService timeoutService;
  private ForwardingQueryProcessingPool pool;

  @BeforeEach
  public void setUp()
  {
    // Single threaded, so that a second task submitted has to wait in the queue for the first one to finish.
    exec = Execs.singleThreaded("ForwardingQueryProcessingPoolTest-%d");
    timeoutService = Execs.scheduledSingleThreaded("ForwardingQueryProcessingPoolTest-timeout-%d");
    pool = new ForwardingQueryProcessingPool(exec, timeoutService);
  }

  @AfterEach
  public void tearDown()
  {
    pool.shutdownNow();
    exec.shutdownNow();
    timeoutService.shutdownNow();
  }

  /**
   * The timeout of a task must not run down while the task is still waiting in the pool's queue.
   */
  @Test
  @Timeout(30)
  public void testTimeoutStartsWhenTaskIsPickedUpAndNotWhenSubmitted() throws Exception
  {
    final CountDownLatch blockerStarted = new CountDownLatch(1);
    final CountDownLatch releaseBlocker = new CountDownLatch(1);

    final ListenableFuture<Integer> blockerFuture = pool.submitRunnerTask(callable(() -> {
      blockerStarted.countDown();
      releaseBlocker.await();
      return 1;
    }));
    blockerStarted.await();

    // Queued behind the blocker, with a timeout much shorter than the time it spends waiting.
    final ListenableFuture<Integer> queuedFuture = pool.submitRunnerTask(
        callable(() -> 2),
        500,
        TimeUnit.MILLISECONDS
    );

    Assertions.assertFalse(queuedFuture.isDone());
    Thread.sleep(1500);
    Assertions.assertFalse(queuedFuture.isDone(), "Queued task should not time out before it starts running");

    releaseBlocker.countDown();
    Assertions.assertEquals(1, blockerFuture.get().intValue());
    Assertions.assertEquals(2, queuedFuture.get().intValue());
  }

  @Test
  @Timeout(30)
  public void testTimeoutFiresOnceTheTaskIsRunningForTooLong()
  {
    final ListenableFuture<Integer> future = pool.submitRunnerTask(
        callable(() -> {
          Thread.sleep(60_000L);
          return 1;
        }),
        100,
        TimeUnit.MILLISECONDS
    );

    final ExecutionException e = Assertions.assertThrows(ExecutionException.class, future::get);
    Assertions.assertInstanceOf(TimeoutException.class, e.getCause());
  }

  @Test
  @Timeout(30)
  public void testTaskCompletesNormallyWithinTimeout() throws Exception
  {
    Assertions.assertEquals(
        1,
        pool.submitRunnerTask(callable(() -> 1), 30_000, TimeUnit.MILLISECONDS).get().intValue()
    );
  }

  /**
   * Cancelling the returned future must cancel the underlying task even while it is still sitting in the queue.
   */
  @Test
  @Timeout(30)
  public void testCancelWhileQueued() throws Exception
  {
    final CountDownLatch blockerStarted = new CountDownLatch(1);
    final CountDownLatch releaseBlocker = new CountDownLatch(1);
    final AtomicBoolean queuedTaskRan = new AtomicBoolean(false);

    final ListenableFuture<Integer> blockerFuture = pool.submitRunnerTask(callable(() -> {
      blockerStarted.countDown();
      releaseBlocker.await();
      return 1;
    }));
    blockerStarted.await();

    final ListenableFuture<Integer> queuedFuture = pool.submitRunnerTask(
        callable(() -> {
          queuedTaskRan.set(true);
          return 2;
        }),
        30_000,
        TimeUnit.MILLISECONDS
    );
    Assertions.assertTrue(queuedFuture.cancel(true));

    releaseBlocker.countDown();
    Assertions.assertEquals(1, blockerFuture.get().intValue());
    // Give the pool a chance to (incorrectly) run the cancelled task.
    Thread.sleep(500);
    Assertions.assertFalse(queuedTaskRan.get(), "Cancelled task should not have been run");
  }

  /**
   * A cancelled task should not leave the returned future hanging, since it never gets to start.
   */
  @Test
  @Timeout(30)
  public void testCancelWhileQueuedCompletesReturnedFuture() throws Exception
  {
    final CountDownLatch blockerStarted = new CountDownLatch(1);
    final CountDownLatch releaseBlocker = new CountDownLatch(1);

    final ListenableFuture<Integer> blockerFuture = pool.submitRunnerTask(callable(() -> {
      blockerStarted.countDown();
      releaseBlocker.await();
      return 1;
    }));
    blockerStarted.await();

    final ListenableFuture<Integer> queuedFuture = pool.submitRunnerTask(
        callable(() -> 2),
        30_000,
        TimeUnit.MILLISECONDS
    );
    queuedFuture.cancel(true);
    releaseBlocker.countDown();

    Assertions.assertEquals(1, blockerFuture.get().intValue());
    Assertions.assertTrue(queuedFuture.isDone());
    Assertions.assertTrue(queuedFuture.isCancelled());
  }

  @Test
  @Timeout(30)
  public void testPriorityAndRunnerArePreserved()
  {
    final QueryRunner<Object> runner = (queryPlus, responseContext) -> null;
    final PrioritizedQueryRunnerCallable<Integer, Object> task =
        new AbstractPrioritizedQueryRunnerCallable<>(7, runner)
        {
          @Override
          public Integer call()
          {
            return 1;
          }
        };

    final PrioritizedQueryRunnerCallable<?, ?>[] submitted = new PrioritizedQueryRunnerCallable<?, ?>[1];
    final ForwardingQueryProcessingPool capturingPool = new ForwardingQueryProcessingPool(exec, timeoutService)
    {
      @Override
      public <T, V> ListenableFuture<T> submitRunnerTask(PrioritizedQueryRunnerCallable<T, V> task)
      {
        submitted[0] = task;
        return super.submitRunnerTask(task);
      }
    };

    capturingPool.submitRunnerTask(task, 30_000, TimeUnit.MILLISECONDS);
    Assertions.assertEquals(7, submitted[0].getPriority());
    Assertions.assertSame(runner, submitted[0].getRunner());
  }

  private static PrioritizedQueryRunnerCallable<Integer, Object> callable(ThrowingSupplier supplier)
  {
    return new AbstractPrioritizedQueryRunnerCallable<>(0, null)
    {
      @Override
      public Integer call() throws Exception
      {
        return supplier.get();
      }
    };
  }

  private interface ThrowingSupplier
  {
    Integer get() throws Exception;
  }
}
