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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.internal.AssumptionViolatedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
@RunWith(Parameterized.class)
public class PrioritizedExecutorServiceTest
{
  private PrioritizedExecutorService exec;
  private CountDownLatch latch;
  private CountDownLatch finishLatch;
  private final boolean useFifo;
  private final DruidProcessingConfig config;

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(new Object[]{true}, new Object[]{false});
  }

  public PrioritizedExecutorServiceTest(final boolean useFifo)
  {
    this.useFifo = useFifo;
    this.config = new DruidProcessingConfig()
    {
      @Override
      public String getFormatString()
      {
        return null;
      }

      @Override
      public boolean isFifo()
      {
        return useFifo;
      }
    };
  }

  @Before
  public void setUp()
  {
    exec = PrioritizedExecutorService.create(
        new Lifecycle(),
        new DruidProcessingConfig()
        {
          @Override
          public String getFormatString()
          {
            return "test";
          }

          @Override
          public int getNumThreads()
          {
            return 1;
          }

          @Override
          public boolean isFifo()
          {
            return useFifo;
          }
        }
    );

    latch = new CountDownLatch(1);
    finishLatch = new CountDownLatch(3);
  }

  @After
  public void tearDown()
  {
    exec.shutdownNow();
  }

  /**
   * Submits a normal priority task to block the queue, followed by low, high, normal priority tasks.
   * Tests to see that the high priority task is executed first, followed by the normal and low priority tasks.
   *
   * @throws Exception
   */
  @Test
  public void testSubmit() throws Exception
  {
    final ConcurrentLinkedQueue<Integer> order = new ConcurrentLinkedQueue<Integer>();

    exec.submit(
        new AbstractPrioritizedCallable<Void>(0)
        {
          @Override
          public Void call() throws Exception
          {
            latch.await();
            return null;
          }
        }
    );

    exec.submit(
        new AbstractPrioritizedCallable<Void>(-1)
        {
          @Override
          public Void call()
          {
            order.add(-1);
            finishLatch.countDown();
            return null;
          }
        }
    );
    exec.submit(
        new AbstractPrioritizedCallable<Void>(0)
        {
          @Override
          public Void call()
          {
            order.add(0);
            finishLatch.countDown();
            return null;
          }
        }
    );
    exec.submit(
        new AbstractPrioritizedCallable<Void>(2)
        {
          @Override
          public Void call()
          {
            order.add(2);
            finishLatch.countDown();
            return null;
          }
        }
    );

    latch.countDown();
    finishLatch.await();

    Assert.assertTrue(order.size() == 3);

    List<Integer> expected = ImmutableList.of(2, 0, -1);
    Assert.assertEquals(expected, ImmutableList.copyOf(order));
  }

  // Make sure entries are processed FIFO
  @Test
  public void testOrderedExecutionEqualPriorityRunnable() throws ExecutionException, InterruptedException
  {
    final int numTasks = 100;
    final List<ListenableFuture<?>> futures = Lists.newArrayListWithExpectedSize(numTasks);
    final AtomicInteger hasRun = new AtomicInteger(0);
    for (int i = 0; i < numTasks; ++i) {
      futures.add(exec.submit(getCheckingPrioritizedRunnable(i, hasRun)));
    }
    latch.countDown();
    checkFutures(futures);
  }

  @Test
  public void testOrderedExecutionEqualPriorityCallable() throws ExecutionException, InterruptedException
  {
    final int numTasks = 1_000;
    final List<ListenableFuture<?>> futures = Lists.newArrayListWithExpectedSize(numTasks);
    final AtomicInteger hasRun = new AtomicInteger(0);
    for (int i = 0; i < numTasks; ++i) {
      futures.add(exec.submit(getCheckingPrioritizedCallable(i, hasRun)));
    }
    latch.countDown();
    checkFutures(futures);
  }

  @Test
  public void testOrderedExecutionEqualPriorityMix() throws ExecutionException, InterruptedException
  {
    exec = new PrioritizedExecutorService(exec.threadPoolExecutor, true, 0, config);
    final int numTasks = 1_000;
    final List<ListenableFuture<?>> futures = Lists.newArrayListWithExpectedSize(numTasks);
    final AtomicInteger hasRun = new AtomicInteger(0);
    final Random random = new Random(789401);
    for (int i = 0; i < numTasks; ++i) {
      switch (random.nextInt(4)) {
        case 0:
          futures.add(exec.submit(getCheckingPrioritizedCallable(i, hasRun)));
          break;
        case 1:
          futures.add(exec.submit(getCheckingPrioritizedRunnable(i, hasRun)));
          break;
        case 2:
          futures.add(exec.submit(getCheckingCallable(i, hasRun)));
          break;
        case 3:
          futures.add(exec.submit(getCheckingRunnable(i, hasRun)));
          break;
        default:
          Assert.fail("Bad random result");
      }
    }
    latch.countDown();
    checkFutures(futures);
  }

  @Test
  public void testOrderedExecutionMultiplePriorityMix() throws ExecutionException, InterruptedException
  {
    final int _default = 0;
    final int min = -1;
    final int max = 1;
    exec = new PrioritizedExecutorService(exec.threadPoolExecutor, true, _default, config);
    final int numTasks = 999;
    final int[] priorities = new int[]{max, _default, min};
    final int tasksPerPriority = numTasks / priorities.length;
    final int[] priorityOffsets = new int[]{0, tasksPerPriority, tasksPerPriority * 2};
    final List<ListenableFuture<?>> futures = Lists.newArrayListWithExpectedSize(numTasks);
    final AtomicInteger hasRun = new AtomicInteger(0);
    final Random random = new Random(789401);
    for (int i = 0; i < numTasks; ++i) {
      final int priorityBucket = i % priorities.length;
      final int myPriority = priorities[priorityBucket];
      final int priorityOffset = priorityOffsets[priorityBucket];
      final int expectedPriorityOrder = i / priorities.length;
      if (random.nextBoolean()) {
        futures.add(
            exec.submit(
                getCheckingPrioritizedCallable(
                    priorityOffset + expectedPriorityOrder,
                    hasRun,
                    myPriority
                )
            )
        );
      } else {
        futures.add(
            exec.submit(
                getCheckingPrioritizedRunnable(
                    priorityOffset + expectedPriorityOrder,
                    hasRun,
                    myPriority
                )
            )
        );
      }
    }
    latch.countDown();
    checkFutures(futures);
  }

  private void checkFutures(Iterable<ListenableFuture<?>> futures) throws InterruptedException, ExecutionException
  {
    for (ListenableFuture<?> future : futures) {
      try {
        future.get();
      }
      catch (ExecutionException e) {
        if (!(e.getCause() instanceof AssumptionViolatedException)) {
          throw e;
        }
      }
    }
  }

  private PrioritizedCallable<Boolean> getCheckingPrioritizedCallable(
      final int myOrder,
      final AtomicInteger hasRun
  )
  {
    return getCheckingPrioritizedCallable(myOrder, hasRun, 0);
  }

  private PrioritizedCallable<Boolean> getCheckingPrioritizedCallable(
      final int myOrder,
      final AtomicInteger hasRun,
      final int priority
  )
  {
    final Callable<Boolean> delegate = getCheckingCallable(myOrder, hasRun);
    return new AbstractPrioritizedCallable<Boolean>(priority)
    {
      @Override
      public Boolean call() throws Exception
      {
        return delegate.call();
      }
    };
  }

  private Callable<Boolean> getCheckingCallable(
      final int myOrder,
      final AtomicInteger hasRun
  )
  {
    final Runnable runnable = getCheckingRunnable(myOrder, hasRun);
    return new Callable<Boolean>()
    {
      @Override
      public Boolean call()
      {
        runnable.run();
        return true;
      }
    };
  }

  private Runnable getCheckingRunnable(
      final int myOrder,
      final AtomicInteger hasRun
  )
  {
    return new Runnable()
    {
      @Override
      public void run()
      {
        try {
          latch.await();
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
        if (useFifo) {
          Assert.assertEquals(myOrder, hasRun.getAndIncrement());
        } else {
          Assume.assumeTrue(Integer.compare(myOrder, hasRun.getAndIncrement()) == 0);
        }
      }
    };
  }


  private PrioritizedRunnable getCheckingPrioritizedRunnable(
      final int myOrder,
      final AtomicInteger hasRun
  )
  {
    return getCheckingPrioritizedRunnable(myOrder, hasRun, 0);
  }

  private PrioritizedRunnable getCheckingPrioritizedRunnable(
      final int myOrder,
      final AtomicInteger hasRun,
      final int priority
  )
  {
    final Runnable delegate = getCheckingRunnable(myOrder, hasRun);
    return new PrioritizedRunnable()
    {
      @Override
      public int getPriority()
      {
        return priority;
      }

      @Override
      public void run()
      {
        delegate.run();
      }
    };
  }
}
