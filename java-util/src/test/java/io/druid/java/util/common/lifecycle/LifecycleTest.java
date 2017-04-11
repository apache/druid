/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.java.util.common.lifecycle;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.druid.java.util.common.ISE;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class LifecycleTest
{
  @Test
  public void testConcurrentStartStopOnce() throws Exception
  {
    final int numThreads = 10;
    ListeningExecutorService executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(numThreads));

    final Lifecycle lifecycle = new Lifecycle();
    final AtomicLong startedCount = new AtomicLong(0L);
    final AtomicLong failedCount = new AtomicLong(0L);
    final Lifecycle.Handler exceptionalHandler = new Lifecycle.Handler()
    {
      final AtomicBoolean started = new AtomicBoolean(false);

      @Override
      public void start() throws Exception
      {
        if (!started.compareAndSet(false, true)) {
          failedCount.incrementAndGet();
          throw new ISE("Already started");
        }
        startedCount.incrementAndGet();
      }

      @Override
      public void stop()
      {
        if (!started.compareAndSet(true, false)) {
          failedCount.incrementAndGet();
          throw new ISE("Not yet started started");
        }
      }
    };
    lifecycle.addHandler(exceptionalHandler);
    Collection<ListenableFuture<?>> futures = new ArrayList<>(numThreads);
    final CyclicBarrier barrier = new CyclicBarrier(numThreads);
    final AtomicBoolean started = new AtomicBoolean(false);
    for (int i = 0; i < numThreads; ++i) {
      futures.add(
          executorService.submit(
              new Runnable()
              {
                @Override
                public void run()
                {
                  try {
                    for (int i = 0; i < 1024; ++i) {
                      if (started.compareAndSet(false, true)) {
                        lifecycle.start();
                      }
                      barrier.await();
                      lifecycle.stop();
                      barrier.await();
                      started.set(false);
                      barrier.await();
                    }
                  }
                  catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw Throwables.propagate(e);
                  }
                  catch (Exception e) {
                    throw Throwables.propagate(e);
                  }
                }
              }
          )
      );
    }
    try {
      Futures.allAsList(futures).get();
    }
    finally {
      lifecycle.stop();
    }
    Assert.assertEquals(0, failedCount.get());
    Assert.assertTrue(startedCount.get() > 0);
    executorService.shutdownNow();
  }

  @Test
  public void testStartStopOnce() throws Exception
  {
    final Lifecycle lifecycle = new Lifecycle();
    final AtomicLong startedCount = new AtomicLong(0L);
    final AtomicLong failedCount = new AtomicLong(0L);
    Lifecycle.Handler exceptionalHandler = new Lifecycle.Handler()
    {
      final AtomicBoolean started = new AtomicBoolean(false);

      @Override
      public void start() throws Exception
      {
        if (!started.compareAndSet(false, true)) {
          failedCount.incrementAndGet();
          throw new ISE("Already started");
        }
        startedCount.incrementAndGet();
      }

      @Override
      public void stop()
      {
        if (!started.compareAndSet(true, false)) {
          failedCount.incrementAndGet();
          throw new ISE("Not yet started started");
        }
      }
    };
    lifecycle.addHandler(exceptionalHandler);
    lifecycle.start();
    lifecycle.stop();
    lifecycle.stop();
    lifecycle.stop();
    lifecycle.start();
    lifecycle.stop();
    Assert.assertEquals(2, startedCount.get());
    Assert.assertEquals(0, failedCount.get());
    Exception ex = null;
    try {
      exceptionalHandler.stop();
    }
    catch (Exception e) {
      ex = e;
    }
    Assert.assertNotNull("Should have exception", ex);
  }

  @Test
  public void testSanity() throws Exception
  {
    Lifecycle lifecycle = new Lifecycle();

    List<Integer> startOrder = Lists.newArrayList();
    List<Integer> stopOrder = Lists.newArrayList();

    lifecycle.addManagedInstance(new ObjectToBeLifecycled(0, startOrder, stopOrder));
    lifecycle.addManagedInstance(new ObjectToBeLifecycled(1, startOrder, stopOrder), Lifecycle.Stage.NORMAL);
    lifecycle.addManagedInstance(new ObjectToBeLifecycled(2, startOrder, stopOrder), Lifecycle.Stage.NORMAL);
    lifecycle.addManagedInstance(new ObjectToBeLifecycled(3, startOrder, stopOrder), Lifecycle.Stage.LAST);
    lifecycle.addStartCloseInstance(new ObjectToBeLifecycled(4, startOrder, stopOrder));
    lifecycle.addManagedInstance(new ObjectToBeLifecycled(5, startOrder, stopOrder));
    lifecycle.addStartCloseInstance(new ObjectToBeLifecycled(6, startOrder, stopOrder), Lifecycle.Stage.LAST);
    lifecycle.addManagedInstance(new ObjectToBeLifecycled(7, startOrder, stopOrder));

    final List<Integer> expectedOrder = Arrays.asList(0, 1, 2, 4, 5, 7, 3, 6);

    lifecycle.start();

    Assert.assertEquals(8, startOrder.size());
    Assert.assertEquals(0, stopOrder.size());
    Assert.assertEquals(expectedOrder, startOrder);

    lifecycle.stop();

    Assert.assertEquals(8, startOrder.size());
    Assert.assertEquals(8, stopOrder.size());
    Assert.assertEquals(Lists.reverse(expectedOrder), stopOrder);
  }

  @Test
  public void testAddToLifecycleInStartMethod() throws Exception
  {
    final Lifecycle lifecycle = new Lifecycle();

    final List<Integer> startOrder = Lists.newArrayList();
    final List<Integer> stopOrder = Lists.newArrayList();

    lifecycle.addManagedInstance(new ObjectToBeLifecycled(0, startOrder, stopOrder));
    lifecycle.addHandler(
        new Lifecycle.Handler()
        {
          @Override
          public void start() throws Exception
          {
            lifecycle.addMaybeStartManagedInstance(
                new ObjectToBeLifecycled(1, startOrder, stopOrder), Lifecycle.Stage.NORMAL
            );
            lifecycle.addMaybeStartManagedInstance(
                new ObjectToBeLifecycled(2, startOrder, stopOrder), Lifecycle.Stage.NORMAL
            );
            lifecycle.addMaybeStartManagedInstance(
                new ObjectToBeLifecycled(3, startOrder, stopOrder), Lifecycle.Stage.LAST
            );
            lifecycle.addMaybeStartStartCloseInstance(new ObjectToBeLifecycled(4, startOrder, stopOrder));
            lifecycle.addMaybeStartManagedInstance(new ObjectToBeLifecycled(5, startOrder, stopOrder));
            lifecycle.addMaybeStartStartCloseInstance(
                new ObjectToBeLifecycled(6, startOrder, stopOrder), Lifecycle.Stage.LAST
            );
            lifecycle.addMaybeStartManagedInstance(new ObjectToBeLifecycled(7, startOrder, stopOrder));
          }

          @Override
          public void stop()
          {

          }
        }
    );

    final List<Integer> expectedOrder = Arrays.asList(0, 1, 2, 4, 5, 7, 3, 6);

    lifecycle.start();

    Assert.assertEquals(expectedOrder, startOrder);
    Assert.assertEquals(0, stopOrder.size());

    lifecycle.stop();

    Assert.assertEquals(expectedOrder, startOrder);
    Assert.assertEquals(Lists.reverse(expectedOrder), stopOrder);
  }

  public static class ObjectToBeLifecycled
  {
    private final int id;
    private final List<Integer> orderOfStarts;
    private final List<Integer> orderOfStops;

    public ObjectToBeLifecycled(
        int id,
        List<Integer> orderOfStarts,
        List<Integer> orderOfStops
    )
    {
      this.id = id;
      this.orderOfStarts = orderOfStarts;
      this.orderOfStops = orderOfStops;
    }

    @LifecycleStart
    public void start()
    {
      orderOfStarts.add(id);
    }

    @LifecycleStop
    public void close()
    {
      orderOfStops.add(id);
    }
  }
}
