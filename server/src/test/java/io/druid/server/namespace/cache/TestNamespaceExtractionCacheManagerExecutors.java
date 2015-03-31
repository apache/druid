/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.server.namespace.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableScheduledFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.metamx.common.lifecycle.Lifecycle;
import io.druid.jackson.DefaultObjectMapper;
import net.spy.memcached.internal.ListenableFuture;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class TestNamespaceExtractionCacheManagerExecutors
{
  @Test
  public void testSimpleSubmission() throws ExecutionException, InterruptedException
  {
    final Lifecycle lifecycle = new Lifecycle();
    final AtomicBoolean ran = new AtomicBoolean(false);
    try {
      NamespaceExtractionCacheManager onHeap = new OnHeapNamespaceExtractionCacheManager(lifecycle);
      onHeap.scheduleOnce(
          new Runnable()
          {
            @Override
            public void run()
            {
              ran.set(true);
            }
          }
      ).get();
    }
    finally {
      lifecycle.stop();
    }
    Assert.assertTrue(ran.get());
  }

  @Test
  public void testRepeatSubmission() throws ExecutionException, InterruptedException
  {
    final Lifecycle lifecycle = new Lifecycle();
    final int repeatCount = 5;
    final int delay = 5;
    final CountDownLatch latch = new CountDownLatch(repeatCount);
    final AtomicLong ranCount = new AtomicLong(0l);
    final long totalRunCount;
    final long start;
    try {
      NamespaceExtractionCacheManager onHeap = new OnHeapNamespaceExtractionCacheManager(lifecycle);
      start = System.currentTimeMillis();
      onHeap.scheduleRepeat(
          "testNamespace",
          new Runnable()
          {
            @Override
            public void run()
            {
              latch.countDown();
              ranCount.incrementAndGet();
            }
          }, delay, TimeUnit.MILLISECONDS
      );
      latch.await();
      long minEnd = start + ((repeatCount - 1) * delay);
      long end = System.currentTimeMillis();
      Assert.assertTrue(String.format("Didn't wait long enough between runs. Expected more than %d was %d", minEnd - start, end - start), minEnd < end);
    }
    finally {
      lifecycle.stop();
    }
    totalRunCount = ranCount.get();
    Thread.sleep(50);
    Assert.assertEquals(totalRunCount, ranCount.get(), 1);
  }


  @Test(timeout = 500)
  public void testDelete()
      throws NoSuchFieldException, IllegalAccessException, InterruptedException, ExecutionException
  {
    final CountDownLatch latch = new CountDownLatch(1);
    final Lifecycle lifecycle = new Lifecycle();
    final NamespaceExtractionCacheManager onHeap;
    final ListenableScheduledFuture future;
    final AtomicLong runs = new AtomicLong(0);
    long prior = 0;
    try {
      onHeap = new OnHeapNamespaceExtractionCacheManager(lifecycle);
      future = onHeap.scheduleRepeat(
          "testNamespace",
          new Runnable()
          {
            @Override
            public void run()
            {
              runs.incrementAndGet();
              latch.countDown();
            }
          }, 1, TimeUnit.MILLISECONDS
      );

      latch.await();
      Assert.assertFalse(future.isCancelled());
      Assert.assertFalse(future.isDone());
      prior = runs.get();
      Thread.sleep(10);
      Assert.assertTrue(runs.get() > prior);

      onHeap.delete("testNamespace");

      prior = runs.get();
      Thread.sleep(10);
      Assert.assertEquals(prior, runs.get());
    }
    finally {
      lifecycle.stop();
    }
  }

  @Test(timeout = 500)
  public void testShutdown()
      throws NoSuchFieldException, IllegalAccessException, InterruptedException, ExecutionException
  {
    final CountDownLatch latch = new CountDownLatch(1);
    final Lifecycle lifecycle = new Lifecycle();
    final NamespaceExtractionCacheManager onHeap;
    final ListenableScheduledFuture future;
    final AtomicLong runs = new AtomicLong(0);
    long prior = 0;
    try {
      onHeap = new OnHeapNamespaceExtractionCacheManager(lifecycle);
      future = onHeap.scheduleRepeat(
          "testNamespace",
          new Runnable()
          {
            @Override
            public void run()
            {
              runs.incrementAndGet();
              latch.countDown();
            }
          }, 1, TimeUnit.MILLISECONDS
      );

      latch.await();
      Assert.assertFalse(future.isCancelled());
      Assert.assertFalse(future.isDone());
      prior = runs.get();
      Thread.sleep(10);
      Assert.assertTrue(runs.get() > prior);
    }
    finally {
      lifecycle.stop();
    }

    prior = runs.get();
    Thread.sleep(10);
    Assert.assertEquals(prior, runs.get());

    Field execField = NamespaceExtractionCacheManager.class.getDeclaredField("listeningScheduledExecutorService");
    execField.setAccessible(true);
    Assert.assertTrue(((ListeningScheduledExecutorService) execField.get(onHeap)).isShutdown());
    Assert.assertTrue(((ListeningScheduledExecutorService) execField.get(onHeap)).isTerminated());
  }

  @Test(timeout = 500)
  public void testRunCount()
      throws InterruptedException, ExecutionException
  {
    final Lifecycle lifecycle = new Lifecycle();
    final NamespaceExtractionCacheManager onHeap;
    final AtomicLong runCount = new AtomicLong(0);
    final CountDownLatch latch = new CountDownLatch(1);
    try {
      onHeap = new OnHeapNamespaceExtractionCacheManager(lifecycle);
      onHeap.scheduleRepeat(
          new Runnable()
          {
            @Override
            public void run()
            {
              latch.countDown();
              runCount.incrementAndGet();
            }
          }, 1, TimeUnit.MILLISECONDS
      );
      latch.countDown();
      Thread.sleep(20);
    }
    finally {
      lifecycle.stop();
    }
    Assert.assertTrue(runCount.get() > 5);
  }
}
