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

package io.druid.segment.data;

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.Clock;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.druid.java.util.common.StringUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;


// AbstractBenchmark makes this ignored unless explicitly run
@RunWith(Parameterized.class)
public class BenchmarkIndexibleWrites extends AbstractBenchmark
{
  @Parameterized.Parameters
  public static Collection<Object[]> getParameters()
  {
    return ImmutableList.<Object[]>of(
        new Object[]{new ConcurrentStandardMap<Integer>()},
        new Object[]{new ConcurrentExpandable<Integer>()}
    );
  }

  public BenchmarkIndexibleWrites(ConcurrentIndexible<Integer> concurrentIndexible)
  {
    this.concurrentIndexible = concurrentIndexible;
  }

  private static interface ConcurrentIndexible<V>
  {
    public void set(Integer index, V object);

    public V get(Integer index);

    public void clear();
  }

  private static class ConcurrentStandardMap<V> implements ConcurrentIndexible<V>
  {
    private final ConcurrentHashMap<Integer, V> delegate = new ConcurrentHashMap<>();

    @Override
    public void set(Integer index, V object)
    {
      delegate.put(index, object);
    }

    @Override
    public V get(Integer index)
    {
      return delegate.get(index);
    }

    @Override
    public void clear()
    {
      delegate.clear();
    }
  }

  private static class ConcurrentExpandable<V> implements ConcurrentIndexible<V>
  {
    private static Integer INIT_SIZE = 1 << 10;
    private final AtomicReference<V[]> reference = new AtomicReference<>();
    private final AtomicLong resizeCount = new AtomicLong(0);
    private final Integer initSize;

    public ConcurrentExpandable()
    {
      this(INIT_SIZE);
    }

    @SuppressWarnings("unchecked")
    public ConcurrentExpandable(Integer initSize)
    {
      reference.set((V[]) new Object[initSize]);
      this.initSize = initSize;
    }

    @Override
    public V get(Integer index)
    {
      return reference.get()[index];
    }

    @SuppressWarnings("unchecked")
    @Override
    public void clear()
    {
      reference.set((V[]) new Object[initSize]);
    }

    private static Boolean wasCopying(Long val)
    {
      return (val & 1L) > 0;
    }

    @Override
    public void set(Integer index, V object)
    {
      ensureCapacity(index + 1);
      Long pre, post;
      do {
        pre = resizeCount.get();
        reference.get()[index] = object;
        post = resizeCount.get();
      } while (wasCopying(pre) || wasCopying(post) || (!pre.equals(post)));
    }

    private final Object resizeMutex = new Object();

    private void ensureCapacity(int capacity)
    {
      synchronized (resizeMutex) {
        if (reference.get().length < capacity) {
          // We increment twice per resize. Once before the copy starts and once after the swap.
          //
          // Any task who sees a resizeCount which is *odd* between the start and stop of their critical section
          // has access to a nebulous aggList and should try again
          //
          // Any task who sees a resizeCount which changes between the start and stop of their critical section
          // should also try again
          resizeCount.incrementAndGet();
          reference.set(Arrays.copyOf(reference.get(), reference.get().length<<1));
          resizeCount.incrementAndGet();
        }
      }
    }
  }

  private final ConcurrentIndexible<Integer> concurrentIndexible;
  private final Integer concurrentThreads = 1<<2;
  private final Integer totalIndexSize = 1<<20;

  @BenchmarkOptions(warmupRounds = 100, benchmarkRounds = 100, clock = Clock.REAL_TIME, callgc = true)
  @Ignore @Test
  /**
   * CALLEN - 2015-01-15 - OSX - Java 1.7.0_71-b14
   BenchmarkIndexibleWrites.testConcurrentWrites[0]: [measured 100 out of 200 rounds, threads: 1 (sequential)]
   round: 0.24 [+- 0.01], round.block: 0.00 [+- 0.00], round.gc: 0.02 [+- 0.00], GC.calls: 396, GC.time: 1.88, time.total: 50.60, time.warmup: 24.84, time.bench: 25.77
   BenchmarkIndexibleWrites.testConcurrentWrites[1]: [measured 100 out of 200 rounds, threads: 1 (sequential)]
   round: 0.15 [+- 0.01], round.block: 0.00 [+- 0.00], round.gc: 0.02 [+- 0.00], GC.calls: 396, GC.time: 2.11, time.total: 33.14, time.warmup: 16.09, time.bench: 17.05
   */
  public void testConcurrentWrites() throws ExecutionException, InterruptedException
  {
    final ListeningExecutorService executorService = MoreExecutors.listeningDecorator(
        Executors.newFixedThreadPool(
            concurrentThreads,
            new ThreadFactoryBuilder()
                .setDaemon(false)
                .setNameFormat("indexible-writes-benchmark-%d")
                .build()
        )
    );
    final AtomicInteger index = new AtomicInteger(0);
    List<ListenableFuture<?>> futures = new LinkedList<>();

    final Integer loops = totalIndexSize / concurrentThreads;

    for (int i = 0; i < concurrentThreads; ++i) {
      futures.add(
          executorService.submit(
              new Runnable()
              {
                @Override
                public void run()
                {
                  for (int i = 0; i < loops; ++i) {
                    final Integer idx = index.getAndIncrement();
                    concurrentIndexible.set(idx, idx);
                  }
                }
              }
          )
      );
    }
    Futures.allAsList(futures).get();
    Assert.assertTrue(StringUtils.format("Index too small %d, expected %d across %d loops", index.get(), totalIndexSize, loops), index.get() >= totalIndexSize);
    for(int i = 0; i < index.get(); ++i){
      Assert.assertEquals(i, concurrentIndexible.get(i).intValue());
    }
    concurrentIndexible.clear();
    futures.clear();
    executorService.shutdown();
  }

  /**
   BenchmarkIndexibleWrites.TestConcurrentReads[0]: [measured 100 out of 200 rounds, threads: 1 (sequential)]
   round: 0.28 [+- 0.02], round.block: 0.00 [+- 0.00], round.gc: 0.02 [+- 0.00], GC.calls: 396, GC.time: 1.84, time.total: 59.98, time.warmup: 30.51, time.bench: 29.48
   BenchmarkIndexibleWrites.TestConcurrentReads[1]: [measured 100 out of 200 rounds, threads: 1 (sequential)]
   round: 0.12 [+- 0.01], round.block: 0.00 [+- 0.00], round.gc: 0.02 [+- 0.00], GC.calls: 396, GC.time: 2.05, time.total: 29.21, time.warmup: 14.65, time.bench: 14.55

   */
  @BenchmarkOptions(warmupRounds = 100, benchmarkRounds = 100, clock = Clock.REAL_TIME, callgc = true)
  @Ignore @Test
  public void testConcurrentReads() throws ExecutionException, InterruptedException
  {
    final ListeningExecutorService executorService = MoreExecutors.listeningDecorator(
        Executors.newFixedThreadPool(
            concurrentThreads,
            new ThreadFactoryBuilder()
                .setDaemon(false)
                .setNameFormat("indexible-writes-benchmark-reader-%d")
                .build()
        )
    );
    final AtomicInteger index = new AtomicInteger(0);
    final AtomicInteger queryableIndex = new AtomicInteger(0);
    List<ListenableFuture<?>> futures = new LinkedList<>();

    final Integer loops = totalIndexSize / concurrentThreads;

    final AtomicBoolean done = new AtomicBoolean(false);

    final CountDownLatch start = new CountDownLatch(1);

    for (int i = 0; i < concurrentThreads; ++i) {
      futures.add(
          executorService.submit(
              new Runnable()
              {
                @Override
                public void run()
                {
                  try {
                    start.await();
                  }
                  catch (InterruptedException e) {
                    throw Throwables.propagate(e);
                  }
                  final Random rndGen = new Random();
                  while(!done.get()){
                    Integer idx = rndGen.nextInt(queryableIndex.get() + 1);
                    Assert.assertEquals(idx, concurrentIndexible.get(idx));
                  }
                }
              }
          )
      );
    }

    {
      final Integer idx = index.getAndIncrement();
      concurrentIndexible.set(idx, idx);
      start.countDown();
    }
    for (int i = 1; i < totalIndexSize; ++i) {
      final Integer idx = index.getAndIncrement();
      concurrentIndexible.set(idx, idx);
      queryableIndex.incrementAndGet();
    }
    done.set(true);

    Futures.allAsList(futures).get();
    executorService.shutdown();

    Assert.assertTrue(StringUtils.format("Index too small %d, expected %d across %d loops", index.get(), totalIndexSize, loops), index.get() >= totalIndexSize);
    for(int i = 0; i < index.get(); ++i){
      Assert.assertEquals(i, concurrentIndexible.get(i).intValue());
    }
    concurrentIndexible.clear();
    futures.clear();
  }
}
