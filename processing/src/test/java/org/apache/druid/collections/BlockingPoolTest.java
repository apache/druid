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

package org.apache.druid.collections;

import com.google.common.base.Suppliers;
import com.google.common.collect.Iterables;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class BlockingPoolTest
{
  private ExecutorService service;

  private CloseableDefaultBlockingPool<Integer> pool;
  private CloseableDefaultBlockingPool<Integer> emptyPool;

  @BeforeEach
  public void setup()
  {
    service = Execs.multiThreaded(2, "blocking-pool-test");
    pool = new CloseableDefaultBlockingPool<>(Suppliers.ofInstance(1), 10);
    emptyPool = new CloseableDefaultBlockingPool<>(Suppliers.ofInstance(1), 0);
  }

  @AfterEach
  public void teardown()
  {
    pool.close();
    emptyPool.close();
    service.shutdownNow();
  }

  @Test
  @Timeout(60)
  public void testParallelInit()
  {
    DefaultBlockingPool<Integer> parallelPool = new DefaultBlockingPool<>(Suppliers.ofInstance(1), 10, true);
    Assertions.assertEquals(10, parallelPool.getPoolSize());
    final ReferenceCountingResourceHolder<Integer> holder =
        Iterables.getOnlyElement(parallelPool.takeBatch(1, 100), null);
    Assertions.assertNotNull(holder);
    Assertions.assertEquals(9, parallelPool.getPoolSize());
    holder.close();
    Assertions.assertEquals(10, parallelPool.getPoolSize());
  }

  @Test
  public void testTakeFromEmptyPool()
  {
    final IllegalStateException e = Assertions.assertThrows(
        IllegalStateException.class,
        () -> emptyPool.takeBatch(1, 0)
    );
    Assertions.assertTrue(e.getMessage().contains("Pool was initialized with limit = 0, there are no objects to take."));
  }

  @Test
  public void testDrainFromEmptyPool()
  {
    final IllegalStateException e = Assertions.assertThrows(
        IllegalStateException.class,
        () -> emptyPool.takeBatch(1, 0)
    );
    Assertions.assertTrue(e.getMessage().contains("Pool was initialized with limit = 0, there are no objects to take."));
  }

  @Test
@Timeout(60)
  public void testTake()
  {
    final ReferenceCountingResourceHolder<Integer> holder = Iterables.getOnlyElement(pool.takeBatch(1, 100), null);
    Assertions.assertNotNull(holder);
    Assertions.assertEquals(9, pool.getPoolSize());
    holder.close();
    Assertions.assertEquals(10, pool.getPoolSize());
  }

  @Test
  @Timeout(60)
  public void testTakeTimeout()
  {
    final List<ReferenceCountingResourceHolder<Integer>> batchHolder = pool.takeBatch(10, 100L);
    final ReferenceCountingResourceHolder<Integer> holder = Iterables.getOnlyElement(pool.takeBatch(1, 100), null);
    Assertions.assertNull(holder);
    batchHolder.forEach(ReferenceCountingResourceHolder::close);
  }

  @Test
  @Timeout(60)
  public void testTakeBatch()
  {
    final List<ReferenceCountingResourceHolder<Integer>> holder = pool.takeBatch(6, 100L);
    Assertions.assertNotNull(holder);
    Assertions.assertEquals(6, holder.size());
    Assertions.assertEquals(4, pool.getPoolSize());
    holder.forEach(ReferenceCountingResourceHolder::close);
    Assertions.assertEquals(10, pool.getPoolSize());
  }

  @Test
  @Timeout(60)
  public void testWaitAndTakeBatch() throws InterruptedException, ExecutionException
  {
    List<ReferenceCountingResourceHolder<Integer>> batchHolder = pool.takeBatch(10, 10);
    Assertions.assertNotNull(batchHolder);
    Assertions.assertEquals(10, batchHolder.size());
    Assertions.assertEquals(0, pool.getPoolSize());

    final Future<List<ReferenceCountingResourceHolder<Integer>>> future = service.submit(
        () -> pool.takeBatch(8, 100)
    );
    Thread.sleep(20);
    batchHolder.forEach(ReferenceCountingResourceHolder::close);

    batchHolder = future.get();
    Assertions.assertNotNull(batchHolder);
    Assertions.assertEquals(8, batchHolder.size());
    Assertions.assertEquals(2, pool.getPoolSize());

    batchHolder.forEach(ReferenceCountingResourceHolder::close);
    Assertions.assertEquals(10, pool.getPoolSize());
  }

  @Test
  @Timeout(60)
  public void testTakeBatchTooManyObjects()
  {
    final List<ReferenceCountingResourceHolder<Integer>> holder = pool.takeBatch(100, 100L);
    Assertions.assertTrue(holder.isEmpty());
  }

  @Test
  @Timeout(60)
  public void testConcurrentTake() throws ExecutionException, InterruptedException
  {
    final int limit1 = pool.maxSize() / 2;
    final int limit2 = pool.maxSize() - limit1 + 1;

    final Future<List<ReferenceCountingResourceHolder<Integer>>> f1 = service.submit(
        () -> {
          List<ReferenceCountingResourceHolder<Integer>> result = new ArrayList<>();
          for (int i = 0; i < limit1; i++) {
            result.add(Iterables.getOnlyElement(pool.takeBatch(1, 10), null));
          }
          return result;
        }
    );
    final Future<List<ReferenceCountingResourceHolder<Integer>>> f2 = service.submit(
        () -> {
          List<ReferenceCountingResourceHolder<Integer>> result = new ArrayList<>();
          for (int i = 0; i < limit2; i++) {
            result.add(Iterables.getOnlyElement(pool.takeBatch(1, 10), null));
          }
          return result;
        }
    );

    final List<ReferenceCountingResourceHolder<Integer>> r1 = f1.get();
    final List<ReferenceCountingResourceHolder<Integer>> r2 = f2.get();

    Assertions.assertEquals(0, pool.getPoolSize());
    Assertions.assertTrue(r1.contains(null) || r2.contains(null));

    int nonNullCount = 0;
    for (ReferenceCountingResourceHolder<Integer> holder : r1) {
      if (holder != null) {
        nonNullCount++;
      }
    }

    for (ReferenceCountingResourceHolder<Integer> holder : r2) {
      if (holder != null) {
        nonNullCount++;
      }
    }
    Assertions.assertEquals(pool.maxSize(), nonNullCount);

    final Future future1 = service.submit(() -> {
      for (ReferenceCountingResourceHolder<Integer> holder : r1) {
        if (holder != null) {
          holder.close();
        }
      }
    });
    final Future future2 = service.submit(() -> {
      for (ReferenceCountingResourceHolder<Integer> holder : r2) {
        if (holder != null) {
          holder.close();
        }
      }
    });

    future1.get();
    future2.get();

    Assertions.assertEquals(pool.maxSize(), pool.getPoolSize());
  }

  @Test
  @Timeout(60)
  public void testConcurrentTakeBatch() throws ExecutionException, InterruptedException
  {
    final int batch1 = pool.maxSize() / 2;
    final Callable<List<ReferenceCountingResourceHolder<Integer>>> c1 = () -> pool.takeBatch(batch1, 10);

    final int batch2 = pool.maxSize() - batch1 + 1;
    final Callable<List<ReferenceCountingResourceHolder<Integer>>> c2 = () -> pool.takeBatch(batch2, 10);

    final Future<List<ReferenceCountingResourceHolder<Integer>>> f1 = service.submit(c1);
    final Future<List<ReferenceCountingResourceHolder<Integer>>> f2 = service.submit(c2);

    final List<ReferenceCountingResourceHolder<Integer>> r1 = f1.get();
    final List<ReferenceCountingResourceHolder<Integer>> r2 = f2.get();

    if (!r1.isEmpty()) {
      Assertions.assertTrue(r2.isEmpty());
      Assertions.assertEquals(pool.maxSize() - batch1, pool.getPoolSize());
      Assertions.assertEquals(batch1, r1.size());
      r1.forEach(ReferenceCountingResourceHolder::close);
    } else {
      Assertions.assertNotNull(r2);
      Assertions.assertEquals(pool.maxSize() - batch2, pool.getPoolSize());
      Assertions.assertEquals(batch2, r2.size());
      r2.forEach(ReferenceCountingResourceHolder::close);
    }

    Assertions.assertEquals(pool.maxSize(), pool.getPoolSize());
  }

  @Test
  @Timeout(60)
  public void testConcurrentBatchClose() throws ExecutionException, InterruptedException
  {
    final int batch1 = pool.maxSize() / 2;
    final Callable<List<ReferenceCountingResourceHolder<Integer>>> c1 = () -> pool.takeBatch(batch1, 10);

    final int batch2 = pool.maxSize() - batch1;
    final Callable<List<ReferenceCountingResourceHolder<Integer>>> c2 = () -> pool.takeBatch(batch2, 10);

    final Future<List<ReferenceCountingResourceHolder<Integer>>> f1 = service.submit(c1);
    final Future<List<ReferenceCountingResourceHolder<Integer>>> f2 = service.submit(c2);

    final List<ReferenceCountingResourceHolder<Integer>> r1 = f1.get();
    final List<ReferenceCountingResourceHolder<Integer>> r2 = f2.get();

    Assertions.assertNotNull(r1);
    Assertions.assertNotNull(r2);
    Assertions.assertEquals(batch1, r1.size());
    Assertions.assertEquals(batch2, r2.size());
    Assertions.assertEquals(0, pool.getPoolSize());

    final Future future1 = service.submit(() -> r1.forEach(ReferenceCountingResourceHolder::close));
    final Future future2 = service.submit(() -> r2.forEach(ReferenceCountingResourceHolder::close));

    future1.get();
    future2.get();

    Assertions.assertEquals(pool.maxSize(), pool.getPoolSize());
  }

  @SuppressWarnings("CatchMayIgnoreException")
  @Test
  @Timeout(60)
  public void testConcurrentTakeBatchClose() throws ExecutionException, InterruptedException
  {
    final List<ReferenceCountingResourceHolder<Integer>> r1 = pool.takeBatch(1, 10);

    final Callable<List<ReferenceCountingResourceHolder<Integer>>> c2 = () -> pool.takeBatch(10, 100);

    final Future<List<ReferenceCountingResourceHolder<Integer>>> f2 = service.submit(c2);
    final Future f1 = service.submit(() -> {
      try {
        Thread.sleep(50);
      }
      catch (InterruptedException e) {
        // ignore
      }
      r1.forEach(ReferenceCountingResourceHolder::close);
    });

    final List<ReferenceCountingResourceHolder<Integer>> r2 = f2.get();
    f1.get();
    Assertions.assertNotNull(r2);
    Assertions.assertEquals(10, r2.size());
    Assertions.assertEquals(0, pool.getPoolSize());

    r2.forEach(ReferenceCountingResourceHolder::close);
    Assertions.assertEquals(pool.maxSize(), pool.getPoolSize());
  }
}
