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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setup()
  {
    service = Execs.multiThreaded(2, "blocking-pool-test");
    pool = new CloseableDefaultBlockingPool<>(Suppliers.ofInstance(1), 10);
    emptyPool = new CloseableDefaultBlockingPool<>(Suppliers.ofInstance(1), 0);
  }

  @After
  public void teardown()
  {
    pool.close();
    emptyPool.close();
    service.shutdownNow();
  }

  @Test
  public void testTakeFromEmptyPool()
  {
    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("Pool was initialized with limit = 0, there are no objects to take.");
    emptyPool.takeBatch(1, 0);
  }

  @Test
  public void testDrainFromEmptyPool()
  {
    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("Pool was initialized with limit = 0, there are no objects to take.");
    emptyPool.takeBatch(1, 0);
  }

  @Test(timeout = 60_000L)
  public void testTake()
  {
    final ReferenceCountingResourceHolder<Integer> holder = Iterables.getOnlyElement(pool.takeBatch(1, 100), null);
    Assert.assertNotNull(holder);
    Assert.assertEquals(9, pool.getPoolSize());
    holder.close();
    Assert.assertEquals(10, pool.getPoolSize());
  }

  @Test(timeout = 60_000L)
  public void testTakeTimeout()
  {
    final List<ReferenceCountingResourceHolder<Integer>> batchHolder = pool.takeBatch(10, 100L);
    final ReferenceCountingResourceHolder<Integer> holder = Iterables.getOnlyElement(pool.takeBatch(1, 100), null);
    Assert.assertNull(holder);
    batchHolder.forEach(ReferenceCountingResourceHolder::close);
  }

  @Test(timeout = 60_000L)
  public void testTakeBatch()
  {
    final List<ReferenceCountingResourceHolder<Integer>> holder = pool.takeBatch(6, 100L);
    Assert.assertNotNull(holder);
    Assert.assertEquals(6, holder.size());
    Assert.assertEquals(4, pool.getPoolSize());
    holder.forEach(ReferenceCountingResourceHolder::close);
    Assert.assertEquals(10, pool.getPoolSize());
  }

  @Test(timeout = 60_000L)
  public void testWaitAndTakeBatch() throws InterruptedException, ExecutionException
  {
    List<ReferenceCountingResourceHolder<Integer>> batchHolder = pool.takeBatch(10, 10);
    Assert.assertNotNull(batchHolder);
    Assert.assertEquals(10, batchHolder.size());
    Assert.assertEquals(0, pool.getPoolSize());

    final Future<List<ReferenceCountingResourceHolder<Integer>>> future = service.submit(
        () -> pool.takeBatch(8, 100)
    );
    Thread.sleep(20);
    batchHolder.forEach(ReferenceCountingResourceHolder::close);

    batchHolder = future.get();
    Assert.assertNotNull(batchHolder);
    Assert.assertEquals(8, batchHolder.size());
    Assert.assertEquals(2, pool.getPoolSize());

    batchHolder.forEach(ReferenceCountingResourceHolder::close);
    Assert.assertEquals(10, pool.getPoolSize());
  }

  @Test(timeout = 60_000L)
  public void testTakeBatchTooManyObjects()
  {
    final List<ReferenceCountingResourceHolder<Integer>> holder = pool.takeBatch(100, 100L);
    Assert.assertTrue(holder.isEmpty());
  }

  @Test(timeout = 60_000L)
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

    Assert.assertEquals(0, pool.getPoolSize());
    Assert.assertTrue(r1.contains(null) || r2.contains(null));

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
    Assert.assertEquals(pool.maxSize(), nonNullCount);

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

    Assert.assertEquals(pool.maxSize(), pool.getPoolSize());
  }

  @Test(timeout = 60_000L)
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
      Assert.assertTrue(r2.isEmpty());
      Assert.assertEquals(pool.maxSize() - batch1, pool.getPoolSize());
      Assert.assertEquals(batch1, r1.size());
      r1.forEach(ReferenceCountingResourceHolder::close);
    } else {
      Assert.assertNotNull(r2);
      Assert.assertEquals(pool.maxSize() - batch2, pool.getPoolSize());
      Assert.assertEquals(batch2, r2.size());
      r2.forEach(ReferenceCountingResourceHolder::close);
    }

    Assert.assertEquals(pool.maxSize(), pool.getPoolSize());
  }

  @Test(timeout = 60_000L)
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

    Assert.assertNotNull(r1);
    Assert.assertNotNull(r2);
    Assert.assertEquals(batch1, r1.size());
    Assert.assertEquals(batch2, r2.size());
    Assert.assertEquals(0, pool.getPoolSize());

    final Future future1 = service.submit(() -> r1.forEach(ReferenceCountingResourceHolder::close));
    final Future future2 = service.submit(() -> r2.forEach(ReferenceCountingResourceHolder::close));

    future1.get();
    future2.get();

    Assert.assertEquals(pool.maxSize(), pool.getPoolSize());
  }

  @SuppressWarnings("CatchMayIgnoreException")
  @Test(timeout = 60_000L)
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
    Assert.assertNotNull(r2);
    Assert.assertEquals(10, r2.size());
    Assert.assertEquals(0, pool.getPoolSize());

    r2.forEach(ReferenceCountingResourceHolder::close);
    Assert.assertEquals(pool.maxSize(), pool.getPoolSize());
  }
}
