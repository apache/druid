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
import com.google.common.collect.ImmutableMap;
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

  private CloseableDefaultBlockingPool<Integer> nonGroupingPool;
  private CloseableDefaultBlockingPool<Integer> emptyPool;
  private CloseableDefaultBlockingPool<Integer> groupingPool;
  private final String firstResourceGroup = "group1";

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setup()
  {
    service = Execs.multiThreaded(2, "blocking-pool-test");
    nonGroupingPool = new CloseableDefaultBlockingPool<>(Suppliers.ofInstance(1), 10);
    emptyPool = new CloseableDefaultBlockingPool<>(Suppliers.ofInstance(1), 0);
    groupingPool = new CloseableDefaultBlockingPool<>(
        Suppliers.ofInstance(1),
        ImmutableMap.of(firstResourceGroup, 5),
        10
    );
  }

  @After
  public void teardown()
  {
    nonGroupingPool.close();
    emptyPool.close();
    groupingPool.close();
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
    take(nonGroupingPool, null);
    take(groupingPool, null);
    take(groupingPool, firstResourceGroup);
  }

  private void take(
      CloseableDefaultBlockingPool<Integer> pool,
      String group
  )
  {
    final ReferenceCountingResourceHolder<Integer> holder = Iterables.getOnlyElement(
        pool.takeBatch(group, 1, 100),
        null
    );
    Assert.assertNotNull(holder);
    Assert.assertEquals(9, pool.getPoolSize());
    holder.close();
    Assert.assertEquals(10, pool.getPoolSize());
  }

  @Test(timeout = 60_000L)
  public void testTakeTimeout()
  {
    takeTimeout(nonGroupingPool, null, 10);
    takeTimeout(groupingPool, null, 10);
    takeTimeout(groupingPool, firstResourceGroup, 5);
  }

  private void takeTimeout(
      CloseableDefaultBlockingPool<Integer> pool,
      String group,
      int groupCapacity
  )
  {
    final List<ReferenceCountingResourceHolder<Integer>> batchHolder = pool.takeBatch(group, groupCapacity, 100L);
    final ReferenceCountingResourceHolder<Integer> holder = Iterables.getOnlyElement(
        pool.takeBatch(group, 1, 100),
        null
    );
    Assert.assertNull(holder);
    batchHolder.forEach(ReferenceCountingResourceHolder::close);
  }

  @Test(timeout = 60_000L)
  public void testTakeBatch()
  {
    takeBatch(nonGroupingPool, null, 6);
    takeBatch(groupingPool, null, 6);
    takeBatch(groupingPool, firstResourceGroup, 2);
  }

  private void takeBatch(
      CloseableDefaultBlockingPool<Integer> pool,
      String group,
      int batch1
  )
  {
    final List<ReferenceCountingResourceHolder<Integer>> holder = pool.takeBatch(group, batch1, 100L);
    Assert.assertNotNull(holder);
    Assert.assertEquals(batch1, holder.size());
    Assert.assertEquals(pool.maxSize() - batch1, pool.getPoolSize());
    holder.forEach(ReferenceCountingResourceHolder::close);
    Assert.assertEquals(pool.maxSize(), pool.getPoolSize());
  }

  @Test(timeout = 60_000L)
  public void testWaitAndTakeBatch() throws InterruptedException, ExecutionException
  {
    waitAndTakeBatch(nonGroupingPool, null, 10, 8);
    waitAndTakeBatch(groupingPool, null, 10, 8);
    waitAndTakeBatch(groupingPool, firstResourceGroup, 5, 3);
  }

  private void waitAndTakeBatch(
      CloseableDefaultBlockingPool<Integer> pool,
      String group,
      int groupCapacity,
      int batchSize
  )
      throws InterruptedException, ExecutionException
  {
    List<ReferenceCountingResourceHolder<Integer>> batchHolder = pool.takeBatch(group, groupCapacity, 10);
    Assert.assertNotNull(batchHolder);
    Assert.assertEquals(groupCapacity, batchHolder.size());
    Assert.assertEquals(pool.maxSize() - groupCapacity, pool.getPoolSize());

    final Future<List<ReferenceCountingResourceHolder<Integer>>> future = service.submit(
        () -> pool.takeBatch(group, batchSize, 100)
    );
    Thread.sleep(20);
    batchHolder.forEach(ReferenceCountingResourceHolder::close);

    batchHolder = future.get();
    Assert.assertNotNull(batchHolder);
    Assert.assertEquals(batchSize, batchHolder.size());
    Assert.assertEquals(pool.maxSize() - batchSize, pool.getPoolSize());

    batchHolder.forEach(ReferenceCountingResourceHolder::close);
    Assert.assertEquals(pool.maxSize(), pool.getPoolSize());
  }

  @Test(timeout = 60_000L)
  public void testTakeBatchTooManyObjects()
  {
    takeBatchTooManyObjects(nonGroupingPool, null);
    takeBatchTooManyObjects(groupingPool, null);
    takeBatchTooManyObjects(groupingPool, firstResourceGroup);
  }

  private void takeBatchTooManyObjects(CloseableDefaultBlockingPool<Integer> pool, String group)
  {
    final List<ReferenceCountingResourceHolder<Integer>> holder = pool.takeBatch(group, 100, 100L);
    Assert.assertTrue(holder.isEmpty());
  }

  @Test(timeout = 60_000L)
  public void testConcurrentTake() throws ExecutionException, InterruptedException
  {
    concurrentTake(nonGroupingPool, null, 10, 5, 6);
    concurrentTake(groupingPool, null, 10, 5, 6);
    concurrentTake(groupingPool, firstResourceGroup, 5, 2, 4);
  }

  private void concurrentTake(
      CloseableDefaultBlockingPool<Integer> pool,
      String group,
      int groupCapacity,
      int limit1,
      int limit2
  )
      throws ExecutionException, InterruptedException
  {
    final Future<List<ReferenceCountingResourceHolder<Integer>>> f1 = service.submit(
        () -> {
          List<ReferenceCountingResourceHolder<Integer>> result = new ArrayList<>();
          for (int i = 0; i < limit1; i++) {
            result.add(Iterables.getOnlyElement(pool.takeBatch(group, 1, 10), null));
          }
          return result;
        }
    );
    final Future<List<ReferenceCountingResourceHolder<Integer>>> f2 = service.submit(
        () -> {
          List<ReferenceCountingResourceHolder<Integer>> result = new ArrayList<>();
          for (int i = 0; i < limit2; i++) {
            result.add(Iterables.getOnlyElement(pool.takeBatch(group, 1, 10), null));
          }
          return result;
        }
    );

    final List<ReferenceCountingResourceHolder<Integer>> r1 = f1.get();
    final List<ReferenceCountingResourceHolder<Integer>> r2 = f2.get();

    Assert.assertEquals(pool.getPoolSize(), pool.maxSize() - groupCapacity);
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
    Assert.assertEquals(groupCapacity, nonNullCount);

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
    concurrentTakeBatch(nonGroupingPool, null, 5, 6);
    concurrentTakeBatch(groupingPool, null, 5, 6);
    concurrentTakeBatch(groupingPool, firstResourceGroup, 2, 4);
  }

  private void concurrentTakeBatch(CloseableDefaultBlockingPool<Integer> pool, String group, int batch1, int batch2)
      throws ExecutionException, InterruptedException
  {
    final Callable<List<ReferenceCountingResourceHolder<Integer>>> c1 = () -> pool.takeBatch(group, batch1, 10);
    final Callable<List<ReferenceCountingResourceHolder<Integer>>> c2 = () -> pool.takeBatch(group, batch2, 10);

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
    concurrentBatchClose(nonGroupingPool, null, 10, 5);
    concurrentBatchClose(groupingPool, null, 10, 5);
    concurrentBatchClose(groupingPool, firstResourceGroup, 5, 2);
  }

  private void concurrentBatchClose(
      CloseableDefaultBlockingPool<Integer> pool,
      String group,
      int groupCapacity,
      int batchSize
  )
      throws ExecutionException, InterruptedException
  {
    final Callable<List<ReferenceCountingResourceHolder<Integer>>> c1 = () -> pool.takeBatch(group, batchSize, 10);
    final Callable<List<ReferenceCountingResourceHolder<Integer>>> c2 = () -> pool.takeBatch(
        group,
        groupCapacity - batchSize,
        10
    );

    final Future<List<ReferenceCountingResourceHolder<Integer>>> f1 = service.submit(c1);
    final Future<List<ReferenceCountingResourceHolder<Integer>>> f2 = service.submit(c2);

    final List<ReferenceCountingResourceHolder<Integer>> r1 = f1.get();
    final List<ReferenceCountingResourceHolder<Integer>> r2 = f2.get();

    Assert.assertNotNull(r1);
    Assert.assertNotNull(r2);
    Assert.assertEquals(batchSize, r1.size());
    Assert.assertEquals(groupCapacity - batchSize, r2.size());
    Assert.assertEquals(pool.maxSize() - groupCapacity, pool.getPoolSize());

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
    concurrentTakeBatchClose(nonGroupingPool, null, null, 10);
    concurrentTakeBatchClose(groupingPool, null, null, 10);
    concurrentTakeBatchClose(groupingPool, firstResourceGroup, firstResourceGroup, 5);
    concurrentTakeBatchClose(groupingPool, firstResourceGroup, null, 10);
  }

  private void concurrentTakeBatchClose(
      CloseableDefaultBlockingPool<Integer> pool,
      String group1,
      String group2,
      int group2Capcity
  )
      throws ExecutionException, InterruptedException
  {
    final List<ReferenceCountingResourceHolder<Integer>> r1 = pool.takeBatch(group1, 1, 10);
    final Callable<List<ReferenceCountingResourceHolder<Integer>>> c2 = () -> pool.takeBatch(
        group2,
        group2Capcity,
        100
    );

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
    Assert.assertEquals(group2Capcity, r2.size());
    Assert.assertEquals(pool.maxSize() - group2Capcity, pool.getPoolSize());

    r2.forEach(ReferenceCountingResourceHolder::close);
    Assert.assertEquals(pool.maxSize(), pool.getPoolSize());
  }
}
