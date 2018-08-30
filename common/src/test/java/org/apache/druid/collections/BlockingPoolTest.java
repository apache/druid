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
import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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
    emptyPool.take(0);
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
    final ReferenceCountingResourceHolder<Integer> holder = pool.take(100);
    assertNotNull(holder);
    assertEquals(9, pool.getPoolSize());
    holder.close();
    assertEquals(10, pool.getPoolSize());
  }

  @Test(timeout = 60_000L)
  public void testTakeTimeout()
  {
    final List<ReferenceCountingResourceHolder<Integer>> batchHolder = pool.takeBatch(10, 100L);
    final ReferenceCountingResourceHolder<Integer> holder = pool.take(100);
    assertNull(holder);
    batchHolder.forEach(ReferenceCountingResourceHolder::close);
  }

  @Test(timeout = 60_000L)
  public void testTakeBatch()
  {
    final List<ReferenceCountingResourceHolder<Integer>> holder = pool.takeBatch(6, 100L);
    assertNotNull(holder);
    assertEquals(6, holder.size());
    assertEquals(4, pool.getPoolSize());
    holder.forEach(ReferenceCountingResourceHolder::close);
    assertEquals(10, pool.getPoolSize());
  }

  @Test(timeout = 60_000L)
  public void testWaitAndTakeBatch() throws InterruptedException, ExecutionException
  {
    List<ReferenceCountingResourceHolder<Integer>> batchHolder = pool.takeBatch(10, 10);
    assertNotNull(batchHolder);
    assertEquals(10, batchHolder.size());
    assertEquals(0, pool.getPoolSize());

    final Future<List<ReferenceCountingResourceHolder<Integer>>> future = service.submit(
        () -> pool.takeBatch(8, 100)
    );
    Thread.sleep(20);
    batchHolder.forEach(ReferenceCountingResourceHolder::close);

    batchHolder = future.get();
    assertNotNull(batchHolder);
    assertEquals(8, batchHolder.size());
    assertEquals(2, pool.getPoolSize());

    batchHolder.forEach(ReferenceCountingResourceHolder::close);
    assertEquals(10, pool.getPoolSize());
  }

  @Test(timeout = 60_000L)
  public void testTakeBatchTooManyObjects()
  {
    final List<ReferenceCountingResourceHolder<Integer>> holder = pool.takeBatch(100, 100L);
    assertTrue(holder.isEmpty());
  }

  @Test(timeout = 60_000L)
  public void testConcurrentTake() throws ExecutionException, InterruptedException
  {
    final int limit1 = pool.maxSize() / 2;
    final int limit2 = pool.maxSize() - limit1 + 1;

    final Future<List<ReferenceCountingResourceHolder<Integer>>> f1 = service.submit(
        new Callable<List<ReferenceCountingResourceHolder<Integer>>>()
        {
          @Override
          public List<ReferenceCountingResourceHolder<Integer>> call()
          {
            List<ReferenceCountingResourceHolder<Integer>> result = Lists.newArrayList();
            for (int i = 0; i < limit1; i++) {
              result.add(pool.take(10));
            }
            return result;
          }
        }
    );
    final Future<List<ReferenceCountingResourceHolder<Integer>>> f2 = service.submit(
        new Callable<List<ReferenceCountingResourceHolder<Integer>>>()
        {
          @Override
          public List<ReferenceCountingResourceHolder<Integer>> call()
          {
            List<ReferenceCountingResourceHolder<Integer>> result = Lists.newArrayList();
            for (int i = 0; i < limit2; i++) {
              result.add(pool.take(10));
            }
            return result;
          }
        }
    );

    final List<ReferenceCountingResourceHolder<Integer>> r1 = f1.get();
    final List<ReferenceCountingResourceHolder<Integer>> r2 = f2.get();

    assertEquals(0, pool.getPoolSize());
    assertTrue(r1.contains(null) || r2.contains(null));

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
    assertEquals(pool.maxSize(), nonNullCount);

    final Future future1 = service.submit(new Runnable()
    {
      @Override
      public void run()
      {
        for (ReferenceCountingResourceHolder<Integer> holder : r1) {
          if (holder != null) {
            holder.close();
          }
        }
      }
    });
    final Future future2 = service.submit(new Runnable()
    {
      @Override
      public void run()
      {
        for (ReferenceCountingResourceHolder<Integer> holder : r2) {
          if (holder != null) {
            holder.close();
          }
        }
      }
    });

    future1.get();
    future2.get();

    assertEquals(pool.maxSize(), pool.getPoolSize());
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

    if (r1 != null) {
      assertTrue(r2.isEmpty());
      assertEquals(pool.maxSize() - batch1, pool.getPoolSize());
      assertEquals(batch1, r1.size());
      r1.forEach(ReferenceCountingResourceHolder::close);
    } else {
      assertNotNull(r2);
      assertEquals(pool.maxSize() - batch2, pool.getPoolSize());
      assertEquals(batch2, r2.size());
      r2.forEach(ReferenceCountingResourceHolder::close);
    }

    assertEquals(pool.maxSize(), pool.getPoolSize());
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

    assertNotNull(r1);
    assertNotNull(r2);
    assertEquals(batch1, r1.size());
    assertEquals(batch2, r2.size());
    assertEquals(0, pool.getPoolSize());

    final Future future1 = service.submit(new Runnable()
    {
      @Override
      public void run()
      {
        r1.forEach(ReferenceCountingResourceHolder::close);
      }
    });
    final Future future2 = service.submit(new Runnable()
    {
      @Override
      public void run()
      {
        r2.forEach(ReferenceCountingResourceHolder::close);
      }
    });

    future1.get();
    future2.get();

    assertEquals(pool.maxSize(), pool.getPoolSize());
  }

  @Test(timeout = 60_000L)
  public void testConcurrentTakeBatchClose() throws ExecutionException, InterruptedException
  {
    final List<ReferenceCountingResourceHolder<Integer>> r1 = pool.takeBatch(1, 10);

    final Callable<List<ReferenceCountingResourceHolder<Integer>>> c2 = () -> pool.takeBatch(10, 100);

    final Future<List<ReferenceCountingResourceHolder<Integer>>> f2 = service.submit(c2);
    final Future f1 = service.submit(new Runnable()
    {
      @Override
      public void run()
      {
        try {
          Thread.sleep(50);
        }
        catch (InterruptedException e) {
          // ignore
        }
        r1.forEach(ReferenceCountingResourceHolder::close);
      }
    });

    final List<ReferenceCountingResourceHolder<Integer>> r2 = f2.get();
    f1.get();
    assertNotNull(r2);
    assertEquals(10, r2.size());
    assertEquals(0, pool.getPoolSize());

    r2.forEach(ReferenceCountingResourceHolder::close);
    assertEquals(pool.maxSize(), pool.getPoolSize());
  }
}
