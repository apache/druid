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

package io.druid.collections;

import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;
import io.druid.java.util.common.concurrent.Execs;
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

  private DefaultBlockingPool<Integer> pool;
  private BlockingPool<Integer> emptyPool;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setup()
  {
    service = Execs.multiThreaded(2, "blocking-pool-test");
    pool = new DefaultBlockingPool<>(Suppliers.ofInstance(1), 10);
    emptyPool = new DefaultBlockingPool<>(Suppliers.ofInstance(1), 0);
  }

  @After
  public void teardown()
  {
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

  @Test(timeout = 5000)
  public void testTake()
  {
    final ReferenceCountingResourceHolder<Integer> holder = pool.take(100);
    assertNotNull(holder);
    assertEquals(9, pool.getPoolSize());
    holder.close();
    assertEquals(10, pool.getPoolSize());
  }

  @Test(timeout = 5000)
  public void testTakeTimeout()
  {
    final ReferenceCountingResourceHolder<List<Integer>> batchHolder = pool.takeBatch(10, 100L);
    final ReferenceCountingResourceHolder<Integer> holder = pool.take(100);
    assertNull(holder);
    batchHolder.close();
  }

  @Test(timeout = 5000)
  public void testTakeBatch()
  {
    final ReferenceCountingResourceHolder<List<Integer>> holder = pool.takeBatch(6, 100L);
    assertNotNull(holder);
    assertEquals(6, holder.get().size());
    assertEquals(4, pool.getPoolSize());
    holder.close();
    assertEquals(10, pool.getPoolSize());
  }

  @Test(timeout = 5000)
  public void testWaitAndTakeBatch() throws InterruptedException, ExecutionException
  {
    ReferenceCountingResourceHolder<List<Integer>> batchHolder = pool.takeBatch(10, 10);
    assertNotNull(batchHolder);
    assertEquals(10, batchHolder.get().size());
    assertEquals(0, pool.getPoolSize());

    final Future<ReferenceCountingResourceHolder<List<Integer>>> future = service.submit(
        new Callable<ReferenceCountingResourceHolder<List<Integer>>>()
        {
          @Override
          public ReferenceCountingResourceHolder<List<Integer>> call() throws Exception
          {
            return pool.takeBatch(8, 100);
          }
        }
    );
    Thread.sleep(20);
    batchHolder.close();

    batchHolder = future.get();
    assertNotNull(batchHolder);
    assertEquals(8, batchHolder.get().size());
    assertEquals(2, pool.getPoolSize());

    batchHolder.close();
    assertEquals(10, pool.getPoolSize());
  }

  @Test(timeout = 5000)
  public void testTakeBatchTooManyObjects()
  {
    final ReferenceCountingResourceHolder<List<Integer>> holder = pool.takeBatch(100, 100L);
    assertNull(holder);
  }

  @Test(timeout = 5000)
  public void testConcurrentTake() throws ExecutionException, InterruptedException
  {
    final int limit1 = pool.maxSize() / 2;
    final int limit2 = pool.maxSize() - limit1 + 1;

    final Future<List<ReferenceCountingResourceHolder<Integer>>> f1 = service.submit(
        new Callable<List<ReferenceCountingResourceHolder<Integer>>>()
        {
          @Override
          public List<ReferenceCountingResourceHolder<Integer>> call() throws Exception
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
          public List<ReferenceCountingResourceHolder<Integer>> call() throws Exception
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

  @Test(timeout = 5000)
  public void testConcurrentTakeBatch() throws ExecutionException, InterruptedException
  {
    final int batch1 = pool.maxSize() / 2;
    final Callable<ReferenceCountingResourceHolder<List<Integer>>> c1 =
        new Callable<ReferenceCountingResourceHolder<List<Integer>>>()
        {
          @Override
          public ReferenceCountingResourceHolder<List<Integer>> call() throws Exception
          {
            return pool.takeBatch(batch1, 10);
          }
        };

    final int batch2 = pool.maxSize() - batch1 + 1;
    final Callable<ReferenceCountingResourceHolder<List<Integer>>> c2 =
        new Callable<ReferenceCountingResourceHolder<List<Integer>>>()
        {
          @Override
          public ReferenceCountingResourceHolder<List<Integer>> call() throws Exception
          {
            return pool.takeBatch(batch2, 10);
          }
        };

    final Future<ReferenceCountingResourceHolder<List<Integer>>> f1 = service.submit(c1);
    final Future<ReferenceCountingResourceHolder<List<Integer>>> f2 = service.submit(c2);

    final ReferenceCountingResourceHolder<List<Integer>> r1 = f1.get();
    final ReferenceCountingResourceHolder<List<Integer>> r2 = f2.get();

    if (r1 != null) {
      assertNull(r2);
      assertEquals(pool.maxSize() - batch1, pool.getPoolSize());
      assertEquals(batch1, r1.get().size());
      r1.close();
    } else {
      assertNotNull(r2);
      assertEquals(pool.maxSize() - batch2, pool.getPoolSize());
      assertEquals(batch2, r2.get().size());
      r2.close();
    }

    assertEquals(pool.maxSize(), pool.getPoolSize());
  }

  @Test(timeout = 5000)
  public void testConcurrentBatchClose() throws ExecutionException, InterruptedException
  {
    final int batch1 = pool.maxSize() / 2;
    final Callable<ReferenceCountingResourceHolder<List<Integer>>> c1 =
        new Callable<ReferenceCountingResourceHolder<List<Integer>>>()
        {
          @Override
          public ReferenceCountingResourceHolder<List<Integer>> call() throws Exception
          {
            return pool.takeBatch(batch1, 10);
          }
        };

    final int batch2 = pool.maxSize() - batch1;
    final Callable<ReferenceCountingResourceHolder<List<Integer>>> c2 =
        new Callable<ReferenceCountingResourceHolder<List<Integer>>>()
        {
          @Override
          public ReferenceCountingResourceHolder<List<Integer>> call() throws Exception
          {
            return pool.takeBatch(batch2, 10);
          }
        };

    final Future<ReferenceCountingResourceHolder<List<Integer>>> f1 = service.submit(c1);
    final Future<ReferenceCountingResourceHolder<List<Integer>>> f2 = service.submit(c2);

    final ReferenceCountingResourceHolder<List<Integer>> r1 = f1.get();
    final ReferenceCountingResourceHolder<List<Integer>> r2 = f2.get();

    assertNotNull(r1);
    assertNotNull(r2);
    assertEquals(batch1, r1.get().size());
    assertEquals(batch2, r2.get().size());
    assertEquals(0, pool.getPoolSize());

    final Future future1 = service.submit(new Runnable()
    {
      @Override
      public void run()
      {
        r1.close();
      }
    });
    final Future future2 = service.submit(new Runnable()
    {
      @Override
      public void run()
      {
        r2.close();
      }
    });

    future1.get();
    future2.get();

    assertEquals(pool.maxSize(), pool.getPoolSize());
  }

  @Test(timeout = 5000)
  public void testConcurrentTakeBatchClose() throws ExecutionException, InterruptedException
  {
    final ReferenceCountingResourceHolder<List<Integer>> r1 = pool.takeBatch(1, 10);

    final Callable<ReferenceCountingResourceHolder<List<Integer>>> c2 =
        new Callable<ReferenceCountingResourceHolder<List<Integer>>>()
        {
          @Override
          public ReferenceCountingResourceHolder<List<Integer>> call() throws Exception
          {
            return pool.takeBatch(10, 100);
          }
        };

    final Future<ReferenceCountingResourceHolder<List<Integer>>> f2 = service.submit(c2);
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
        r1.close();
      }
    });

    final ReferenceCountingResourceHolder<List<Integer>> r2 = f2.get();
    f1.get();
    assertNotNull(r2);
    assertEquals(10, r2.get().size());
    assertEquals(0, pool.getPoolSize());

    r2.close();
    assertEquals(pool.maxSize(), pool.getPoolSize());
  }
}
