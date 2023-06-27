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

package org.apache.druid.client.cache;


import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

//from 278 lines to 245
class BytesBoundedLinkedQueueTest
{
  private static final int DELAY_MS = 50;
  private final ExecutorService exec = Executors.newCachedThreadPool();

  private static BlockingQueue<TestObject> getQueue(final int capacity)
  {
    return new BytesBoundedLinkedQueue<TestObject>(capacity)
    {
      @Override
      public long getBytesSize(final TestObject o)
      {
        return o.getSize();
      }
    };
  }

  @Test
  void testPoll() throws InterruptedException
  {
    final BlockingQueue<TestObject> q = getQueue(10);
    long startTime = System.nanoTime();
    assertNull(q.poll(DELAY_MS, TimeUnit.MILLISECONDS));
    assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime) >= DELAY_MS);

    TestObject obj = new TestObject(2);
    assertTrue(q.offer(obj, DELAY_MS, TimeUnit.MILLISECONDS));
    assertSame(obj, q.poll(DELAY_MS, TimeUnit.MILLISECONDS));

    Thread.currentThread().interrupt();

    assertThrows(
        InterruptedException.class,
        () -> q.poll(DELAY_MS, TimeUnit.MILLISECONDS)
    );
    assertFalse(Thread.interrupted());
  }

  @Test
  void testTake() throws Exception
  {
    final BlockingQueue<TestObject> q = getQueue(10);
    Thread.currentThread().interrupt();

    assertThrows(InterruptedException.class, q::take);

    final CountDownLatch latch = new CountDownLatch(1);
    final TestObject object = new TestObject(4);
    Future<TestObject> future = exec.submit(
        () -> {
          latch.countDown();
          return q.take();

        }
    );
    latch.await();
    // test take blocks on empty queue
    assertThrows(TimeoutException.class, () -> future.get(DELAY_MS, TimeUnit.MILLISECONDS));
    assertTrue(q.offer(object));
    assertEquals(object, future.get());
  }

  @Test
  void testOfferAndPut() throws Exception
  {
    final BlockingQueue<TestObject> q = getQueue(10);

    assertThrows(NullPointerException.class, () -> q.offer(null));

    final TestObject obj = new TestObject(2);
    while (q.remainingCapacity() > 0) {
      assertTrue(q.offer(obj, DELAY_MS, TimeUnit.MILLISECONDS));
    }
    // queue full
    assertEquals(0, q.remainingCapacity());
    assertFalse(q.offer(obj, DELAY_MS, TimeUnit.MILLISECONDS));
    assertFalse(q.offer(obj));
    final CyclicBarrier barrier = new CyclicBarrier(2);

    Future<Boolean> future = exec.submit(
        () -> {
          barrier.await();
          assertTrue(q.offer(obj, DELAY_MS, TimeUnit.MILLISECONDS));
          assertEquals(q.remainingCapacity(), 0);
          barrier.await();
          q.put(obj);
          return true;
        }
    );
    barrier.await();
    q.take();
    barrier.await();
    q.take();
    assertTrue(future.get());

  }

  @Test
  void testAddBiggerElementThanCapacityFails()
  {
    BlockingQueue<TestObject> q = getQueue(5);
    assertThrows(IllegalArgumentException.class, () -> q.offer(new TestObject(10)));
  }

  @Test
  void testAddedObjectExceedsCapacity() throws Exception
  {
    BlockingQueue<TestObject> q = getQueue(4);
    assertTrue(q.offer(new TestObject(3)));
    assertFalse(q.offer(new TestObject(2)));
    assertFalse(q.offer(new TestObject(2), DELAY_MS, TimeUnit.MILLISECONDS));
  }

  @Test
  void testConcurrentOperations() throws Exception
  {
    final BlockingQueue<TestObject> q = getQueue(Integer.MAX_VALUE);
    long duration = TimeUnit.SECONDS.toMillis(10);
    ExecutorService executor = Executors.newCachedThreadPool();
    final AtomicBoolean stopTest = new AtomicBoolean(false);
    List<Future<Boolean>> futures = new ArrayList<>();

    IntStream.range(0, 5)
             .forEach(
                 numberInRange ->
                     futures.add(
                         executor.submit(
                             () -> {
                               while (!stopTest.get()) {
                                 q.add(new TestObject(1));
                                 q.add(new TestObject(2));
                               }
                               return true;
                             }
                         )
                     ));

    IntStream.range(0, 10)
             .forEach(
                 numberInRange ->
                     futures.add(
                         executor.submit(
                             () -> {
                               while (!stopTest.get()) {
                                 q.poll(100, TimeUnit.MILLISECONDS);
                                 q.offer(new TestObject(2));
                               }
                               return true;
                             }
                         )
                     ));

    IntStream.range(0, 5)
             .forEach(
                 numberInRange ->
                     futures.add(
                         executor.submit(
                             () -> {
                               while (!stopTest.get()) {
                                 q.drainTo(new ArrayList<>(), Integer.MAX_VALUE);
                               }
                               return true;
                             }
                         )
                     ));

    Thread.sleep(duration);
    stopTest.set(true);

    futures.forEach(future -> {
      try {
        Boolean aBoolean = (Boolean) future.get();
        assertTrue(aBoolean);
      }
      catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    });
  }

  public static class TestObject
  {
    public final int size;

    TestObject(int size)
    {
      this.size = size;
    }

    public int getSize()
    {
      return size;
    }
  }
}
