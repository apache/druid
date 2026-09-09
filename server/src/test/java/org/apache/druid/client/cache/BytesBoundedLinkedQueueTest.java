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


import org.apache.druid.java.util.common.ISE;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;


public class BytesBoundedLinkedQueueTest
{
  private static int delayMS = 50;
  private ExecutorService exec = Executors.newCachedThreadPool();

  private static BlockingQueue<TestObject> getQueue(final int capacity)
  {
    return new BytesBoundedLinkedQueue<>(capacity)
    {
      @Override
      public long getBytesSize(TestObject o)
      {
        return o.getSize();
      }
    };
  }

  @Test
  public void testPoll() throws InterruptedException
  {
    final BlockingQueue q = getQueue(10);
    long startTime = System.nanoTime();
    Assertions.assertNull(q.poll(delayMS, TimeUnit.MILLISECONDS));
    Assertions.assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime) >= delayMS);
    TestObject obj = new TestObject(2);
    Assertions.assertTrue(q.offer(obj, delayMS, TimeUnit.MILLISECONDS));
    Assertions.assertSame(obj, q.poll(delayMS, TimeUnit.MILLISECONDS));

    Thread.currentThread().interrupt();
    try {
      q.poll(delayMS, TimeUnit.MILLISECONDS);
      throw new ISE("FAIL");
    }
    catch (InterruptedException success) {
    }
    Assertions.assertFalse(Thread.interrupted());
  }

  @Test
  public void testTake() throws Exception
  {
    final BlockingQueue<TestObject> q = getQueue(10);
    Thread.currentThread().interrupt();
    try {
      q.take();
      Assertions.fail();
    }
    catch (InterruptedException success) {
      //
    }
    final CountDownLatch latch = new CountDownLatch(1);
    final TestObject object = new TestObject(4);
    Future<TestObject> future = exec.submit(
        new Callable<>()
        {
          @Override
          public TestObject call() throws Exception
          {
            latch.countDown();
            return q.take();

          }
        }
    );
    latch.await();
    // test take blocks on empty queue
    try {
      future.get(delayMS, TimeUnit.MILLISECONDS);
      Assertions.fail();
    }
    catch (TimeoutException success) {

    }

    q.offer(object);
    Assertions.assertEquals(object, future.get());
  }

  @Test
  public void testOfferAndPut() throws Exception
  {
    final BlockingQueue<TestObject> q = getQueue(10);
    try {
      q.offer(null);
      Assertions.fail();
    }
    catch (NullPointerException success) {

    }

    final TestObject obj = new TestObject(2);
    while (q.remainingCapacity() > 0) {
      Assertions.assertTrue(q.offer(obj, delayMS, TimeUnit.MILLISECONDS));
    }
    // queue full
    Assertions.assertEquals(0, q.remainingCapacity());
    Assertions.assertFalse(q.offer(obj, delayMS, TimeUnit.MILLISECONDS));
    Assertions.assertFalse(q.offer(obj));
    final CyclicBarrier barrier = new CyclicBarrier(2);

    Future<Boolean> future = exec.submit(
        new Callable<>()
        {
          @Override
          public Boolean call() throws Exception
          {
            barrier.await();
            Assertions.assertTrue(q.offer(obj, delayMS, TimeUnit.MILLISECONDS));
            Assertions.assertEquals(q.remainingCapacity(), 0);
            barrier.await();
            q.put(obj);
            return true;
          }
        }
    );
    barrier.await();
    q.take();
    barrier.await();
    q.take();
    Assertions.assertTrue(future.get());

  }

  @Test
  public void testAddBiggerElementThanCapacityFails()
  {
    BlockingQueue<TestObject> q = getQueue(5);
    try {
      q.offer(new TestObject(10));
      Assertions.fail();
    }
    catch (IllegalArgumentException success) {

    }
  }

  @Test
  public void testAddedObjectExceedsCapacity() throws Exception
  {
    BlockingQueue<TestObject> q = getQueue(4);
    Assertions.assertTrue(q.offer(new TestObject(3)));
    Assertions.assertFalse(q.offer(new TestObject(2)));
    Assertions.assertFalse(q.offer(new TestObject(2), delayMS, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testConcurrentOperations() throws Exception
  {
    final BlockingQueue<TestObject> q = getQueue(Integer.MAX_VALUE);
    long duration = TimeUnit.SECONDS.toMillis(10);
    ExecutorService executor = Executors.newCachedThreadPool();
    final AtomicBoolean stopTest = new AtomicBoolean(false);
    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      futures.add(
          executor.submit(
              new Callable<Boolean>()
              {
                @Override
                public Boolean call()
                {
                  while (!stopTest.get()) {
                    q.add(new TestObject(1));
                    q.add(new TestObject(2));
                  }
                  return true;

                }
              }
          )
      );
    }

    for (int i = 0; i < 10; i++) {
      futures.add(
          executor.submit(
              new Callable<Boolean>()
              {
                @Override
                public Boolean call() throws InterruptedException
                {
                  while (!stopTest.get()) {
                    q.poll(100, TimeUnit.MILLISECONDS);
                    q.offer(new TestObject(2));
                  }
                  return true;

                }
              }
          )
      );
    }

    for (int i = 0; i < 5; i++) {
      futures.add(
          executor.submit(
              new Callable<Boolean>()
              {
                @Override
                public Boolean call()
                {
                  while (!stopTest.get()) {
                    q.drainTo(new ArrayList<>(), Integer.MAX_VALUE);
                  }
                  return true;
                }
              }
          )
      );
    }
    Thread.sleep(duration);
    stopTest.set(true);
    for (Future<Boolean> future : futures) {
      Assertions.assertTrue(future.get());
    }
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
