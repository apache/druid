/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.client.cache;


import com.metamx.common.ISE;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class BytesBoundedLinkedQueueTest
{
  private static int delayMS = 50;
  private ExecutorService exec = Executors.newCachedThreadPool();

  private static BlockingQueue<TestObject> getQueue(final int capacity)
  {
    return new BytesBoundedLinkedQueue<TestObject>(capacity)
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
    Assert.assertNull(q.poll(delayMS, TimeUnit.MILLISECONDS));
    Assert.assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime) >= delayMS);
    TestObject obj = new TestObject(2);
    Assert.assertTrue(q.offer(obj, delayMS, TimeUnit.MILLISECONDS));
    Assert.assertSame(obj, q.poll(delayMS, TimeUnit.MILLISECONDS));

    Thread.currentThread().interrupt();
    try {
      q.poll(delayMS, TimeUnit.MILLISECONDS);
      throw new ISE("FAIL");
    }
    catch (InterruptedException success) {
    }
    Assert.assertFalse(Thread.interrupted());
  }

  @Test
  public void testTake() throws Exception
  {
    final BlockingQueue<TestObject> q = getQueue(10);
    Thread.currentThread().interrupt();
    try {
      q.take();
      Assert.fail();
    }
    catch (InterruptedException success) {
      //
    }
    final CountDownLatch latch = new CountDownLatch(1);
    final TestObject object = new TestObject(4);
    Future<TestObject> future = exec.submit(
        new Callable<TestObject>()
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
      Assert.fail();
    }
    catch (TimeoutException success) {

    }

    q.offer(object);
    Assert.assertEquals(object, future.get());
  }

  @Test
  public void testOfferAndPut() throws Exception
  {
    final BlockingQueue<TestObject> q = getQueue(10);
    try {
      q.offer(null);
      Assert.fail();
    }
    catch (NullPointerException success) {

    }

    final TestObject obj = new TestObject(2);
    while (q.remainingCapacity() > 0) {
      Assert.assertTrue(q.offer(obj, delayMS, TimeUnit.MILLISECONDS));
    }
    // queue full
    Assert.assertEquals(0, q.remainingCapacity());
    Assert.assertFalse(q.offer(obj, delayMS, TimeUnit.MILLISECONDS));
    Assert.assertFalse(q.offer(obj));
    final CyclicBarrier barrier = new CyclicBarrier(2);

    Future<Boolean> future = exec.submit(
        new Callable<Boolean>()
        {
          @Override
          public Boolean call() throws Exception
          {
            barrier.await();
            Assert.assertTrue(q.offer(obj, delayMS, TimeUnit.MILLISECONDS));
            Assert.assertEquals(q.remainingCapacity(), 0);
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
    Assert.assertTrue(future.get());

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