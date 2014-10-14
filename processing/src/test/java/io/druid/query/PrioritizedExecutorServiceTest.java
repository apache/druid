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

package io.druid.query;

import com.google.common.collect.ImmutableList;
import com.metamx.common.concurrent.ExecutorServiceConfig;
import com.metamx.common.lifecycle.Lifecycle;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

/**
 */
public class PrioritizedExecutorServiceTest
{
  private ExecutorService exec;
  private CountDownLatch latch;
  private CountDownLatch finishLatch;

  @Before
  public void setUp() throws Exception
  {
    exec = PrioritizedExecutorService.create(
        new Lifecycle(),
        new ExecutorServiceConfig()
        {
          @Override
          public String getFormatString()
          {
            return "test";
          }

          @Override
          public int getNumThreads()
          {
            return 1;
          }
        }
    );

    latch = new CountDownLatch(1);
    finishLatch = new CountDownLatch(3);
  }

  /**
   * Submits a normal priority task to block the queue, followed by low, high, normal priority tasks.
   * Tests to see that the high priority task is executed first, followed by the normal and low priority tasks.
   *
   * @throws Exception
   */
  @Test
  public void testSubmit() throws Exception
  {
    final ConcurrentLinkedQueue<Integer> order = new ConcurrentLinkedQueue<Integer>();

    exec.submit(
        new AbstractPrioritizedCallable<Void>(0)
        {
          @Override
          public Void call() throws Exception
          {
            latch.await();
            return null;
          }
        }
    );

    exec.submit(
        new AbstractPrioritizedCallable<Void>(-1)
        {
          @Override
          public Void call() throws Exception
          {
            order.add(-1);
            finishLatch.countDown();
            return null;
          }
        }
    );
    exec.submit(
        new AbstractPrioritizedCallable<Void>(0)
        {
          @Override
          public Void call() throws Exception
          {
            order.add(0);
            finishLatch.countDown();
            return null;
          }
        }
    );
    exec.submit(
        new AbstractPrioritizedCallable<Void>(2)
        {
          @Override
          public Void call() throws Exception
          {
            order.add(2);
            finishLatch.countDown();
            return null;
          }
        }
    );

    latch.countDown();
    finishLatch.await();

    Assert.assertTrue(order.size() == 3);

    List<Integer> expected = ImmutableList.of(2, 0, -1);
    Assert.assertEquals(expected, ImmutableList.copyOf(order));
  }
}
