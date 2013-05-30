/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.query;

import com.google.common.collect.Lists;
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
public class PriorityExecutorServiceTest
{
  private ExecutorService exec;
  private CountDownLatch latch;
  private CountDownLatch finishLatch;

  @Before
  public void setUp() throws Exception
  {
    exec = PriorityExecutorService.create(
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
    final ConcurrentLinkedQueue<Queries.Priority> order = new ConcurrentLinkedQueue<Queries.Priority>();

    exec.submit(
        new PrioritizedCallable<Void>()
        {
          @Override
          public int getPriority()
          {
            return Queries.Priority.NORMAL.ordinal();
          }

          @Override
          public Void call() throws Exception
          {
            latch.await();
            return null;
          }
        }
    );

    exec.submit(
        new PrioritizedCallable<Void>()
        {
          @Override
          public int getPriority()
          {
            return Queries.Priority.LOW.ordinal();
          }

          @Override
          public Void call() throws Exception
          {
            order.add(Queries.Priority.LOW);
            finishLatch.countDown();
            return null;
          }
        }
    );
    exec.submit(
        new PrioritizedCallable<Void>()
        {
          @Override
          public int getPriority()
          {
            return Queries.Priority.HIGH.ordinal();
          }

          @Override
          public Void call() throws Exception
          {
            order.add(Queries.Priority.HIGH);
            finishLatch.countDown();
            return null;
          }
        }
    );
    exec.submit(
        new PrioritizedCallable<Void>()
        {
          @Override
          public int getPriority()
          {
            return Queries.Priority.NORMAL.ordinal();
          }

          @Override
          public Void call() throws Exception
          {
            order.add(Queries.Priority.NORMAL);
            finishLatch.countDown();
            return null;
          }
        }
    );

    latch.countDown();
    finishLatch.await();

    Assert.assertTrue(order.size() == 3);

    List<Queries.Priority> expected = Lists.newArrayList(
        Queries.Priority.HIGH,
        Queries.Priority.NORMAL,
        Queries.Priority.LOW
    );

    int i = 0;
    for (Queries.Priority priority : order) {
      Assert.assertEquals(expected.get(i), priority);
      i++;
    }
  }
}
