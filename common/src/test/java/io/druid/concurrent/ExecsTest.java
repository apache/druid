/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013, 2014  Metamarkets Group Inc.
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

package io.druid.concurrent;

import com.google.common.base.Throwables;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class ExecsTest
{
  @Test
  public void testBlockingExecutorService() throws Exception
  {
    final int capacity = 3;
    final ExecutorService blockingExecutor = Execs.newBlockingSingleThreaded("test%d", capacity);
    final CountDownLatch queueFullSignal = new CountDownLatch(capacity + 1);
    final CountDownLatch taskCompletedSignal = new CountDownLatch(2 * capacity);
    final CountDownLatch taskStartSignal = new CountDownLatch(1);
    final AtomicInteger producedCount = new AtomicInteger();
    final AtomicInteger consumedCount = new AtomicInteger();
    ExecutorService producer = Executors.newSingleThreadExecutor();
    producer.submit(
        new Runnable()
        {
          public void run()
          {
            for (int i = 0; i < 2 * capacity; i++) {
              final int taskID = i;
              System.out.println("Produced task" + taskID);
              blockingExecutor.submit(
                  new Runnable()
                  {
                    @Override
                    public void run()
                    {
                      System.out.println("Starting task" + taskID);
                      try {
                        taskStartSignal.await();
                        consumedCount.incrementAndGet();
                        taskCompletedSignal.countDown();
                      }
                      catch (Exception e) {
                        throw Throwables.propagate(e);
                      }
                      System.out.println("Completed task" + taskID);
                    }
                  }
              );
              producedCount.incrementAndGet();
              queueFullSignal.countDown();
            }
          }
        }
    );

    queueFullSignal.await();
    // verify that the producer blocks
    Assert.assertEquals(capacity + 1, producedCount.get());
    // let the tasks run
    taskStartSignal.countDown();
    // wait until all tasks complete
    taskCompletedSignal.await();
    // verify all tasks consumed
    Assert.assertEquals(2 * capacity, consumedCount.get());
    // cleanup
    blockingExecutor.shutdown();
    producer.shutdown();

  }
}
