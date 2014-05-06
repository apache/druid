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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.metamx.common.logger.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class ExecsTest
{
  private static final Logger log = new Logger(ExecsTest.class);

  @Test
  public void testBlockingExecutorServiceZeroCapacity() throws Exception
  {
    runTest(0);
  }

  @Test
  public void testBlockingExecutorServiceOneCapacity() throws Exception
  {
    runTest(1);
  }

  @Test
  public void testBlockingExecutorServiceThreeCapacity() throws Exception
  {
    runTest(3);
  }

  private static void runTest(final int capacity) throws Exception
  {
    final int nTasks = (capacity + 1) * 3;
    final ExecutorService blockingExecutor = Execs.newBlockingSingleThreaded("ExecsTest-Blocking-%d", capacity);
    final CountDownLatch queueShouldBeFullSignal = new CountDownLatch(capacity + 1);
    final CountDownLatch taskCompletedSignal = new CountDownLatch(nTasks);
    final CountDownLatch taskStartSignal = new CountDownLatch(1);
    final AtomicInteger producedCount = new AtomicInteger();
    final AtomicInteger consumedCount = new AtomicInteger();
    final ExecutorService producer = Executors.newSingleThreadExecutor(
        new ThreadFactoryBuilder().setNameFormat(
            "ExecsTest-Producer-%d"
        ).build()
    );
    producer.submit(
        new Runnable()
        {
          public void run()
          {
            for (int i = 0; i < nTasks; i++) {
              final int taskID = i;
              System.out.println("Produced task" + taskID);
              blockingExecutor.submit(
                  new Runnable()
                  {
                    @Override
                    public void run()
                    {
                      log.info("Starting task: %s", taskID);
                      try {
                        taskStartSignal.await();
                        consumedCount.incrementAndGet();
                        taskCompletedSignal.countDown();
                      }
                      catch (Exception e) {
                        throw Throwables.propagate(e);
                      }
                      log.info("Completed task: %s", taskID);
                    }
                  }
              );
              producedCount.incrementAndGet();
              queueShouldBeFullSignal.countDown();
            }
          }
        }
    );

    queueShouldBeFullSignal.await();
    // Verify that the producer blocks. I don't think it's possible to be sure that the producer is blocking (since
    // it could be doing nothing for any reason). But waiting a short period of time and checking that it hasn't done
    // anything should hopefully be sufficient.
    Thread.sleep(500);
    Assert.assertEquals(capacity + 1, producedCount.get());
    // let the tasks run
    taskStartSignal.countDown();
    // wait until all tasks complete
    taskCompletedSignal.await();
    // verify all tasks consumed
    Assert.assertEquals(nTasks, consumedCount.get());
    // cleanup
    blockingExecutor.shutdown();
    producer.shutdown();
  }
}
