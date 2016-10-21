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

package io.druid.concurrent;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.druid.java.util.common.logger.Logger;

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
