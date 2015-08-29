/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.query;

import com.google.common.collect.ImmutableList;
import com.metamx.common.concurrent.ExecutorServiceConfig;
import com.metamx.common.lifecycle.Lifecycle;
import org.junit.Assert;
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
