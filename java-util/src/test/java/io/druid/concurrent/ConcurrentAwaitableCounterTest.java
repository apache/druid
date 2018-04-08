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

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ConcurrentAwaitableCounterTest
{

  @Test(timeout = 1000)
  public void smokeTest() throws InterruptedException
  {
    ConcurrentAwaitableCounter counter = new ConcurrentAwaitableCounter();
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch finish = new CountDownLatch(7);
    for (int i = 0; i < 2; i++) {
      new Thread(() -> {
        try {
          start.await();
          for (int j = 0; j < 10_000; j++) {
            counter.increment();
          }
          finish.countDown();
        }
        catch (InterruptedException e) {
          throw new RuntimeException(e);
        }

      }).start();
    }
    for (int awaitCount : new int[] {0, 1, 100, 10_000, 20_000}) {
      new Thread(() -> {
        try {
          start.await();
          counter.awaitCount(awaitCount);
          finish.countDown();
        }
        catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }).start();
    }
    start.countDown();
    finish.await();
  }

  @Test
  public void testAwaitFirstUpdate() throws InterruptedException
  {
    int[] value = new int[1];
    ConcurrentAwaitableCounter counter = new ConcurrentAwaitableCounter();
    Thread t = new Thread(() -> {
      try {
        Assert.assertTrue(counter.awaitFirstIncrement(10, TimeUnit.SECONDS));
        Assert.assertEquals(1, value[0]);
      }
      catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

    });
    t.start();
    Thread.sleep(2_000);
    value[0] = 1;
    counter.increment();
    t.join();
  }
}
