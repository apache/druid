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

package org.apache.druid.concurrent;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class LifecycleLockTest
{

  @Test
  public void testOnlyOneCanStart() throws InterruptedException
  {
    for (int i = 0; i < 100; i++) {
      testOnlyOneCanStartRun();
    }
  }

  private void testOnlyOneCanStartRun() throws InterruptedException
  {
    final LifecycleLock lifecycleLock = new LifecycleLock();
    final CountDownLatch startLatch = new CountDownLatch(1);
    int numThreads = 100;
    final CountDownLatch finishLatch = new CountDownLatch(numThreads);
    final AtomicInteger successful = new AtomicInteger(0);
    for (int i = 0; i < numThreads; i++) {
      new Thread()
      {
        @Override
        public void run()
        {
          try {
            startLatch.await();
            if (lifecycleLock.canStart()) {
              successful.incrementAndGet();
            }
            finishLatch.countDown();
          }
          catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      }.start();
    }
    startLatch.countDown();
    finishLatch.await();
    Assertions.assertEquals(1, successful.get());
  }

  @Test
  public void testOnlyOneCanStop() throws InterruptedException
  {
    for (int i = 0; i < 100; i++) {
      testOnlyOneCanStopRun();
    }
  }

  private void testOnlyOneCanStopRun() throws InterruptedException
  {
    final LifecycleLock lifecycleLock = new LifecycleLock();
    Assertions.assertTrue(lifecycleLock.canStart());
    lifecycleLock.started();
    lifecycleLock.exitStart();
    final CountDownLatch startLatch = new CountDownLatch(1);
    int numThreads = 100;
    final CountDownLatch finishLatch = new CountDownLatch(numThreads);
    final AtomicInteger successful = new AtomicInteger(0);
    for (int i = 0; i < numThreads; i++) {
      new Thread()
      {
        @Override
        public void run()
        {
          try {
            startLatch.await();
            if (lifecycleLock.canStop()) {
              successful.incrementAndGet();
            }
            finishLatch.countDown();
          }
          catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      }.start();
    }
    startLatch.countDown();
    finishLatch.await();
    Assertions.assertEquals(1, successful.get());
  }

  @Test
  public void testNoStartAfterStop()
  {
    LifecycleLock lifecycleLock = new LifecycleLock();
    Assertions.assertTrue(lifecycleLock.canStart());
    lifecycleLock.started();
    lifecycleLock.exitStart();
    Assertions.assertTrue(lifecycleLock.canStop());
    Assertions.assertFalse(lifecycleLock.canStart());
  }

  @Test
  public void testNotStarted()
  {
    LifecycleLock lifecycleLock = new LifecycleLock();
    Assertions.assertTrue(lifecycleLock.canStart());
    lifecycleLock.exitStart();
    Assertions.assertFalse(lifecycleLock.awaitStarted());
    Assertions.assertFalse(lifecycleLock.canStop());
  }

  @Test
  public void testRestart()
  {
    LifecycleLock lifecycleLock = new LifecycleLock();
    Assertions.assertTrue(lifecycleLock.canStart());
    lifecycleLock.started();
    lifecycleLock.exitStart();
    Assertions.assertTrue(lifecycleLock.canStop());
    lifecycleLock.exitStopAndReset();
    Assertions.assertTrue(lifecycleLock.canStart());
  }

  @Test
  public void testDoubleStarted()
  {
    LifecycleLock lifecycleLock = new LifecycleLock();
    lifecycleLock.canStart();
    lifecycleLock.started();
    Assertions.assertThrows(IllegalMonitorStateException.class, lifecycleLock::started);
  }

  @Test
  public void testDoubleExitStart()
  {
    LifecycleLock lifecycleLock = new LifecycleLock();
    lifecycleLock.canStart();
    lifecycleLock.started();
    lifecycleLock.exitStart();
    Assertions.assertThrows(IllegalMonitorStateException.class, lifecycleLock::exitStart);
  }

  @Test
  public void testCanStopNotExitedStart()
  {
    LifecycleLock lifecycleLock = new LifecycleLock();
    lifecycleLock.canStart();
    lifecycleLock.started();
    Assertions.assertThrows(IllegalMonitorStateException.class, lifecycleLock::canStop);
  }
}
