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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public final class Threads
{

  /**
   * Equivalent of {@link Thread#sleep(long)} with arguments and semantics of timed wait methods in classes from {@link
   * java.util.concurrent} (like {@link java.util.concurrent.Semaphore#tryAcquire(long, TimeUnit)},
   * {@link java.util.concurrent.locks.Lock#tryLock(long, TimeUnit)}, etc.): if the sleepTime argument is negative or
   * zero, the method returns immediately. {@link Thread#sleep}, on the contrary, throws an IllegalArgumentException if
   * the argument is negative and attempts to unschedule the thread if the argument is zero.
   *
   * @throws InterruptedException if the current thread is interrupted when this method is called or during sleeping.
   */
  public static void sleepFor(long sleepTime, TimeUnit unit) throws InterruptedException
  {
    if (Thread.interrupted()) {
      throw new InterruptedException();
    }
    if (sleepTime <= 0) {
      return;
    }
    long sleepTimeLimitNanos = System.nanoTime() + unit.toNanos(sleepTime);
    while (true) {
      long sleepTimeoutNanos = sleepTimeLimitNanos - System.nanoTime();
      if (sleepTimeoutNanos <= 0) {
        return;
      }
      LockSupport.parkNanos(sleepTimeoutNanos);
      if (Thread.interrupted()) {
        throw new InterruptedException();
      }
    }
  }

  private Threads()
  {
  }
}
