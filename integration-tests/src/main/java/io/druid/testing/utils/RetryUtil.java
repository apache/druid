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

package io.druid.testing.utils;

import com.google.common.base.Throwables;

import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class RetryUtil
{

  private static final Logger LOG = new Logger(RetryUtil.class);

  public static int DEFAULT_RETRY_COUNT = 10;

  public static long DEFAULT_RETRY_SLEEP = TimeUnit.SECONDS.toMillis(30);

  public static void retryUntilTrue(Callable<Boolean> callable, String task)
  {
    retryUntil(callable, true, DEFAULT_RETRY_SLEEP, DEFAULT_RETRY_COUNT, task);
  }

  public static void retryUntilFalse(Callable<Boolean> callable, String task)
  {
    retryUntil(callable, false, DEFAULT_RETRY_SLEEP, DEFAULT_RETRY_COUNT, task);
  }

  public static void retryUntil(
      Callable<Boolean> callable,
      boolean expectedValue,
      long delayInMillis,
      int retryCount,
      String taskMessage
  )
  {
    try {
      int currentTry = 0;
      while (callable.call() != expectedValue) {
        if (currentTry > retryCount) {
          throw new ISE("Max number of retries[%d] exceeded for Task[%s]. Failing.", retryCount, taskMessage);
        }
        LOG.info(
            "Attempt[%d]: Task %s still not complete. Next retry in %d ms",
            currentTry, taskMessage, delayInMillis
        );
        Thread.sleep(delayInMillis);

        currentTry++;
      }
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

}
