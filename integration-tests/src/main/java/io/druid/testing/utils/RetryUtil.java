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

package io.druid.testing.utils;

import com.google.common.base.Throwables;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;

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
