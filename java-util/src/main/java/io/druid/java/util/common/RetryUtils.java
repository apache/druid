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

package io.druid.java.util.common;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import io.druid.java.util.common.logger.Logger;

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;

public class RetryUtils
{
  public static final Logger log = new Logger(RetryUtils.class);

  /**
   * Retry an operation using fuzzy exponentially increasing backoff. The wait time after the nth failed attempt is
   * min(60000ms, 1000ms * pow(2, n - 1)), fuzzed by a number drawn from a Gaussian distribution with mean 0 and
   * standard deviation 0.2.
   *
   * If maxTries is exhausted, or if shouldRetry returns false, the last exception thrown by "f" will be thrown
   * by this function.
   *
   * @param f           the operation
   * @param shouldRetry predicate determining whether we should retry after a particular exception thrown by "f"
   * @param quietTries  first quietTries attempts will log exceptions at DEBUG level rather than WARN
   * @param maxTries    maximum number of attempts
   *
   * @return result of the first successful operation
   *
   * @throws Exception if maxTries is exhausted, or shouldRetry returns false
   */
  public static <T> T retry(
      final Callable<T> f,
      Predicate<Throwable> shouldRetry,
      final int quietTries,
      final int maxTries
  ) throws Exception
  {
    Preconditions.checkArgument(maxTries > 0, "maxTries > 0");
    int nTry = 0;
    while (true) {
      try {
        nTry++;
        return f.call();
      }
      catch (Throwable e) {
        if (nTry < maxTries && shouldRetry.apply(e)) {
          awaitNextRetry(e, nTry, nTry <= quietTries);
        } else {
          Throwables.propagateIfInstanceOf(e, Exception.class);
          throw Throwables.propagate(e);
        }
      }
    }
  }

  /**
   * Same as {@link #retry(Callable, Predicate, int, int)} with quietTries = 0.
   */
  public static <T> T retry(final Callable<T> f, Predicate<Throwable> shouldRetry, final int maxTries) throws Exception
  {
    return retry(f, shouldRetry, 0, maxTries);
  }

  private static void awaitNextRetry(final Throwable e, final int nTry, final boolean quiet) throws InterruptedException
  {
    final long baseSleepMillis = 1000;
    final long maxSleepMillis = 60000;
    final double fuzzyMultiplier = Math.min(Math.max(1 + 0.2 * ThreadLocalRandom.current().nextGaussian(), 0), 2);
    final long sleepMillis = (long) (Math.min(maxSleepMillis, baseSleepMillis * Math.pow(2, nTry - 1))
                                     * fuzzyMultiplier);
    if (quiet) {
      log.debug(e, "Failed on try %d, retrying in %,dms.", nTry, sleepMillis);
    } else {
      log.warn(e, "Failed on try %d, retrying in %,dms.", nTry, sleepMillis);
    }
    Thread.sleep(sleepMillis);
  }
}
