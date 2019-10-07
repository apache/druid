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

package org.apache.druid.java.util.common;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;
import java.util.concurrent.ThreadLocalRandom;

public class RetryUtils
{
  public static final Logger log = new Logger(RetryUtils.class);
  public static final long MAX_SLEEP_MILLIS = 60000;
  public static final long BASE_SLEEP_MILLIS = 1000;

  public interface Task<T>
  {
    /**
     * This method is tried up to maxTries times unless it succeeds.
     */
    T perform() throws Exception;
  }

  public interface CleanupAfterFailure
  {
    /**
     * This is called once {@link Task#perform()} fails. Retrying is stopped once this method throws an exception,
     * so errors inside this method should be ignored if you don't want to stop retrying.
     */
    void cleanup();
  }

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
      final Task<T> f,
      final Predicate<Throwable> shouldRetry,
      final int quietTries,
      final int maxTries,
      @Nullable final CleanupAfterFailure cleanupAfterFailure,
      @Nullable final String messageOnRetry
  ) throws Exception
  {
    Preconditions.checkArgument(maxTries > 0, "maxTries > 0");
    Preconditions.checkArgument(quietTries >= 0, "quietTries >= 0");
    int nTry = 0;
    final int maxRetries = maxTries - 1;
    while (true) {
      try {
        nTry++;
        return f.perform();
      }
      catch (Throwable e) {
        if (cleanupAfterFailure != null) {
          cleanupAfterFailure.cleanup();
        }
        if (nTry < maxTries && shouldRetry.apply(e)) {
          awaitNextRetry(e, messageOnRetry, nTry, maxRetries, nTry <= quietTries);
        } else {
          Throwables.propagateIfInstanceOf(e, Exception.class);
          throw new RuntimeException(e);
        }
      }
    }
  }

  public static <T> T retry(final Task<T> f, Predicate<Throwable> shouldRetry, final int maxTries) throws Exception
  {
    return retry(f, shouldRetry, 0, maxTries);
  }

  public static <T> T retry(
      final Task<T> f,
      final Predicate<Throwable> shouldRetry,
      final int quietTries,
      final int maxTries
  ) throws Exception
  {
    return retry(f, shouldRetry, quietTries, maxTries, null, null);
  }

  public static <T> T retry(
      final Task<T> f,
      final Predicate<Throwable> shouldRetry,
      final CleanupAfterFailure onEachFailure,
      final int maxTries,
      final String messageOnRetry
  ) throws Exception
  {
    return retry(f, shouldRetry, 0, maxTries, onEachFailure, messageOnRetry);
  }

  public static void awaitNextRetry(
      final Throwable e,
      @Nullable final String messageOnRetry,
      final int nTry,
      final int maxRetries,
      final boolean quiet
  ) throws InterruptedException
  {
    final long sleepMillis = nextRetrySleepMillis(nTry);
    final String fullMessage;

    if (messageOnRetry == null) {
      fullMessage = StringUtils.format("Retrying (%d of %d) in %,dms.", nTry, maxRetries, sleepMillis);
    } else {
      fullMessage = StringUtils.format(
          "%s, retrying (%d of %d) in %,dms.",
          messageOnRetry,
          nTry,
          maxRetries,
          sleepMillis
      );
    }

    if (quiet) {
      log.debug(e, fullMessage);
    } else {
      log.warn(e, fullMessage);
    }

    Thread.sleep(sleepMillis);
  }

  public static long nextRetrySleepMillis(final int nTry)
  {
    final double fuzzyMultiplier = Math.min(Math.max(1 + 0.2 * ThreadLocalRandom.current().nextGaussian(), 0), 2);
    final long sleepMillis = (long) (Math.min(MAX_SLEEP_MILLIS, BASE_SLEEP_MILLIS * Math.pow(2, nTry - 1))
                                     * fuzzyMultiplier);
    return sleepMillis;
  }
}
