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

package org.apache.druid.curator;

import org.apache.curator.RetrySleeper;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.druid.java.util.common.logger.Logger;

/**
 * BoundedExponentialBackoffRetryWithQuit extends BoundedExponentialBackoffRetry for simplicity. It's not actually a
 * BoundedExponentialBackoffRetry from the Liskov substitution principle point of view,
 * but it doesn't matter in this code.
 *
 */
public class BoundedExponentialBackoffRetryWithQuit extends BoundedExponentialBackoffRetry
{

  private static final Logger log = new Logger(BoundedExponentialBackoffRetryWithQuit.class);

  private final Runnable exitRunner;

  public BoundedExponentialBackoffRetryWithQuit(
      Runnable exitRunner,
      int baseSleepTimeMs,
      int maxSleepTimeMs,
      int maxRetries
  )
  {
    super(baseSleepTimeMs, maxSleepTimeMs, maxRetries);
    this.exitRunner = exitRunner;
    log.info("BoundedExponentialBackoffRetryWithQuit Retry Policy selected.");
  }

  @Override
  public boolean allowRetry(int retryCount, long elapsedTimeMs, RetrySleeper sleeper)
  {
    log.warn("Zookeeper can't be reached, retrying (retryCount = %s out of %s)...", retryCount, this.getN());
    boolean shouldRetry = super.allowRetry(retryCount, elapsedTimeMs, sleeper);
    if (!shouldRetry) {
      log.warn("Since Zookeeper can't be reached after retries exhausted, calling exit function...");
      exitRunner.run();
    }
    return shouldRetry;
  }

}
