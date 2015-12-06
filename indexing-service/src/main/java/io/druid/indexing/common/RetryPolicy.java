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

package io.druid.indexing.common;

import com.metamx.emitter.EmittingLogger;
import org.joda.time.Duration;

/**
 */
public class RetryPolicy
{
  private static final EmittingLogger log = new EmittingLogger(RetryPolicy.class);

  private final long maxNumRetries;
  private final Duration maxRetryDelay;

  private volatile Duration currRetryDelay;
  private volatile int retryCount;

  public RetryPolicy(RetryPolicyConfig config)
  {
    this.maxNumRetries = config.getMaxRetryCount();
    this.maxRetryDelay = config.getMaxWait().toStandardDuration();

    this.currRetryDelay = config.getMinWait().toStandardDuration();
    this.retryCount = 0;
  }

  public Duration getAndIncrementRetryDelay()
  {
    if (hasExceededRetryThreshold()) {
      return null;
    }

    Duration retVal = currRetryDelay;
    currRetryDelay = new Duration(Math.min(currRetryDelay.getMillis() * 2, maxRetryDelay.getMillis()));
    ++retryCount;
    return retVal;
  }

  public boolean hasExceededRetryThreshold()
  {
    return retryCount >= maxNumRetries;
  }
}
