/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.merger.coordinator;

import java.util.List;

import com.google.common.collect.Lists;
import com.metamx.druid.merger.coordinator.config.RetryPolicyConfig;
import com.metamx.emitter.EmittingLogger;

/**
 */
public class RetryPolicy
{
  private static final EmittingLogger log = new EmittingLogger(RetryPolicy.class);

  private final long MAX_NUM_RETRIES;
  private final long MAX_RETRY_DELAY_MILLIS;

  private final List<Runnable> runnables = Lists.newArrayList();

  private volatile long currRetryDelay;
  private volatile int retryCount;

  public RetryPolicy(RetryPolicyConfig config)
  {
    this.MAX_NUM_RETRIES = config.getMaxRetryCount();
    this.MAX_RETRY_DELAY_MILLIS = config.getRetryMaxMillis();

    this.currRetryDelay = config.getRetryMinMillis();
    this.retryCount = 0;
  }

  /**
   * Register runnables that can be run at any point in a given retry.
   * @param runnable
   */
  public void registerRunnable(Runnable runnable)
  {
    runnables.add(runnable);
  }

  public void runRunnables()
  {
    for (Runnable runnable : runnables) {
      runnable.run();
    }
    runnables.clear();
  }

  public long getAndIncrementRetryDelay()
  {
    long retVal = currRetryDelay;
    if (currRetryDelay < MAX_RETRY_DELAY_MILLIS) {
      currRetryDelay *= 2;
    }

    retryCount++;

    return retVal;
  }

  public int getNumRetries()
  {
    return retryCount;
  }

  public boolean hasExceededRetryThreshold()
  {
    return retryCount >= MAX_NUM_RETRIES;
  }
}
