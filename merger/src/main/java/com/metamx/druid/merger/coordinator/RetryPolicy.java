package com.metamx.druid.merger.coordinator;

import com.google.common.collect.Lists;
import com.metamx.druid.merger.coordinator.config.RetryPolicyConfig;
import com.metamx.emitter.EmittingLogger;

import java.util.List;

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
