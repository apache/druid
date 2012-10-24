package com.metamx.druid.merger.coordinator.config;

import org.skife.config.Config;
import org.skife.config.Default;

/**
 */
public abstract class RetryPolicyConfig
{
  @Config("druid.indexer.retry.minWaitMillis")
  @Default("10000")
  public abstract long getRetryMinMillis();

  @Config("druid.indexer.retry.maxWaitMillis")
  @Default("60000")
  public abstract long getRetryMaxMillis();

  @Config("druid.indexer.retry.maxRetryCount")
  @Default("10")
  public abstract long getMaxRetryCount();
}
