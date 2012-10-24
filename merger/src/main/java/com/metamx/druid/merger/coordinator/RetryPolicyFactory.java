package com.metamx.druid.merger.coordinator;

import com.metamx.druid.merger.coordinator.config.RetryPolicyConfig;

/**
 */
public class RetryPolicyFactory
{
  private final RetryPolicyConfig config;

  public RetryPolicyFactory(RetryPolicyConfig config)
  {
    this.config = config;
  }

  public RetryPolicy makeRetryPolicy()
  {
    return new RetryPolicy(config);
  }
}
