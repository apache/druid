package com.metamx.druid.query.group;

import org.skife.config.Config;

/**
 */
public abstract class GroupByQueryRunnerFactoryConfig
{
  @Config("druid.query.groupBy.singleThreaded")
  public boolean isSingleThreaded()
  {
    return false;
  }
}
