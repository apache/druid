package com.metamx.druid;

import org.skife.config.Config;
import org.skife.config.Default;

/**
 */
public abstract class GroupByQueryEngineConfig
{
  @Config("druid.query.groupBy.maxIntermediateRows")
  @Default("50000")
  public abstract int getMaxIntermediateRows();
}
