package com.metamx.druid.client;

import org.skife.config.Config;

/**
 */
public abstract class SegmentInventoryManagerConfig
{
  @Config("druid.zk.paths.indexesPath")
  public abstract String getBasePath();
}
