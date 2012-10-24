package com.metamx.druid.client;

import org.skife.config.Config;
import org.skife.config.Default;

/**
 */
public abstract class ServerInventoryManagerConfig
{
  @Config("druid.zk.paths.announcementsPath")
  public abstract String getServerIdPath();

  @Config("druid.zk.paths.servedSegmentsPath")
  public abstract String getServerInventoryPath();

  @Config("druid.master.removedSegmentLifetime")
  @Default("1")
  public abstract int getRemovedSegmentLifetime();
}