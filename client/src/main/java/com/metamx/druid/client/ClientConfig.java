package com.metamx.druid.client;

import org.skife.config.Config;

/**
 */
public abstract class ClientConfig
{
  @Config("druid.zk.paths.announcementsPath")
  public abstract String getAnnouncementsPath();

  @Config("druid.zk.paths.servedSegmentsPath")
  public abstract String getServedSegmentsPath();

  public InventoryManagerConfig getClientInventoryManagerConfig()
  {
    return new InventoryManagerConfig(
        getAnnouncementsPath(),
        getServedSegmentsPath()        
    );
  }
}
