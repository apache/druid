package com.metamx.druid.coordination;

import org.skife.config.Config;

import java.io.File;

/**
 */
public abstract class ZkCoordinatorConfig
{
  @Config("druid.zk.paths.announcementsPath")
  public abstract String getAnnounceLocation();

  @Config("druid.zk.paths.servedSegmentsPath")
  public abstract String getServedSegmentsLocation();

  @Config("druid.zk.paths.loadQueuePath")
  public abstract String getLoadQueueLocation();

  @Config("druid.paths.segmentInfoCache")
  public abstract File getSegmentInfoCacheDirectory();
}
