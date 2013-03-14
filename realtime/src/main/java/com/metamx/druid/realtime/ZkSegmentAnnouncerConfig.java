package com.metamx.druid.realtime;

import org.skife.config.Config;
import org.skife.config.Default;

public abstract class ZkSegmentAnnouncerConfig
{
  @Config("druid.host")
  public abstract String getServerName();

  @Config("druid.host")
  public abstract String getHost();

  @Config("druid.server.maxSize")
  @Default("0")
  public abstract long getMaxSize();

  @Config("druid.zk.paths.announcementsPath")
  public abstract String getAnnounceLocation();

  @Config("druid.zk.paths.servedSegmentsPath")
  public abstract String getServedSegmentsLocation();
}
