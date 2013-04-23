package com.metamx.druid.coordination;

import org.skife.config.Config;
import org.skife.config.Default;

public abstract class CuratorDataSegmentAnnouncerConfig
{
  @Config("druid.zk.paths.announcementsPath")
  public abstract String getAnnouncementsPath();

  @Config("druid.zk.paths.servedSegmentsPath")
  public abstract String getServedSegmentsPath();

}
