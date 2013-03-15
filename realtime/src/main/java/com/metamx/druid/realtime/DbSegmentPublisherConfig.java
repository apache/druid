package com.metamx.druid.realtime;

import org.skife.config.Config;

public abstract class DbSegmentPublisherConfig
{
  @Config("druid.database.segmentTable")
  public abstract String getSegmentTable();
}
