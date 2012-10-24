package com.metamx.druid.db;


import org.joda.time.Duration;
import org.skife.config.Config;
import org.skife.config.Default;

/**
 */
public abstract class DatabaseSegmentManagerConfig
{
  @Config("druid.database.segmentTable")
  public abstract String getSegmentTable();

  @Config("druid.database.poll.duration")
  @Default("PT1M")
  public abstract Duration getPollDuration();
}
