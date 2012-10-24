package com.metamx.druid.master;

import org.joda.time.Duration;
import org.skife.config.Config;
import org.skife.config.Default;

/**
 */
public abstract class DruidMasterConfig
{
  @Config("druid.host")
  public abstract String getHost();

  @Config("druid.zk.paths.masterPath")
  public abstract String getBasePath();

  @Config("druid.zk.paths.loadQueuePath")
  public abstract String getLoadQueuePath();

  @Config("druid.zk.paths.servedSegmentsPath")
  public abstract String getServedSegmentsLocation();

  @Config("druid.master.startDelay")
  @Default("PT600s")
  public abstract Duration getMasterStartDelay();

  @Config("druid.master.period")
  @Default("PT60s")
  public abstract Duration getMasterPeriod();

  @Config("druid.master.period.segmentMerger")
  @Default("PT1800s")
  public abstract Duration getMasterSegmentMergerPeriod();

  @Config("druid.master.millisToWaitBeforeDeleting")
  public long getMillisToWaitBeforeDeleting()
  {
    return 15 * 60 * 1000L;
  }

  @Config("druid.master.merger.on")
  public boolean isMergeSegments()
  {
    return true;
  }

  @Config("druid.master.merger.service")
  public abstract String getMergerServiceName();

  @Config("druid.master.merge.threshold")
  public long getMergeThreshold()
  {
    return 100000000L;
  }
}
