package com.metamx.druid.initialization;

import org.skife.config.Config;
import org.skife.config.Default;

/**
 */
public abstract class ZkDataSegmentAnnouncerConfig extends ZkPathsConfig
{
  @Config("druid.zk.segmentsPerNode")
  @Default("50")
  public abstract int getSegmentsPerNode();

  @Config("druid.zk.maxNumBytesPerNode")
  @Default("512000")
  public abstract long getMaxNumBytes();
}
