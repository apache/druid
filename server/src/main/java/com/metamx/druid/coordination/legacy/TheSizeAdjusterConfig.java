package com.metamx.druid.coordination.legacy;

import org.skife.config.Config;

/**
 */
public abstract class TheSizeAdjusterConfig
{
  @Config("druid.zk.paths.indexesPath")
  public abstract String getSegmentBasePath();
}
