package com.metamx.druid.coordination;

import org.skife.config.Config;

/**
 */
public abstract class DruidClusterInfoConfig
{
  @Config("druid.zk.paths.masterPath")
  public abstract String getMasterPath();
}
