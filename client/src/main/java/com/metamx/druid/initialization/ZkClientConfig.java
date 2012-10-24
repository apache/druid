package com.metamx.druid.initialization;

import org.skife.config.Config;

/**
 */
public abstract class ZkClientConfig
{
  @Config("druid.zk.service.host")
  public abstract String getZkHosts();

  @Config("druid.zk.service.connectionTimeout")
  public int getConnectionTimeout()
  {
    return Integer.MAX_VALUE;
  }
}
