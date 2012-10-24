package com.metamx.druid.initialization;

import org.skife.config.Config;

/**
 */
public abstract class ServiceDiscoveryConfig
{
  @Config("druid.service")
  public abstract String getServiceName();

  @Config("druid.port")
  public abstract int getPort();

  @Config("druid.zk.service.host")
  public abstract String getZkHosts();

  @Config("druid.zk.paths.discoveryPath")
  public abstract String getDiscoveryPath();
}
