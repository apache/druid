package com.metamx.druid.client;

import org.skife.config.Config;
import org.skife.config.Default;

/**
 */
public abstract class DruidServerConfig
{
  @Config("druid.host")
  public abstract String getServerName();

  @Config("druid.host")
  public abstract String getHost();

  @Config("druid.server.maxSize")
  public abstract long getMaxSize();

  @Config("druid.server.type")
  @Default("historical")
  public abstract String getType();
}
