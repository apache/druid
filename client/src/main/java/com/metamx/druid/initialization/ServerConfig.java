package com.metamx.druid.initialization;

import org.skife.config.Config;
import org.skife.config.Default;

/**
 */
public abstract class ServerConfig
{
  @Config("druid.port")
  public abstract int getPort();

  @Config("druid.http.numThreads")
  @Default("10")
  public abstract int getNumThreads();

  @Config("druid.http.maxIdleTimeMillis")
  public int getMaxIdleTimeMillis()
  {
    return 5 * 60 * 1000; // 5 minutes
  }
}
