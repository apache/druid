package com.metamx.druid.client.cache;

import org.skife.config.Config;
import org.skife.config.Default;

/**
 */
public abstract class MapCacheBrokerConfig
{
  @Config("${prefix}.sizeInBytes")
  @Default("0")
  public abstract long getSizeInBytes();

  @Config("${prefix}.initialSize")
  public int getInitialSize()
  {
    return 500000;
  }

  @Config("${prefix}.logEvictionCount")
  public int getLogEvictionCount()
  {
    return 0;
  }
}
