package com.metamx.druid.client.cache;

import org.skife.config.Config;
import org.skife.config.Default;

public abstract class CacheConfig
{
  @Config("druid.bard.cache.type")
  @Default("local")
  public abstract String getType();
}
