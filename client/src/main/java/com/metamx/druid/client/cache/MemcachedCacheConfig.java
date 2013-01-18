package com.metamx.druid.client.cache;

import org.skife.config.Config;
import org.skife.config.Default;

public abstract class MemcachedCacheConfig
{
  @Config("${prefix}.expiration")
  @Default("31536000")
  public abstract int getExpiration();

  @Config("${prefix}.timeout")
  @Default("500")
  public abstract int getTimeout();

  @Config("${prefix}.hosts")
  public abstract String getHosts();

  @Config("${prefix}.maxObjectSize")
  public abstract int getMaxObjectSize();
}
