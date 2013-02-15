package com.metamx.druid.client.cache;

import org.skife.config.Config;
import org.skife.config.Default;

public abstract class MemcachedCacheConfig
{
  @Config("${prefix}.expiration")
  @Default("2592000")
  public abstract int getExpiration();

  @Config("${prefix}.timeout")
  @Default("500")
  public abstract int getTimeout();

  @Config("${prefix}.hosts")
  public abstract String getHosts();

  @Config("${prefix}.maxObjectSize")
  @Default("52428800")
  public abstract int getMaxObjectSize();

  @Config("${prefix}.memcachedPrefix")
  @Default("druid")
  public abstract String getMemcachedPrefix();
}
