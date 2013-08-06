package com.metamx.druid.client.cache;

public class MemcachedCacheProvider extends MemcachedCacheConfig implements CacheProvider
{
  @Override
  public Cache get()
  {
    return MemcachedCache.create(this);
  }
}
