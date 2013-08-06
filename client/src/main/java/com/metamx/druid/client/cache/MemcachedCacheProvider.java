package com.metamx.druid.client.cache;

public class MemcachedCacheProvider extends MemcachedCacheConfiger implements CacheProvider
{
  @Override
  public Cache get()
  {
    return MemcachedCache.create(this);
  }
}
