package com.metamx.druid.client.cache;

/**
 */
public interface CacheBroker
{
  public CacheStats getStats();
  public Cache provideCache(String identifier);
}
