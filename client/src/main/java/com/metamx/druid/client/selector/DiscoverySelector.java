package com.metamx.druid.client.selector;

/**
 */
public interface DiscoverySelector<T>
{
  public T pick();
}
