package com.metamx.druid.client.cache;

/**
 */
public interface CacheSerde<V>
{
  public byte[] serialize(V object);
  public V deserialize(byte[] bytes);
}
