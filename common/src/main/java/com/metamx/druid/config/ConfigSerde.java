package com.metamx.druid.config;

/**
*/
public interface ConfigSerde<T>
{
  public byte[] serialize(T obj);
  public T deserialize(byte[] bytes);
}
