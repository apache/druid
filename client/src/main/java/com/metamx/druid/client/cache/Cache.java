package com.metamx.druid.client.cache;

/**
 * An interface to limit the operations that can be done on a Cache so that it is easier to reason about what
 * is actually going to be done.
 */
public interface Cache
{
  public byte[] get(byte[] key);
  public byte[] put(byte[] key, byte[] value);
  public void close();
}
