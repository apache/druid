package com.metamx.druid.kv;

import java.nio.ByteBuffer;
import java.util.Comparator;

/**
*/
public interface ObjectStrategy<T> extends Comparator<T>
{
  public Class<? extends T> getClazz();
  public T fromByteBuffer(ByteBuffer buffer, int numBytes);
  public byte[] toBytes(T val);
}
