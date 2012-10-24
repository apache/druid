package com.metamx.druid.collect;

import java.io.Closeable;

/**
 */
public interface ResourceHolder<T> extends Closeable
{
  public T get();
}
