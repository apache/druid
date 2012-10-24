package com.metamx.druid.collect;

import java.io.IOException;

/**
 */
public class StupidResourceHolder<T> implements ResourceHolder<T>
{
  private final T obj;

  public static <T> StupidResourceHolder create(T obj)
  {
    return new StupidResourceHolder(obj);
  }

  public StupidResourceHolder(
      T obj
  )
  {
    this.obj = obj;
  }

  @Override
  public T get()
  {
    return obj;
  }

  @Override
  public void close() throws IOException
  {
    // Do nothing
  }
}
