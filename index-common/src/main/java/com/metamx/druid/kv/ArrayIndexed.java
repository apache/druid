package com.metamx.druid.kv;

import java.util.Arrays;
import java.util.Iterator;

/**
 */
public class ArrayIndexed<T> implements Indexed<T>
{
  private final T[] baseArray;
  private final Class<? extends T> clazz;

  public ArrayIndexed(
      T[] baseArray,
      Class<? extends T> clazz
  )
  {
    this.baseArray = baseArray;
    this.clazz = clazz;
  }

  @Override
  public Class<? extends T> getClazz()
  {
    return clazz;
  }

  @Override
  public int size()
  {
    return baseArray.length;
  }

  @Override
  public T get(int index)
  {
    return baseArray[index];
  }

  @Override
  public int indexOf(T value)
  {
    return Arrays.asList(baseArray).indexOf(value);
  }

  @Override
  public Iterator<T> iterator()
  {
    return Arrays.asList(baseArray).iterator();
  }
}
