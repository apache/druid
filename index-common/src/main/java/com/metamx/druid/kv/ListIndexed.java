package com.metamx.druid.kv;

import java.util.Iterator;
import java.util.List;

/**
 */
public class ListIndexed<T> implements Indexed<T>
{
  private final List<T> baseList;
  private final Class<? extends T> clazz;

  public ListIndexed(
      List<T> baseList,
      Class<? extends T> clazz
  )
  {
    this.baseList = baseList;
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
    return baseList.size();
  }

  @Override
  public T get(int index)
  {
    return baseList.get(index);
  }

  @Override
  public int indexOf(T value)
  {
    return baseList.indexOf(value);
  }

  @Override
  public Iterator<T> iterator()
  {
    return baseList.iterator();
  }
}
