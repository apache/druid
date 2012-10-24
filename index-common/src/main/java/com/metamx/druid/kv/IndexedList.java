package com.metamx.druid.kv;

import java.util.AbstractList;

/**
 */
public class IndexedList<T> extends AbstractList<T>
{
  public static <T> IndexedList<T> from(Indexed<T> indexed)
  {
    return new IndexedList<T>(indexed);
  }

  private final Indexed<T> base;

  public IndexedList(
      Indexed<T> base
  )
  {
    this.base = base;
  }

  @Override
  public T get(int index)
  {
    return base.get(index);
  }

  @Override
  public int size()
  {
    return base.size();
  }
}
