package com.metamx.druid.kv;

import com.google.common.collect.Iterators;

import java.util.Iterator;

/**
*/
public class SingleIndexedInts implements IndexedInts
{
  private final int value;

  public SingleIndexedInts(int value) {
    this.value = value;
  }

  @Override
  public int size()
  {
    return 1;
  }

  @Override
  public int get(int index)
  {
    return value;
  }

  @Override
  public Iterator<Integer> iterator()
  {
    return Iterators.singletonIterator(value);
  }
}
