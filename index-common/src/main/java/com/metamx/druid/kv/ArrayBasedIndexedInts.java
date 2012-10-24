package com.metamx.druid.kv;

import java.util.Iterator;

/**
*/
public class ArrayBasedIndexedInts implements IndexedInts
{
  private final int[] expansion;

  public ArrayBasedIndexedInts(int[] expansion) {this.expansion = expansion;}

  @Override
  public int size()
  {
    return expansion.length;
  }

  @Override
  public int get(int index)
  {
    return expansion[index];
  }

  @Override
  public Iterator<Integer> iterator()
  {
    return new IndexedIntsIterator(this);
  }
}
