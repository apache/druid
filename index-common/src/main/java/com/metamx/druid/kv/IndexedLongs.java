package com.metamx.druid.kv;

import java.io.Closeable;

/**
 * Get a long at an index (array or list lookup abstraction without boxing).
 */
public interface IndexedLongs extends Closeable
{
  public int size();
  public long get(int index);
  public void fill(int index, long[] toFill);
  int binarySearch(long key);
  int binarySearch(long key, int from, int to);
}
