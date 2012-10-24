package com.metamx.druid.kv;

/**
 * Get a int an index (array or list lookup abstraction without boxing).
 * Typically wraps an {@link com.metamx.druid.kv.Indexed}.
 */
public interface IndexedInts extends Iterable<Integer>
{
  int size();
  int get(int index);
}
