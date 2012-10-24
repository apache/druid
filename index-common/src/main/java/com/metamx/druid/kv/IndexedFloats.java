package com.metamx.druid.kv;

import java.io.Closeable;

/**
 * Get a float at an index (array or list lookup abstraction without boxing).
 */
public interface IndexedFloats extends Closeable
{
  public int size();
  public float get(int index);
  public void fill(int index, float[] toFill);
}
