package com.metamx.druid.kv;

import com.google.common.collect.ImmutableList;

import java.util.Collections;
import java.util.Iterator;

/**
 */
public class EmptyIndexedInts implements IndexedInts
{
  public static EmptyIndexedInts instance = new EmptyIndexedInts();

  @Override
  public int size()
  {
    return 0;
  }

  @Override
  public int get(int index)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterator<Integer> iterator()
  {
    return ImmutableList.<Integer>of().iterator();
  }
}
