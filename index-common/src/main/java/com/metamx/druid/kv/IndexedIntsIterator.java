package com.metamx.druid.kv;

import java.util.Iterator;

/**
 */
public class IndexedIntsIterator implements Iterator<Integer>
{
  private final IndexedInts baseInts;
  private final int size;

  int currIndex = 0;

  public IndexedIntsIterator(
      IndexedInts baseInts
  )
  {
    this.baseInts = baseInts;

    size = baseInts.size();
  }


  @Override
  public boolean hasNext()
  {
    return currIndex < size;
  }

  @Override
  public Integer next()
  {
    return baseInts.get(currIndex++);
  }

  @Override
  public void remove()
  {
    throw new UnsupportedOperationException();
  }
}
