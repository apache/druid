package com.metamx.druid.index.v1;

import com.metamx.druid.index.v1.processing.Offset;
import com.metamx.druid.kv.IndexedInts;

/**
*/
class IndexedIntsOffset implements Offset
{
  int currRow;
  private final IndexedInts invertedIndex;

  public IndexedIntsOffset(IndexedInts invertedIndex)
  {
    this.invertedIndex = invertedIndex;
    currRow = 0;
  }

  @Override
  public void increment()
  {
    ++currRow;
  }

  @Override
  public boolean withinBounds()
  {
    return currRow < invertedIndex.size();
  }

  @Override
  public Offset clone()
  {
    final IndexedIntsOffset retVal = new IndexedIntsOffset(invertedIndex);
    retVal.currRow = currRow;
    return retVal;
  }

  @Override
  public int getOffset()
  {
    return invertedIndex.get(currRow);
  }
}
