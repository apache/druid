package com.metamx.druid.index.v1;

import com.metamx.druid.index.v1.processing.Offset;
import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;
import it.uniroma3.mat.extendedset.intset.IntSet;

/**
 */
public class ConciseOffset implements Offset
{
  private static final int INVALID_VALUE = -1;

  IntSet.IntIterator itr;
  private final ImmutableConciseSet invertedIndex;
  private volatile int val;

  public ConciseOffset(ImmutableConciseSet invertedIndex)
  {
    this.invertedIndex = invertedIndex;
    this.itr = invertedIndex.iterator();
    increment();
  }

  private ConciseOffset(ConciseOffset otherOffset)
  {
    this.invertedIndex = otherOffset.invertedIndex;
    this.itr = otherOffset.itr.clone();
    this.val = otherOffset.val;
  }

  @Override
  public void increment()
  {
    if (itr.hasNext()) {
      val = itr.next();
    } else {
      val = INVALID_VALUE;
    }
  }

  @Override
  public boolean withinBounds()
  {
    return val > INVALID_VALUE;
  }

  @Override
  public Offset clone()
  {
    if (invertedIndex == null || invertedIndex.size() == 0) {
      return new ConciseOffset(new ImmutableConciseSet());
    }

    return new ConciseOffset(this);
  }

  @Override
  public int getOffset()
  {
    return val;
  }
}
