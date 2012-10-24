package com.metamx.druid.index.v1.processing;

/**
 */
public class ArrayBasedOffset implements Offset
{
  private final int[] ints;
  private int currIndex;

  public ArrayBasedOffset(
      int[] ints
  )
  {
    this(ints, 0);
  }

  public ArrayBasedOffset(
      int[] ints,
      int startIndex
  )
  {
    this.ints = ints;
    this.currIndex = startIndex;
  }

  @Override
  public int getOffset()
  {
    return ints[currIndex];
  }

  @Override
  public void increment()
  {
    ++currIndex;
  }

  @Override
  public boolean withinBounds()
  {
    return currIndex < ints.length;
  }

  @Override
  public Offset clone()
  {
    final ArrayBasedOffset retVal = new ArrayBasedOffset(ints);
    retVal.currIndex = currIndex;
    return retVal;
  }
}
