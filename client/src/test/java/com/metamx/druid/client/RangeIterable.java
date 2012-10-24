package com.metamx.druid.client;

import java.util.Iterator;

/**
 */
public class RangeIterable implements Iterable<Integer>
{
  private final int startValue;
  private final int endValue;
  private final int increment;

  public RangeIterable(int endValue)
  {
    this(0, endValue);
  }

  public RangeIterable(int startValue, int endValue)
  {
    this(startValue, endValue, 1);
  }

  public RangeIterable(int startValue, int endValue, int increment)
  {
    this.startValue = startValue;
    this.endValue = endValue;
    this.increment = increment;
  }

  @Override
  public Iterator<Integer> iterator()
  {
    return new Iterator<Integer>()
    {
      int value = startValue;

      @Override
      public boolean hasNext()
      {
        return value < endValue;
      }

      @Override
      public Integer next()
      {
        try {
          return value;
        }
        finally {
          value += increment;
        }
      }

      @Override
      public void remove()
      {
        throw new UnsupportedOperationException();
      }
    };
  }
}
