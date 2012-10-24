package com.metamx.druid.index.v1.processing;

/**
 */
public class StartLimitedOffset implements Offset
{
  private final Offset baseOffset;
  private final int limit;

  public StartLimitedOffset(
      Offset baseOffset,
      int limit
  )
  {
    this.baseOffset = baseOffset;
    this.limit = limit;

    while (baseOffset.withinBounds() && baseOffset.getOffset() < limit) {
      baseOffset.increment();
    }
  }

  @Override
  public void increment()
  {
    baseOffset.increment();
  }

  @Override
  public boolean withinBounds()
  {
    return baseOffset.withinBounds();
  }

  @Override
  public Offset clone()
  {
    return new StartLimitedOffset(baseOffset.clone(), limit);
  }

  @Override
  public int getOffset()
  {
    return baseOffset.getOffset();
  }
}
