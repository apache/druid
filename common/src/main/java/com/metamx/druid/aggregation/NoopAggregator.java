package com.metamx.druid.aggregation;

/**
 */
public class NoopAggregator implements Aggregator
{
  private final String name;

  public NoopAggregator(
      String name
  )
  {
    this.name = name;
  }

  @Override
  public void aggregate()
  {
  }

  @Override
  public void reset()
  {
  }

  @Override
  public Object get()
  {
    return null;
  }

  @Override
  public float getFloat()
  {
    return 0;
  }

  @Override
  public String getName()
  {
    return name;
  }
}
