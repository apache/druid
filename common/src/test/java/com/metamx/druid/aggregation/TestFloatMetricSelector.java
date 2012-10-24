package com.metamx.druid.aggregation;


import com.metamx.druid.processing.FloatMetricSelector;

/**
 */
public class TestFloatMetricSelector implements FloatMetricSelector
{
  private final float[] floats;

  private int index = 0;

  public TestFloatMetricSelector(float[] floats)
  {
    this.floats = floats;
  }

  @Override
  public float get()
  {
    return floats[index];
  }

  public void increment()
  {
    ++index;
  }
}
