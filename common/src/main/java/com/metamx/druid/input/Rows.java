package com.metamx.druid.input;

import java.util.List;

/**
 */
public class Rows
{
  public static InputRow toInputRow(final Row row, final List<String> dimensions)
  {
    return new InputRow()
    {
      @Override
      public List<String> getDimensions()
      {
        return dimensions;
      }

      @Override
      public long getTimestampFromEpoch()
      {
        return row.getTimestampFromEpoch();
      }

      @Override
      public List<String> getDimension(String dimension)
      {
        return row.getDimension(dimension);
      }

      @Override
      public float getFloatMetric(String metric)
      {
        return row.getFloatMetric(metric);
      }
    };
  }
}
