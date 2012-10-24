package com.metamx.druid.index.v1.serde;

import com.metamx.druid.input.InputRow;

/**
 */
public interface ComplexMetricExtractor
{
  public Class<?> extractedClass();
  public Object extractValue(InputRow inputRow, String metricName);
}
