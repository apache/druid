package com.metamx.druid.processing;

import java.io.Closeable;

/**
 * Factory class for MetricSelectors
 */
public interface MetricSelectorFactory
{
  public FloatMetricSelector makeFloatMetricSelector(String metricName);
  public ComplexMetricSelector makeComplexMetricSelector(String metricName);
}
