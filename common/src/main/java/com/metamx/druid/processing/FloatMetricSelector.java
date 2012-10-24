package com.metamx.druid.processing;

/**
 * An object that gets a metric value.  Metric values are always floats and there is an assumption that the
 * FloatMetricSelector has a handle onto some other stateful object (e.g. an Offset) which is changing between calls
 * to get() (though, that doesn't have to be the case if you always want the same value...).
 */
public interface FloatMetricSelector
{
  public float get();
}
