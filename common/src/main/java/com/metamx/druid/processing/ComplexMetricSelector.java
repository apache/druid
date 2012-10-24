package com.metamx.druid.processing;

/**
 */
public interface ComplexMetricSelector<T>
{
  public Class<T> classOfObject();
  public T get();
}
