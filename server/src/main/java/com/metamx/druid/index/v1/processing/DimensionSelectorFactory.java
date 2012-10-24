package com.metamx.druid.index.v1.processing;

/**
 */
public interface DimensionSelectorFactory
{
  public DimensionSelector makeDimensionSelector(String dimensionName);
}
