package com.metamx.druid.index.v1;

/**
 */
public class DimensionColumn
{
  final int[][] dimensionLookups;
  final int[] dimensionValues;

  public DimensionColumn(
      int[][] dimensionLookups,
      int[] dimensionValues
  )
  {
    this.dimensionLookups = dimensionLookups;
    this.dimensionValues = dimensionValues;
  }

  public int[] getDimValues(int rowId)
  {
    return dimensionLookups[dimensionValues[rowId]];
  }

  public int[][] getDimensionExpansions()
  {
    return dimensionLookups;
  }

  public int[] getDimensionRowValues()
  {
    return dimensionValues;
  }
}
