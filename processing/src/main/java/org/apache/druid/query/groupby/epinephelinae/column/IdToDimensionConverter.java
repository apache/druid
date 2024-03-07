package org.apache.druid.query.groupby.epinephelinae.column;

// Doesn't handle GROUP_BY_MISSING_VALUE, should be done by the callers
public interface IdToDimensionConverter<DimensionType>
{
  DimensionType idToKey(int id);

  boolean canCompareIds();
}
