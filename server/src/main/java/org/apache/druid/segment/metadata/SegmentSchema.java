package org.apache.druid.segment.metadata;

import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.column.RowSignature;

import java.util.Map;

public class SegmentSchema
{
  String schemaId;
  String segmentId;
  RowSignature rowSignature;
  Long numRows;
  Map<String, AggregatorFactory> aggregatorFactoryMap;

  public RowSignature getRowSignature()
  {
    return rowSignature;
  }
}
