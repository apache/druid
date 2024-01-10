package org.apache.druid.timeline;

import org.apache.druid.segment.column.SegmentSchema;

public class DataSegmentWithSchema
{
  private final DataSegment dataSegment;
  private final SegmentSchema segmentSchema;

  public DataSegmentWithSchema(DataSegment dataSegment, SegmentSchema segmentSchema)
  {
    this.dataSegment = dataSegment;
    this.segmentSchema = segmentSchema;
  }

  public DataSegment getDataSegment()
  {
    return dataSegment;
  }

  public SegmentSchema getSegmentSchema()
  {
    return segmentSchema;
  }
}
