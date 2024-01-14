package org.apache.druid.timeline;

import org.apache.druid.segment.column.SegmentSchemaMetadata;

public class DataSegmentWithSchema
{
  private final DataSegment dataSegment;
  private final SegmentSchemaMetadata segmentSchema;

  public DataSegmentWithSchema(DataSegment dataSegment, SegmentSchemaMetadata segmentSchema)
  {
    this.dataSegment = dataSegment;
    this.segmentSchema = segmentSchema;
  }

  public DataSegment getDataSegment()
  {
    return dataSegment;
  }

  public SegmentSchemaMetadata getSegmentSchema()
  {
    return segmentSchema;
  }
}
