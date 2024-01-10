package org.apache.druid.timeline;

import org.apache.druid.segment.column.SchemaPayloadWithNumRows;

public class DataSegmentWithSchema
{
  private final DataSegment dataSegment;
  private final SchemaPayloadWithNumRows segmentSchema;

  public DataSegmentWithSchema(DataSegment dataSegment, SchemaPayloadWithNumRows segmentSchema)
  {
    this.dataSegment = dataSegment;
    this.segmentSchema = segmentSchema;
  }

  public DataSegment getDataSegment()
  {
    return dataSegment;
  }

  public SchemaPayloadWithNumRows getSegmentSchema()
  {
    return segmentSchema;
  }
}
