package org.apache.druid.segment.column;


public class SegmentSchemaMetadata
{
  private final SchemaPayload schemaPayload;
  private final Long numRows;

  public SegmentSchemaMetadata(
      SchemaPayload schemaPayload,
      Long numRows
  )
  {
    this.numRows = numRows;
    this.schemaPayload = schemaPayload;
  }

  public SchemaPayload getSchemaPayload()
  {
    return schemaPayload;
  }

  public Long getNumRows()
  {
    return numRows;
  }
}
