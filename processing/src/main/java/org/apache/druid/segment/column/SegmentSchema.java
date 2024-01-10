package org.apache.druid.segment.column;


public class SegmentSchema
{
  String segmentId;
  Long numRows;
  SchemaPayload schemaPayload;

  public SegmentSchema(
      String segmentId,
      Long numRows,
      SchemaPayload schemaPayload)
  {
    this.segmentId = segmentId;
    this.numRows = numRows;
    this.schemaPayload = schemaPayload;
  }

  public String getSegmentId()
  {
    return segmentId;
  }

  public Long getNumRows()
  {
    return numRows;
  }

  public SchemaPayload getSchemaPayload()
  {
    return schemaPayload;
  }
}
