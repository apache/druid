package org.apache.druid.segment.column;


public class SchemaPayloadWithNumRows
{
  Long numRows;
  SchemaPayload schemaPayload;

  public SchemaPayloadWithNumRows(
      String segmentId,
      Long numRows,
      SchemaPayload schemaPayload)
  {
    this.numRows = numRows;
    this.schemaPayload = schemaPayload;
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
