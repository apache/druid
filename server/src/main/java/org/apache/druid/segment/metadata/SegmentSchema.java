package org.apache.druid.segment.metadata;

public class SegmentSchema
{
  String fingerprint;
  String segmentId;
  Long numRows;
  SchemaPayload schemaPayload;

  public SegmentSchema(
      String fingerprint,
      String segmentId,
      Long numRows,
      SchemaPayload schemaPayload)
  {
    this.fingerprint = fingerprint;
    this.segmentId = segmentId;
    this.numRows = numRows;
    this.schemaPayload = schemaPayload;
  }

  public String getFingerprint()
  {
    return fingerprint;
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
