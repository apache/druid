package org.apache.druid.segment.column;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SegmentSchemaMetadata
{
  private final SchemaPayload schemaPayload;
  private final Long numRows;

  @JsonCreator
  public SegmentSchemaMetadata(
      @JsonProperty("schemaPayload") SchemaPayload schemaPayload,
      @JsonProperty("numRows") Long numRows
  )
  {
    this.numRows = numRows;
    this.schemaPayload = schemaPayload;
  }

  @JsonProperty
  public SchemaPayload getSchemaPayload()
  {
    return schemaPayload;
  }

  @JsonProperty
  public Long getNumRows()
  {
    return numRows;
  }

  @Override
  public String toString()
  {
    return "SegmentSchemaMetadata{" +
           "schemaPayload=" + schemaPayload +
           ", numRows=" + numRows +
           '}';
  }
}
