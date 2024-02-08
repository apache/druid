package org.apache.druid.segment.column;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

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
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SegmentSchemaMetadata that = (SegmentSchemaMetadata) o;
    return Objects.equals(schemaPayload, that.schemaPayload) && Objects.equals(numRows, that.numRows);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(schemaPayload, numRows);
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
