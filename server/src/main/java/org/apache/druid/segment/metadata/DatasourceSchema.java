package org.apache.druid.segment.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.segment.column.RowSignature;

public class DatasourceSchema
{
  private final String datasource;
  private final RowSignature rowSignature;

  @JsonCreator
  public DatasourceSchema(
      @JsonProperty("datasource") String datasource,
      @JsonProperty("rowSignature") RowSignature rowSignature)
  {
    this.datasource = datasource;
    this.rowSignature = rowSignature;
  }

  @JsonProperty
  public String getDatasource()
  {
    return datasource;
  }

  @JsonProperty
  public RowSignature getRowSignature()
  {
    return rowSignature;
  }
}
