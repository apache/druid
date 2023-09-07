package org.apache.druid.segment.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.segment.column.RowSignature;

import java.util.Objects;

/**
 * Encapsulates schema information of a dataSource.
 */
public class DataSourceSchema
{
  private final String datasource;
  private final RowSignature rowSignature;

  @JsonCreator
  public DataSourceSchema(
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

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DataSourceSchema that = (DataSourceSchema) o;
    return Objects.equals(datasource, that.datasource) && Objects.equals(
        rowSignature,
        that.rowSignature
    );
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(datasource, rowSignature);
  }

  @Override
  public String toString()
  {
    return "DataSourceSchema{" +
           "datasource='" + datasource + '\'' +
           ", rowSignature=" + rowSignature +
           '}';
  }
}
