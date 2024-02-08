package org.apache.druid.segment.column;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.query.aggregation.AggregatorFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class SchemaPayload
{
  private final RowSignature rowSignature;
  private final Map<String, AggregatorFactory> aggregatorFactories;

  @JsonCreator
  public SchemaPayload(
      @JsonProperty("rowSignature") RowSignature rowSignature,
      @JsonProperty("aggreagatorFactories") Map<String, AggregatorFactory> aggregatorFactories)
  {
    this.rowSignature = rowSignature;
    this.aggregatorFactories = aggregatorFactories;
  }

  public SchemaPayload(RowSignature rowSignature)
  {
    this.rowSignature = rowSignature;
    this.aggregatorFactories = new HashMap<>();
  }

  @JsonProperty
  public RowSignature getRowSignature()
  {
    return rowSignature;
  }

  @JsonProperty
  public Map<String, AggregatorFactory> getAggregatorFactories()
  {
    return aggregatorFactories;
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
    SchemaPayload that = (SchemaPayload) o;
    return Objects.equals(rowSignature, that.rowSignature) && Objects.equals(
        aggregatorFactories,
        that.aggregatorFactories
    );
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(rowSignature, aggregatorFactories);
  }

  @Override
  public String toString()
  {
    return "SchemaPayload{" +
           "rowSignature=" + rowSignature +
           ", aggregatorFactories=" + aggregatorFactories +
           '}';
  }
}
