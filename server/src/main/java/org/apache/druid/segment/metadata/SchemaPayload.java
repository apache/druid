package org.apache.druid.segment.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.column.RowSignature;

import java.util.Map;

public class SchemaPayload
{
  RowSignature rowSignature;
  Map<String, AggregatorFactory> aggregatorFactories;

  @JsonCreator
  public SchemaPayload(
      @JsonProperty("rowSignature") RowSignature rowSignature,
      @JsonProperty("aggreagatorFactories") Map<String, AggregatorFactory> aggregatorFactories)
  {
    this.rowSignature = rowSignature;
    this.aggregatorFactories = aggregatorFactories;
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
}
