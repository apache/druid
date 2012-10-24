package com.metamx.druid.indexer.rollup;

import com.metamx.common.Granularity;
import com.metamx.druid.QueryGranularity;
import com.metamx.druid.aggregation.AggregatorFactory;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.List;

/**
 * Class uses public fields to work around http://jira.codehaus.org/browse/MSHADE-92
 *
 * Adjust to JsonCreator and final fields when resolved.
 */
public class DataRollupSpec
{
  @JsonProperty
  public List<AggregatorFactory> aggs;

  @JsonProperty
  public QueryGranularity rollupGranularity;

  @JsonProperty
  public int rowFlushBoundary = 500000;

  public DataRollupSpec() {}

  public DataRollupSpec(List<AggregatorFactory> aggs, QueryGranularity rollupGranularity)
  {
    this.aggs = aggs;
    this.rollupGranularity = rollupGranularity;
  }

  public List<AggregatorFactory> getAggs()
  {
    return aggs;
  }

  public QueryGranularity getRollupGranularity()
  {
    return rollupGranularity;
  }
}
