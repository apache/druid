package com.metamx.druid.merge;

import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.client.DataSegment;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.List;

/**
 */
public class ClientDefaultMergeQuery implements ClientMergeQuery
{
  private final String dataSource;
  private final List<DataSegment> segments;
  private final List<AggregatorFactory> aggregators;

  @JsonCreator
  public ClientDefaultMergeQuery(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("segments") List<DataSegment> segments,
      @JsonProperty("aggregations") List<AggregatorFactory> aggregators
  )
  {
    this.dataSource = dataSource;
    this.segments = segments;
    this.aggregators = aggregators;

  }

  @JsonProperty
  @Override
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  @Override
  public List<DataSegment> getSegments()
  {
    return segments;
  }

  @JsonProperty
  public List<AggregatorFactory> getAggregators()
  {
    return aggregators;
  }

  @Override
  public String toString()
  {
    return "ClientDefaultMergeQuery{" +
           "dataSource='" + dataSource + '\'' +
           ", segments=" + segments +
           ", aggregators=" + aggregators +
           '}';
  }
}
