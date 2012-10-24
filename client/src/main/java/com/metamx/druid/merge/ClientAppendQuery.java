package com.metamx.druid.merge;

import com.metamx.druid.client.DataSegment;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.List;

/**
 */
public class ClientAppendQuery implements ClientMergeQuery
{
  private final String dataSource;
  private final List<DataSegment> segments;

  @JsonCreator
  public ClientAppendQuery(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("segments") List<DataSegment> segments
  )
  {
    this.dataSource = dataSource;
    this.segments = segments;
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

  @Override
  public String toString()
  {
    return "ClientAppendQuery{" +
           "dataSource='" + dataSource + '\'' +
           ", segments=" + segments +
           '}';
  }
}
