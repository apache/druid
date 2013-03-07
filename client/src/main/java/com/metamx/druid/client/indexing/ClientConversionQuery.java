package com.metamx.druid.client.indexing;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.druid.client.DataSegment;
import org.joda.time.Interval;

/**
 */
public class ClientConversionQuery
{
  private final String dataSource;
  private final Interval interval;
  private final DataSegment segment;

  public ClientConversionQuery(
      DataSegment segment
  )
  {
    this.dataSource = segment.getDataSource();
    this.interval = segment.getInterval();
    this.segment = segment;
  }

  public ClientConversionQuery(
      String dataSource,
      Interval interval
  )
  {
    this.dataSource = dataSource;
    this.interval = interval;
    this.segment = null;
  }

  @JsonProperty
  public String getType()
  {
    return "version_converter";
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public Interval getInterval()
  {
    return interval;
  }

  @JsonProperty
  public DataSegment getSegment()
  {
    return segment;
  }
}
