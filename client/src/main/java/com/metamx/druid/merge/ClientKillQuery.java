package com.metamx.druid.merge;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.Interval;

/**
 */
public class ClientKillQuery
{
  private final String dataSource;
  private final Interval interval;

  @JsonCreator
  public ClientKillQuery(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval
  )
  {
    this.dataSource = dataSource;
    this.interval = interval;
  }

  @JsonProperty
  public String getType()
  {
    return "kill";
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
}
