package com.metamx.druid.merge;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.joda.time.Interval;

public class ClientDeleteQuery
{
  private final String dataSource;
  private final Interval interval;

  @JsonCreator
  public ClientDeleteQuery(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval
  )
  {
    this.dataSource = dataSource;
    this.interval = interval;
  }

  public String getDataSource()
  {
    return dataSource;
  }

  public Interval getInterval()
  {
    return interval;
  }

  @Override
  public String toString()
  {
    return "ClientDeleteQuery{" +
           "dataSource='" + dataSource + '\'' +
           ", interval=" + interval +
           '}';
  }
}
