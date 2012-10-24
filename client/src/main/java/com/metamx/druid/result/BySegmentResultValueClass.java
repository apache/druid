package com.metamx.druid.result;

import org.codehaus.jackson.annotate.JsonProperty;

import java.util.List;

/**
 */
public class BySegmentResultValueClass<T>
{
  private final List<T> results;
  private final String segmentId;
  private final String intervalString;

  public BySegmentResultValueClass(
      @JsonProperty("results") List<T> results,
      @JsonProperty("segment") String segmentId,
      @JsonProperty("interval") String intervalString
  )
  {
    this.results = results;
    this.segmentId = segmentId;
    this.intervalString = intervalString;
  }

  @JsonProperty("results")
  public List<T> getResults()
  {
    return results;
  }

  @JsonProperty("segment")
  public String getSegmentId()
  {
    return segmentId;
  }

  @JsonProperty("interval")
  public String getIntervalString()
  {
    return intervalString;
  }

  @Override
  public String toString()
  {
    return "BySegmentTimeseriesResultValue{" +
           "results=" + results +
           ", segmentId='" + segmentId + '\'' +
           ", intervalString='" + intervalString + '\'' +
           '}';
  }
}
