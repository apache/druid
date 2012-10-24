package com.metamx.druid.result;

import com.metamx.druid.query.search.SearchHit;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonValue;

import java.util.List;

/**
 */
public class BySegmentSearchResultValue extends SearchResultValue implements BySegmentResultValue<SearchResultValue>
{
  private final List<Result<SearchResultValue>> results;
  private final String segmentId;
  private final String intervalString;

  public BySegmentSearchResultValue(
      @JsonProperty("results") List<Result<SearchResultValue>> results,
      @JsonProperty("segment") String segmentId,
      @JsonProperty("interval") String intervalString
  )
  {
    super(null);

    this.results = results;
    this.segmentId = segmentId;
    this.intervalString = intervalString;
  }

  @Override
  @JsonValue(false)
  public List<SearchHit> getValue()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  @JsonProperty("results")
  public List<Result<SearchResultValue>> getResults()
  {
    return results;
  }

  @Override
  @JsonProperty("segment")
  public String getSegmentId()
  {
    return segmentId;
  }

  @Override
  @JsonProperty("interval")
  public String getIntervalString()
  {
    return intervalString;
  }

  @Override
  public String toString()
  {
    return "BySegmentSearchResultValue{" +
           "results=" + results +
           ", segmentId='" + segmentId + '\'' +
           ", intervalString='" + intervalString + '\'' +
           '}';
  }
}
