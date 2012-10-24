package com.metamx.druid.http;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 */
public class SegmentToMove
{
  private final String fromServer;
  private final String toServer;
  private final String segmentName;

  @JsonCreator
  public SegmentToMove(
      @JsonProperty("from") String fromServer,
      @JsonProperty("to") String toServer,
      @JsonProperty("segmentName") String segmentName
  )
  {
    this.fromServer = fromServer;
    this.toServer = toServer;
    this.segmentName = segmentName;
  }

  public String getFromServer()
  {
    return fromServer;
  }

  public String getToServer()
  {
    return toServer;
  }

  public String getSegmentName()
  {
    return segmentName;
  }
}
