package com.metamx.druid.http;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 */
public class SegmentToDrop
{
  private final String fromServer;
  private final String segmentName;

  @JsonCreator
  public SegmentToDrop(
      @JsonProperty("from") String fromServer,
      @JsonProperty("segmentName") String segmentName
  )
  {
    this.fromServer = fromServer;
    this.segmentName = segmentName;
  }

  public String getFromServer()
  {
    return fromServer;
  }

  public String getSegmentName()
  {
    return segmentName;
  }
}
