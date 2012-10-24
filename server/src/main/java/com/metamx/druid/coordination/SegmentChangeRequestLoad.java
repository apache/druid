package com.metamx.druid.coordination;

import com.metamx.druid.client.DataSegment;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonUnwrapped;

/**
 */
public class SegmentChangeRequestLoad implements DataSegmentChangeRequest
{
  private final DataSegment segment;

  @JsonCreator
  public SegmentChangeRequestLoad(
      @JsonUnwrapped DataSegment segment
  )
  {
    this.segment = segment;
  }

  @Override
  public void go(DataSegmentChangeHandler handler)
  {
    handler.addSegment(segment);
  }

  @JsonProperty
  @JsonUnwrapped
  @Override
  public DataSegment getSegment()
  {
    return segment;
  }

  @Override
  public String toString()
  {
    return "SegmentChangeRequestLoad{" +
           "segment=" + segment +
           '}';
  }
}
