package com.metamx.druid.coordination;

import com.metamx.druid.client.DataSegment;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonUnwrapped;

/**
 */
public class SegmentChangeRequestDrop implements DataSegmentChangeRequest
{
  private final DataSegment segment;

  @JsonCreator
  public SegmentChangeRequestDrop(
      @JsonUnwrapped DataSegment segment
  )
  {
    this.segment = segment;
  }

  @JsonProperty
  @JsonUnwrapped
  @Override
  public DataSegment getSegment()
  {
    return segment;
  }

  @Override
  public void go(DataSegmentChangeHandler handler)
  {
    handler.removeSegment(segment);
  }

  @Override
  public String toString()
  {
    return "SegmentChangeRequestDrop{" +
           "segment=" + segment +
           '}';
  }
}
