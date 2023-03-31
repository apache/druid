package org.apache.druid.timeline;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;

public class SegmentWithOvershadowedStatusChangeRequest
{
  @JsonUnwrapped
  private final SegmentWithOvershadowedStatus segmentWithOvershadowedStatus;

  private final boolean load;

  @JsonCreator
  public SegmentWithOvershadowedStatusChangeRequest(
      @JsonProperty("load") boolean load
  )
  {
    this(null, load);
  }

  public SegmentWithOvershadowedStatusChangeRequest(
      SegmentWithOvershadowedStatus segmentWithOvershadowedStatus,
      boolean load
  )
  {
    this.segmentWithOvershadowedStatus = segmentWithOvershadowedStatus;
    this.load = load;
  }

  @JsonProperty
  public SegmentWithOvershadowedStatus getSegmentWithOvershadowedStatus()
  {
    return segmentWithOvershadowedStatus;
  }

  @JsonProperty
  public boolean isLoad()
  {
    return load;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SegmentWithOvershadowedStatusChangeRequest)) {
      return false;
    }
    final SegmentWithOvershadowedStatusChangeRequest that = (SegmentWithOvershadowedStatusChangeRequest) o;
    if (!segmentWithOvershadowedStatus.equals(that.segmentWithOvershadowedStatus)) {
      return false;
    }
    return load == (that.load);
  }

  @Override
  public int hashCode()
  {
    int result = segmentWithOvershadowedStatus.hashCode();
    result = 31 * result + Boolean.hashCode(load);
    return result;
  }

  @Override
  public String toString()
  {
    return "SegmentWithOvershadowedStatusChangeRequest{" +
           "load=" + load +
           ", segmentWithOvershadowedStatus=" + segmentWithOvershadowedStatus +
           '}';
  }
}
