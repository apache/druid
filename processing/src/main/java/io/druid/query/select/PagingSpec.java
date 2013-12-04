package io.druid.query.select;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

/**
 */
public class PagingSpec
{
  private final int start;
  private final int end;

  @JsonCreator
  public PagingSpec(
      @JsonProperty("start") int start,
      @JsonProperty("end") int end
  )
  {
    Preconditions.checkArgument(end > start, "end must be greater than start");

    this.start = start;
    this.end = end;
  }

  @JsonProperty
  public int getStart()
  {
    return start;
  }

  @JsonProperty
  public int getEnd()
  {
    return end;
  }

  public int getThreshold()
  {
    return end - start;
  }
}
