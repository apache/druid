package org.apache.druid.msq.statistics;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Set;

/**
 * Class sent by worker to controller after reading input to generate partition boundries.
 */
public class PartialKeyStatisticsInformation
{
  private final Set<Long> timeSegments;

  private final boolean hasMultipleValues;

  private final double bytesRetained;

  @JsonCreator
  public PartialKeyStatisticsInformation(
      @JsonProperty("timeSegments") Set<Long> timeSegments,
      @JsonProperty("hasMultipleValues") boolean hasMultipleValues,
      @JsonProperty("bytesRetained") double bytesRetained
  )
  {
    this.timeSegments = timeSegments;
    this.hasMultipleValues = hasMultipleValues;
    this.bytesRetained = bytesRetained;
  }

  @JsonProperty("timeSegments")
  public Set<Long> getTimeSegments()
  {
    return timeSegments;
  }

  @JsonProperty("hasMultipleValues")
  public boolean isHasMultipleValues()
  {
    return hasMultipleValues;
  }

  @JsonProperty("bytesRetained")
  public double getBytesRetained()
  {
    return bytesRetained;
  }
}
