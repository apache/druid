package com.metamx.druid.query.segment;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.joda.time.Interval;

/**
*/
public class SegmentDescriptor
{
  private final Interval interval;
  private final String version;
  private final int partitionNumber;

  @JsonCreator
  public SegmentDescriptor(
      @JsonProperty("itvl") Interval interval,
      @JsonProperty("ver") String version,
      @JsonProperty("part") int partitionNumber)
  {
    this.interval = interval;
    this.version = version;
    this.partitionNumber = partitionNumber;
  }

  @JsonProperty("itvl")
  public Interval getInterval()
  {
    return interval;
  }

  @JsonProperty("ver")
  public String getVersion()
  {
    return version;
  }

  @JsonProperty("part")
  public int getPartitionNumber()
  {
    return partitionNumber;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SegmentDescriptor that = (SegmentDescriptor) o;

    if (partitionNumber != that.partitionNumber) {
      return false;
    }
    if (interval != null ? !interval.equals(that.interval) : that.interval != null) {
      return false;
    }
    if (version != null ? !version.equals(that.version) : that.version != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = interval != null ? interval.hashCode() : 0;
    result = 31 * result + (version != null ? version.hashCode() : 0);
    result = 31 * result + partitionNumber;
    return result;
  }

  @Override
  public String toString()
  {
    return "SegmentDescriptor{" +
           "interval=" + interval +
           ", version='" + version + '\'' +
           ", partitionNumber=" + partitionNumber +
           '}';
  }
}
