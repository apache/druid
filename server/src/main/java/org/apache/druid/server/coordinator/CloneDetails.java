package org.apache.druid.server.coordinator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class CloneDetails
{
  final String sourceServer;
  final long segmentsRemaining;
  final long bytesRemaining;

  @JsonCreator
  public CloneDetails(@JsonProperty String sourceServer, @JsonProperty long segmentsRemaining, @JsonProperty long bytesRemaining)
  {
    this.sourceServer = sourceServer;
    this.segmentsRemaining = segmentsRemaining;
    this.bytesRemaining = bytesRemaining;
  }

  @JsonProperty
  public String getSourceServer()
  {
    return sourceServer;
  }

  @JsonProperty
  public long getSegmentsRemaining()
  {
    return segmentsRemaining;
  }

  @JsonProperty
  public long getBytesRemaining()
  {
    return bytesRemaining;
  }

  public enum Status {
    CLONING
  }
}
