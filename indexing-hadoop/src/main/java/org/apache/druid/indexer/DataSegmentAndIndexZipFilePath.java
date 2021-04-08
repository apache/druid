package org.apache.druid.indexer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.timeline.DataSegment;

import java.util.Objects;

public class DataSegmentAndIndexZipFilePath
{
  private final DataSegment segment;
  private final String tmpIndexZipFilePath;
  private final String finalIndexZipFilePath;

  @JsonCreator
  public DataSegmentAndIndexZipFilePath(
      @JsonProperty("segment") DataSegment segment,
      @JsonProperty("tmpIndexZipFilePath") String tmpIndexZipFilePath,
      @JsonProperty("finalIndexZipFilePath") String finalIndexZipFilePath
  )
  {
    this.segment = segment;
    this.tmpIndexZipFilePath = tmpIndexZipFilePath;
    this.finalIndexZipFilePath = finalIndexZipFilePath;
  }

  @JsonProperty
  public DataSegment getSegment()
  {
    return segment;
  }

  @JsonProperty
  public String getTmpIndexZipFilePath()
  {
    return tmpIndexZipFilePath;
  }

  @JsonProperty
  public String getFinalIndexZipFilePath()
  {
    return finalIndexZipFilePath;
  }

  @Override
  public boolean equals(Object o)
  {
    if (o instanceof DataSegmentAndIndexZipFilePath) {
      return segment.equals(((DataSegmentAndIndexZipFilePath) o).getSegment());
    }
    return false;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(segment.getId(), tmpIndexZipFilePath);
  }

  @Override
  public String toString()
  {
    return "DataSegmentAndIndexZipFilePath{" +
           "segment=" + segment +
           ", tmpIndexZipFilePath=" + tmpIndexZipFilePath +
           ", finalIndexZipFilePath=" + finalIndexZipFilePath +
           '}';
  }
}
