package org.apache.druid.indexer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.timeline.DataSegment;

import java.util.Objects;

public class DataSegmentAndTmpPath
{
  private final DataSegment segment;
  private final String indexZipFilePath;

  @JsonCreator
  public DataSegmentAndTmpPath(
      @JsonProperty("segment") DataSegment segment,
      @JsonProperty("indexZipFilePath") String indexZipFilePath
  )
  {
    this.segment = segment;
    this.indexZipFilePath = indexZipFilePath;
  }

  @JsonProperty
  public DataSegment getSegment()
  {
    return segment;
  }

  @JsonProperty
  public String getIndexZipFilePath()
  {
    return indexZipFilePath;
  }

  @Override
  public boolean equals(Object o)
  {
    if (o instanceof DataSegmentAndTmpPath) {
      return segment.getId().equals(((DataSegmentAndTmpPath) o).getSegment().getId());
    }
    return false;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(segment.getId(), indexZipFilePath);
  }

  @Override
  public String toString()
  {
    return "DataSegmentAndTmpPath{" +
           "segment=" + segment +
           ", indexZipFilePath=" + indexZipFilePath +
           '}';
  }
}
