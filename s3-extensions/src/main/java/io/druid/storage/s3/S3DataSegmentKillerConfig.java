package io.druid.storage.s3;

import com.fasterxml.jackson.annotation.JsonProperty;

public class S3DataSegmentKillerConfig
{
  @JsonProperty
  public boolean archive = true;

  @JsonProperty
  public String archiveBucket = "";

  public boolean isArchive()
  {
    return archive;
  }

  public String getArchiveBucket()
  {
    return archiveBucket;
  }
}
