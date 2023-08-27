package org.apache.druid.segment.metadata;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.Period;

public class SegmentMetadataCacheConfig
{
  @JsonProperty
  private boolean awaitInitializationOnStart = true;

  @JsonProperty
  private Period metadataRefreshPeriod = new Period("PT1M");

  @JsonProperty
  private SegmentMetadataCache.ColumnTypeMergePolicy metadataColumnTypeMergePolicy =
      new SegmentMetadataCache.LeastRestrictiveTypeMergePolicy();

  public static SegmentMetadataCacheConfig create()
  {
    return new SegmentMetadataCacheConfig();
  }

  public static SegmentMetadataCacheConfig create(
      String metadataRefreshPeriod
  )
  {
    SegmentMetadataCacheConfig config = new SegmentMetadataCacheConfig();
    config.metadataRefreshPeriod = new Period(metadataRefreshPeriod);
    return config;
  }

  public boolean isAwaitInitializationOnStart()
  {
    return awaitInitializationOnStart;
  }

  public SegmentMetadataCache.ColumnTypeMergePolicy getMetadataColumnTypeMergePolicy()
  {
    return metadataColumnTypeMergePolicy;
  }

  public Period getMetadataRefreshPeriod()
  {
    return metadataRefreshPeriod;
  }
}
