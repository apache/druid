package org.apache.druid.sql.calcite.schema;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.segment.metadata.SegmentMetadataCacheConfig;

public class BrokerSegmentMetadataCacheConfig extends SegmentMetadataCacheConfig
{
  @JsonProperty
  private boolean metadataSegmentCacheEnable = true;

  @JsonProperty
  private long metadataSegmentPollPeriod = 60000;

  @JsonProperty
  private boolean segmentMetadataCacheEnabled = false;

  public boolean isMetadataSegmentCacheEnable()
  {
    return metadataSegmentCacheEnable;
  }

  public long getMetadataSegmentPollPeriod()
  {
    return metadataSegmentPollPeriod;
  }

  public boolean isSegmentMetadataCacheEnabled()
  {
    return segmentMetadataCacheEnabled;
  }
}
