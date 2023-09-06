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
  private boolean awaitInitializationOnStart = true;

  public boolean isMetadataSegmentCacheEnable()
  {
    return metadataSegmentCacheEnable;
  }

  public long getMetadataSegmentPollPeriod()
  {
    return metadataSegmentPollPeriod;
  }

  @Override
  public boolean isAwaitInitializationOnStart()
  {
    return awaitInitializationOnStart;
  }
}
