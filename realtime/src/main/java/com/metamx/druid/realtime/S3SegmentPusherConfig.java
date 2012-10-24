package com.metamx.druid.realtime;

import org.skife.config.Config;
import org.skife.config.Default;

/**
 */
public abstract class S3SegmentPusherConfig
{

  @Config("druid.pusher.s3.bucket")
  public abstract String getBucket();

  @Config("druid.pusher.s3.baseKey")
  @Default("")
  public abstract String getBaseKey();
}
