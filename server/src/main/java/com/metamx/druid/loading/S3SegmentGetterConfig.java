package com.metamx.druid.loading;

import org.skife.config.Config;

import java.io.File;

/**
 */
public abstract class S3SegmentGetterConfig
{
  @Config("druid.paths.indexCache")
  public abstract File getCacheDirectory();
}
