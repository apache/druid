package com.metamx.druid.loading;

import org.skife.config.Config;

/**
 */
public abstract class QueryableLoaderConfig extends S3SegmentGetterConfig
{
  @Config("druid.queryable.factory")
  public String getQueryableFactoryType()
  {
    return "mmap";
  }
}
