package com.metamx.druid.indexing.common.config;

import org.skife.config.Config;
import org.skife.config.DefaultNull;

public abstract class TaskLogConfig
{
  @Config("druid.indexer.logs.s3bucket")
  @DefaultNull
  public abstract String getLogStorageBucket();

  @Config("druid.indexer.logs.s3prefix")
  @DefaultNull
  public abstract String getLogStoragePrefix();
}
