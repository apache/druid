package com.metamx.druid.merger.common.config;

import org.skife.config.Config;
import org.skife.config.DefaultNull;

public abstract class TaskLogConfig
{
  @Config("druid.merger.logs.s3bucket")
  @DefaultNull
  public abstract String getLogStorageBucket();

  @Config("druid.merger.logs.s3prefix")
  @DefaultNull
  public abstract String getLogStoragePrefix();
}
