package com.metamx.druid.merger.worker.config;

import org.skife.config.Config;
import org.skife.config.Default;

/**
 */
public abstract class WorkerConfig
{
  @Config("druid.merger.threads")
  @Default("1")
  public abstract int getNumThreads();

  @Config("druid.host")
  public abstract String getHost();
}
