package com.metamx.druid.merger.coordinator.config;

import org.joda.time.Duration;
import org.skife.config.Config;
import org.skife.config.Default;

/**
 */
public abstract class WorkerSetupManagerConfig
{
  @Config("druid.indexer.workerSetupTable")
  public abstract String getWorkerSetupTable();

  @Config("druid.indexer.poll.duration")
  @Default("PT1M")
  public abstract Duration getPollDuration();
}
