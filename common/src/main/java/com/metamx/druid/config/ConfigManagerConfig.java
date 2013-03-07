package com.metamx.druid.config;

import org.joda.time.Duration;
import org.skife.config.Config;
import org.skife.config.Default;

/**
 */
public abstract class ConfigManagerConfig
{
  @Config("druid.indexer.configTable")
  public abstract String getConfigTable();

  @Config("druid.indexer.poll.duration")
  @Default("PT1M")
  public abstract Duration getPollDuration();

}
