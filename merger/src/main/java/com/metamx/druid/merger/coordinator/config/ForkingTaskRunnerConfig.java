package com.metamx.druid.merger.coordinator.config;

import org.skife.config.Config;
import org.skife.config.Default;

public abstract class ForkingTaskRunnerConfig
{
  @Config("druid.indexer.fork.java")
  @Default("java")
  public abstract String getJavaCommand();

  @Config("druid.indexer.fork.opts")
  @Default("")
  public abstract String getJavaOptions();

  @Config("druid.indexer.fork.classpath")
  public String getJavaClasspath() {
    return System.getProperty("java.class.path");
  }

  @Config("druid.indexer.fork.hostpattern")
  public abstract String getHostPattern();

  @Config("druid.indexer.fork.startport")
  public abstract int getStartPort();
}
