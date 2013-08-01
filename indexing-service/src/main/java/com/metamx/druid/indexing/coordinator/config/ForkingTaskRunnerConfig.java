package com.metamx.druid.indexing.coordinator.config;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metamx.druid.indexing.worker.executor.ExecutorMain;
import org.skife.config.Config;
import org.skife.config.Default;

import java.io.File;
import java.util.List;
import java.util.Set;

public abstract class ForkingTaskRunnerConfig
{
  @Config("druid.indexer.taskDir")
  public abstract File getBaseTaskDir();

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

  @Config("druid.indexer.fork.main")
  public String getMainClass()
  {
    return ExecutorMain.class.getName();
  }

  @Config("druid.indexer.fork.hostpattern")
  public abstract String getHostPattern();

  @Config("druid.indexer.fork.startport")
  public abstract int getStartPort();

  @Config("druid.indexer.properties.prefixes")
  public List<String> getAllowedPrefixes()
  {
    return Lists.newArrayList("com.metamx", "druid");
  }
}
