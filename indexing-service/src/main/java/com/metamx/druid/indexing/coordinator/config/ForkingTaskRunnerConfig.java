package com.metamx.druid.indexing.coordinator.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.metamx.druid.indexing.worker.executor.ExecutorMain;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.List;

public class ForkingTaskRunnerConfig
{
  @JsonProperty
  @Min(1)
  private int maxForks = 1;

  @JsonProperty
  @NotNull
  private String taskDir = "/tmp/persistent";

  @JsonProperty
  @NotNull
  private String javaCommand = "java";

  @JsonProperty
  @NotNull
  private String javaOpts = "";

  @JsonProperty
  @NotNull
  private String classpath = System.getProperty("java.class.path");

  @JsonProperty
  @NotNull
  private String mainClass = ExecutorMain.class.getName();

  @JsonProperty
  @Min(1024) @Max(65535)
  private int startPort = 8080;

  @JsonProperty
  @NotNull
  List<String> allowedPrefixes = Lists.newArrayList("com.metamx", "druid", "io.druid");

  public int maxForks()
  {
    return maxForks;
  }

  public String getTaskDir()
  {
    return taskDir;
  }

  public String getJavaCommand()
  {
    return javaCommand;
  }

  public String getJavaOpts()
  {
    return javaOpts;
  }

  public String getClasspath()
  {
    return classpath;
  }

  public String getMainClass()
  {
    return mainClass;
  }

  public int getStartPort()
  {
    return startPort;
  }

  public List<String> getAllowedPrefixes()
  {
    return allowedPrefixes;
  }
}
