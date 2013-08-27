package com.metamx.druid.indexing.coordinator.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;

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

  /**
   * This is intended for setting -X parameters on the underlying java.  It is used by first splitting on whitespace,
   * so it cannot handle properties that have whitespace in the value.  Those properties should be set via a
   * druid.indexer.fork.property. property instead.
   */
  @JsonProperty
  @NotNull
  private String javaOpts = "";

  @JsonProperty
  @NotNull
  private String classpath = System.getProperty("java.class.path");

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

  public int getStartPort()
  {
    return startPort;
  }

  public List<String> getAllowedPrefixes()
  {
    return allowedPrefixes;
  }
}
