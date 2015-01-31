/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.indexing.common.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.io.File;
import java.util.List;

public class TaskConfig
{
  public static final List<String> DEFAULT_DEFAULT_HADOOP_COORDINATES = ImmutableList.of(
      "org.apache.hadoop:hadoop-client:2.3.0"
  );

  @JsonProperty
  private final String baseDir;

  @JsonProperty
  private final File baseTaskDir;

  @JsonProperty
  private final String hadoopWorkingPath;

  @JsonProperty
  private final int defaultRowFlushBoundary;

  @JsonProperty
  private final List<String> defaultHadoopCoordinates;

  @JsonCreator
  public TaskConfig(
      @JsonProperty("baseDir") String baseDir,
      @JsonProperty("baseTaskDir") String baseTaskDir,
      @JsonProperty("hadoopWorkingPath") String hadoopWorkingPath,
      @JsonProperty("defaultRowFlushBoundary") Integer defaultRowFlushBoundary,
      @JsonProperty("defaultHadoopCoordinates") List<String> defaultHadoopCoordinates
  )
  {
    this.baseDir = baseDir == null ? "/tmp" : baseDir;
    this.baseTaskDir = new File(defaultDir(baseTaskDir, "persistent/task"));
    this.hadoopWorkingPath = defaultDir(hadoopWorkingPath, "druid-indexing");
    this.defaultRowFlushBoundary = defaultRowFlushBoundary == null ? 500000 : defaultRowFlushBoundary;
    this.defaultHadoopCoordinates = defaultHadoopCoordinates == null
                                    ? DEFAULT_DEFAULT_HADOOP_COORDINATES
                                    : defaultHadoopCoordinates;
  }

  @JsonProperty
  public String getBaseDir()
  {
    return baseDir;
  }

  @JsonProperty
  public File getBaseTaskDir()
  {
    return baseTaskDir;
  }

  @JsonProperty
  public String getHadoopWorkingPath()
  {
    return hadoopWorkingPath;
  }

  @JsonProperty
  public int getDefaultRowFlushBoundary()
  {
    return defaultRowFlushBoundary;
  }

  @JsonProperty
  public List<String> getDefaultHadoopCoordinates()
  {
    return defaultHadoopCoordinates;
  }

  private String defaultDir(String configParameter, final String defaultVal)
  {
    if (configParameter == null) {
      return String.format("%s/%s", getBaseDir(), defaultVal);
    }

    return configParameter;
  }
}
