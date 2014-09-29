/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
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
