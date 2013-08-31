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

import java.io.File;

public class TaskConfig
{
  @JsonProperty
  private final String baseDir;

  @JsonProperty
  private final File baseTaskDir;

  @JsonProperty
  private final String hadoopWorkingPath;

  @JsonProperty
  private final int defaultRowFlushBoundary;

  @JsonCreator
  public TaskConfig(
      @JsonProperty("baseDir") String baseDir,
      @JsonProperty("baseTaskDir") String baseTaskDir,
      @JsonProperty("hadoopWorkingPath") String hadoopWorkingPath,
      @JsonProperty("defaultRowFlushBoundary") Integer defaultRowFlushBoundary
  )
  {
    this.baseDir = baseDir == null ? "/tmp" : baseDir;
    this.baseTaskDir = new File(defaultDir(baseTaskDir, "persistent/task"));
    this.hadoopWorkingPath = defaultDir(hadoopWorkingPath, "druid-indexing");
    this.defaultRowFlushBoundary = defaultRowFlushBoundary == null ? 500000 : defaultRowFlushBoundary;
  }

  public String getBaseDir()
  {
    return baseDir;
  }

  public File getBaseTaskDir()
  {
    return baseTaskDir;
  }

  public String getHadoopWorkingPath()
  {
    return hadoopWorkingPath;
  }

  public int getDefaultRowFlushBoundary()
  {
    return defaultRowFlushBoundary;
  }

  private String defaultDir(String configParameter, final String defaultVal)
  {
    if (configParameter == null) {
      return String.format("%s/%s", getBaseDir(), defaultVal);
    }

    return configParameter;
  }
}