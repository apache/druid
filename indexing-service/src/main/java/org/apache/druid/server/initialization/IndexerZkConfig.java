/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.server.initialization;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.curator.utils.ZKPaths;

/**
 *
 */
public class IndexerZkConfig
{
  @JsonCreator
  public IndexerZkConfig(
      @JacksonInject ZkPathsConfig zkPathsConfig,
      @JsonProperty("base") String base,
      @JsonProperty("announcementsPath") String announcementsPath,
      @JsonProperty("tasksPath") String tasksPath,
      @JsonProperty("statusPath") String statusPath
  )
  {
    this.zkPathsConfig = zkPathsConfig;
    this.base = base;
    this.announcementsPath = announcementsPath;
    this.tasksPath = tasksPath;
    this.statusPath = statusPath;
  }

  @JacksonInject
  private final ZkPathsConfig zkPathsConfig;

  @JsonProperty
  private final String base;

  @JsonProperty
  private final String announcementsPath;

  @JsonProperty
  private final String tasksPath;

  @JsonProperty
  private final String statusPath;

  private String defaultIndexerPath(final String subPath)
  {
    return ZKPaths.makePath(getBase(), subPath);
  }

  public String getBase()
  {
    return base == null ? getZkPathsConfig().defaultPath("indexer") : base;
  }

  public String getAnnouncementsPath()
  {
    return announcementsPath == null ? defaultIndexerPath("announcements") : announcementsPath;
  }

  public String getTasksPath()
  {
    return tasksPath == null ? defaultIndexerPath("tasks") : tasksPath;
  }

  public String getStatusPath()
  {
    return statusPath == null ? defaultIndexerPath("status") : statusPath;
  }

  public ZkPathsConfig getZkPathsConfig()
  {
    return zkPathsConfig;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    IndexerZkConfig that = (IndexerZkConfig) o;

    if (announcementsPath != null
        ? !announcementsPath.equals(that.announcementsPath)
        : that.announcementsPath != null) {
      return false;
    }
    if (base != null ? !base.equals(that.base) : that.base != null) {
      return false;
    }
    if (statusPath != null ? !statusPath.equals(that.statusPath) : that.statusPath != null) {
      return false;
    }
    if (tasksPath != null ? !tasksPath.equals(that.tasksPath) : that.tasksPath != null) {
      return false;
    }
    if (zkPathsConfig != null ? !zkPathsConfig.equals(that.zkPathsConfig) : that.zkPathsConfig != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = zkPathsConfig != null ? zkPathsConfig.hashCode() : 0;
    result = 31 * result + (base != null ? base.hashCode() : 0);
    result = 31 * result + (announcementsPath != null ? announcementsPath.hashCode() : 0);
    result = 31 * result + (tasksPath != null ? tasksPath.hashCode() : 0);
    result = 31 * result + (statusPath != null ? statusPath.hashCode() : 0);
    return result;
  }
}
