/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013, 2014  Metamarkets Group Inc.
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

package io.druid.server.initialization;

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
      @JsonProperty("statusPath") String statusPath,
      @JsonProperty("leaderLatchPath") String leaderLatchPath
  )
  {
    this.zkPathsConfig = zkPathsConfig;
    this.base = base;
    this.announcementsPath = announcementsPath;
    this.tasksPath = tasksPath;
    this.statusPath = statusPath;
    this.leaderLatchPath = leaderLatchPath;
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

  @JsonProperty
  private final String leaderLatchPath;

  private String defaultIndexerPath(final String subPath)
  {
    return getZkPathsConfig().defaultPath(ZKPaths.makePath(getBase(), subPath));
  }

  public String getBase()
  {
    return base == null ? "indexer" : base;
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

  public String getLeaderLatchPath()
  {
    return leaderLatchPath == null ? defaultIndexerPath("leaderLatchPath") : leaderLatchPath;
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
    if (leaderLatchPath != null ? !leaderLatchPath.equals(that.leaderLatchPath) : that.leaderLatchPath != null) {
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
    result = 31 * result + (leaderLatchPath != null ? leaderLatchPath.hashCode() : 0);
    return result;
  }
}
