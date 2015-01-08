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
      @JsonProperty("status") String status,
      @JsonProperty("leaderLatchPath") String leaderLatchPath
  )
  {
    this.zkPathsConfig = zkPathsConfig;
    this.base = base;
    this.announcementsPath = announcementsPath;
    this.tasksPath = tasksPath;
    this.status = status;
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
  private final String status;

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

  public String getStatus()
  {
    return status == null ? defaultIndexerPath("status") : status;
  }

  public String getLeaderLatchPath()
  {
    return leaderLatchPath == null ? defaultIndexerPath("leaderLatchPath") : leaderLatchPath;
  }

  public ZkPathsConfig getZkPathsConfig()
  {
    return zkPathsConfig;
  }
}
