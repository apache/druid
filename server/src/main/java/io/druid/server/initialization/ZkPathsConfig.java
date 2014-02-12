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

package io.druid.server.initialization;

import org.apache.curator.utils.ZKPaths;
import org.skife.config.Config;

public abstract class ZkPathsConfig
{
  @Config("druid.zk.paths.base")
  public String getZkBasePath()
  {
    return "druid";
  }

  @Config("druid.zk.paths.propertiesPath")
  public String getPropertiesPath()
  {
    return defaultPath("properties");
  }

  @Config("druid.zk.paths.announcementsPath")
  public String getAnnouncementsPath()
  {
    return defaultPath("announcements");
  }

  @Config("druid.zk.paths.servedSegmentsPath")
  public String getServedSegmentsPath()
  {
    return defaultPath("servedSegments");
  }

  @Config("druid.zk.paths.liveSegmentsPath")
  public String getLiveSegmentsPath()
  {
    return defaultPath("segments");
  }

  @Config("druid.zk.paths.loadQueuePath")
  public String getLoadQueuePath()
  {
    return defaultPath("loadQueue");
  }

  @Config("druid.zk.paths.coordinatorPath")
  public String getCoordinatorPath()
  {
    return defaultPath("coordinator");
  }

  @Config("druid.zk.paths.connectorPath")
  public String getConnectorPath()
  {
    return defaultPath("connector");
  }

  @Config("druid.zk.paths.indexer.announcementsPath")
  public String getIndexerAnnouncementPath()
  {
    return defaultPath("indexer/announcements");
  }

  @Config("druid.zk.paths.indexer.tasksPath")
  public String getIndexerTaskPath()
  {
    return defaultPath("indexer/tasks");
  }

  @Config("druid.zk.paths.indexer.statusPath")
  public String getIndexerStatusPath()
  {
    return defaultPath("indexer/status");
  }

  @Config("druid.zk.paths.indexer.leaderLatchPath")
  public String getIndexerLeaderLatchPath()
  {
    return defaultPath("indexer/leaderLatchPath");
  }

  private String defaultPath(final String subPath)
  {
    return ZKPaths.makePath(getZkBasePath(), subPath);
  }
}
