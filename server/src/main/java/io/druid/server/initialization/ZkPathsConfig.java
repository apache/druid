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
