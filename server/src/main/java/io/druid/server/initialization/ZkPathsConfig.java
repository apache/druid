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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.curator.utils.ZKPaths;

public class ZkPathsConfig
{
  @JsonProperty
  private
  String base = "druid";
  @JsonProperty
  private
  String propertiesPath;
  @JsonProperty
  private
  String announcementsPath;
  @JsonProperty
  private
  String servedSegmentsPath;
  @JsonProperty
  private
  String liveSegmentsPath;
  @JsonProperty
  private
  String coordinatorPath;
  @JsonProperty
  private
  String loadQueuePath;
  @JsonProperty
  private
  String connectorPath;

  public String getBase()
  {
    return base;
  }

  public String getPropertiesPath()
  {
    return (null == propertiesPath) ? defaultPath("properties") : propertiesPath;
  }

  public String getAnnouncementsPath()
  {
    return (null == announcementsPath) ? defaultPath("announcements") : announcementsPath;
  }

  public String getServedSegmentsPath()
  {
    return (null == servedSegmentsPath) ?  defaultPath("servedSegments") : servedSegmentsPath;
  }

  public String getLiveSegmentsPath()
  {
    return (null == liveSegmentsPath) ? defaultPath("segments") : liveSegmentsPath;
  }

  public String getCoordinatorPath()
  {
    return (null == coordinatorPath) ?  defaultPath("coordinator") : coordinatorPath;
  }

  public String getLoadQueuePath()
  {
    return (null == loadQueuePath) ?  defaultPath("loadQueue") : loadQueuePath;
  }

  public String getConnectorPath()
  {
    return (null == connectorPath) ?  defaultPath("connector") : connectorPath;
  }

  protected String defaultPath(final String subPath)
  {
    return ZKPaths.makePath(getBase(), subPath);
  }

  @Override
  public boolean equals(Object other){
    if(null == other){
      return false;
    }
    if(this == other){
      return true;
    }
    if(!(other instanceof ZkPathsConfig)){
      return false;
    }
    ZkPathsConfig otherConfig = (ZkPathsConfig) other;
    if(
        this.getBase().equals(otherConfig.getBase()) &&
        this.getAnnouncementsPath().equals(otherConfig.getAnnouncementsPath()) &&
        this.getConnectorPath().equals(otherConfig.getConnectorPath()) &&
        this.getLiveSegmentsPath().equals(otherConfig.getLiveSegmentsPath()) &&
        this.getCoordinatorPath().equals(otherConfig.getCoordinatorPath()) &&
        this.getLoadQueuePath().equals(otherConfig.getLoadQueuePath()) &&
        this.getPropertiesPath().equals(otherConfig.getPropertiesPath()) &&
        this.getServedSegmentsPath().equals(otherConfig.getServedSegmentsPath())
        ){
      return true;
    }
    return false;
  }
}
