/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.indexing.coordinator.setup;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 */
public class WorkerSetupData
{
  public static final String CONFIG_KEY = "worker.setup";

  private final String minVersion;
  private final int minNumWorkers;
  private final int maxNumWorkers;
  private final EC2NodeData nodeData;
  private final GalaxyUserData userData;

  @JsonCreator
  public WorkerSetupData(
      @JsonProperty("minVersion") String minVersion,
      @JsonProperty("minNumWorkers") int minNumWorkers,
      @JsonProperty("maxNumWorkers") int maxNumWorkers,
      @JsonProperty("nodeData") EC2NodeData nodeData,
      @JsonProperty("userData") GalaxyUserData userData
  )
  {
    this.minVersion = minVersion;
    this.minNumWorkers = minNumWorkers;
    this.maxNumWorkers = maxNumWorkers;
    this.nodeData = nodeData;
    this.userData = userData;
  }

  @JsonProperty
  public String getMinVersion()
  {
    return minVersion;
  }

  @JsonProperty
  public int getMinNumWorkers()
  {
    return minNumWorkers;
  }

  @JsonProperty
  public int getMaxNumWorkers()
  {
    return maxNumWorkers;
  }

  @JsonProperty
  public EC2NodeData getNodeData()
  {
    return nodeData;
  }

  @JsonProperty
  public GalaxyUserData getUserData()
  {
    return userData;
  }

  @Override
  public String toString()
  {
    return "WorkerSetupData{" +
           "minVersion='" + minVersion + '\'' +
           ", minNumWorkers=" + minNumWorkers +
           ", maxNumWorkers=" + maxNumWorkers +
           ", nodeData=" + nodeData +
           ", userData=" + userData +
           '}';
  }
}
