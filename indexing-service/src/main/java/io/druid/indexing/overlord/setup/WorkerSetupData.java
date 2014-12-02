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

package io.druid.indexing.overlord.setup;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.indexing.overlord.autoscaling.ec2.EC2NodeData;
import io.druid.indexing.overlord.autoscaling.ec2.EC2UserData;

/**
 */
@Deprecated
public class WorkerSetupData
{
  public static final String CONFIG_KEY = "worker.setup";

  private final int minNumWorkers;
  private final int maxNumWorkers;
  private final String availabilityZone;
  private final EC2NodeData nodeData;
  private final EC2UserData userData;

  @JsonCreator
  public WorkerSetupData(
      @JsonProperty("minNumWorkers") int minNumWorkers,
      @JsonProperty("maxNumWorkers") int maxNumWorkers,
      @JsonProperty("availabilityZone") String availabilityZone,
      @JsonProperty("nodeData") EC2NodeData nodeData,
      @JsonProperty("userData") EC2UserData userData
  )
  {
    this.minNumWorkers = minNumWorkers;
    this.maxNumWorkers = maxNumWorkers;
    this.availabilityZone = availabilityZone;
    this.nodeData = nodeData;
    this.userData = userData;
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
  public String getAvailabilityZone()
  {
    return availabilityZone;
  }

  @JsonProperty
  public EC2NodeData getNodeData()
  {
    return nodeData;
  }

  @JsonProperty
  public EC2UserData getUserData()
  {
    return userData;
  }

  @Override
  public String toString()
  {
    return "WorkerSetupData{" +
           ", minNumWorkers=" + minNumWorkers +
           ", maxNumWorkers=" + maxNumWorkers +
           ", availabilityZone=" + availabilityZone +
           ", nodeData=" + nodeData +
           ", userData=" + userData +
           '}';
  }
}
