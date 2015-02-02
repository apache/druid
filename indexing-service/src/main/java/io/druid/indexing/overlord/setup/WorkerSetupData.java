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
