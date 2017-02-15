/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.overlord.setup;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.indexing.overlord.TwoCloudWorkerSelectStrategy;

import java.util.Objects;

public class TwoCloudConfig implements BaseWorkerBehaviorConfig
{
  private final String taskLabel1;
  private final String ipPrefix1;
  private final WorkerBehaviorConfig cloud1Config;
  private final String taskLabel2;
  private final String ipPrefix2;
  private final WorkerBehaviorConfig cloud2Config;

  @JsonCreator
  public TwoCloudConfig(
      @JsonProperty("taskLabel1") String taskLabel1,
      @JsonProperty("ipPrefix1") String ipPrefix1,
      @JsonProperty("cloud1Config") WorkerBehaviorConfig cloud1Config,
      @JsonProperty("taskLabel2") String taskLabel2,
      @JsonProperty("ipPrefix2") String ipPrefix2,
      @JsonProperty("cloud2Config") WorkerBehaviorConfig cloud2Config
  )
  {
    this.taskLabel1 = taskLabel1;
    this.ipPrefix1 = ipPrefix1;
    this.cloud1Config = cloud1Config;
    this.taskLabel2 = taskLabel2;
    this.ipPrefix2 = ipPrefix2;
    this.cloud2Config = cloud2Config;
  }

  @JsonProperty
  public String getTaskLabel1() {
    return taskLabel1;
  }

  @JsonProperty
  public String getIpPrefix1()
  {
    return ipPrefix1;
  }

  @JsonProperty
  public WorkerBehaviorConfig getCloud1Config()
  {
    return cloud1Config;
  }

  @JsonProperty
  public String getTaskLabel2() {
    return taskLabel2;
  }

  @JsonProperty
  public String getIpPrefix2()
  {
    return ipPrefix2;
  }

  @JsonProperty
  public WorkerBehaviorConfig getCloud2Config()
  {
    return cloud2Config;
  }

  public String getIpFilter(String taskLabel) {
    if (taskLabel == null || taskLabel.equals(taskLabel1)) {
      return ipPrefix1;
    }
    return ipPrefix2;
  }

  public WorkerBehaviorConfig getWorkerBehaviorConfig(String taskLabel) {
    if (taskLabel == null || taskLabel.equals(taskLabel1)) {
      return cloud1Config;
    }
    return cloud2Config;
  }

  @Override
  public WorkerSelectStrategy getSelectStrategy()
  {
    return new TwoCloudWorkerSelectStrategy(this);
  }

  @Override
  public String toString()
  {
    return "TwoCloudConfig{" +
           "taskLabel1='" + taskLabel1 + '\'' +
           ", ipPrefix1='" + ipPrefix1 + '\'' +
           ", cloud1Config=" + cloud1Config +
           ", taskLabel2='" + taskLabel2 + '\'' +
           ", ipPrefix2='" + ipPrefix2 + '\'' +
           ", cloud2Config=" + cloud2Config +
           '}';
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
    TwoCloudConfig that = (TwoCloudConfig) o;
    return Objects.equals(taskLabel1, that.taskLabel1) &&
           Objects.equals(ipPrefix1, that.ipPrefix1) &&
           Objects.equals(cloud1Config, that.cloud1Config) &&
           Objects.equals(taskLabel2, that.taskLabel2) &&
           Objects.equals(ipPrefix2, that.ipPrefix2) &&
           Objects.equals(cloud2Config, that.cloud2Config);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(taskLabel1, ipPrefix1, cloud1Config, taskLabel2, ipPrefix2, cloud2Config);
  }
}
