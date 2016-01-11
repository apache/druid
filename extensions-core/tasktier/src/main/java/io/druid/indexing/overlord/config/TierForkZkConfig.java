/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.indexing.overlord.config;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.druid.server.initialization.ZkPathsConfig;
import org.apache.curator.utils.ZKPaths;

public class TierForkZkConfig
{
  @JsonCreator
  public TierForkZkConfig(
      @JacksonInject ZkPathsConfig zkPathsConfig,
      @JsonProperty("tierLeaderBasePath") String tierLeaderBasePath,
      @JsonProperty("tierTaskIDPath") String tierTaskIDPath
  )
  {
    this.tierLeaderBasePath = tierLeaderBasePath;
    this.tierTaskIDPath = tierTaskIDPath;
    this.zkPathsConfig = zkPathsConfig;
  }

  @JsonProperty
  public String tierTaskIDPath = null;

  @JsonProperty
  public String tierLeaderBasePath = null;

  @JsonIgnore
  @JacksonInject
  public ZkPathsConfig zkPathsConfig = new ZkPathsConfig();

  public String getTierTaskIDPath()
  {
    return tierTaskIDPath != null ? tierTaskIDPath : zkPathsConfig.defaultPath("tierTasks");
  }

  public String getTierLeaderBasePath()
  {
    return tierLeaderBasePath != null ? tierLeaderBasePath : zkPathsConfig.defaultPath("tierLeaders");
  }

  public String getTierLeaderPath(String tier)
  {
    return ZKPaths.makePath(getTierLeaderBasePath(), Preconditions.checkNotNull(Strings.emptyToNull(tier), "tier"));
  }

  public String getTierTaskIDPath(String taskId)
  {
    return ZKPaths.makePath(getTierTaskIDPath(), taskId);
  }
}
