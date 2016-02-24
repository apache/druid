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

package io.druid.indexing.overlord.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.curator.CuratorUtils;
import org.joda.time.Period;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 */
public class RemoteTaskRunnerConfig
{
  @JsonProperty
  @NotNull
  private Period taskAssignmentTimeout = new Period("PT5M");

  @JsonProperty
  @NotNull
  private Period taskCleanupTimeout = new Period("PT15M");

  @JsonProperty
  private String minWorkerVersion = "0";

  @JsonProperty
  @Min(10 * 1024)
  private int maxZnodeBytes = CuratorUtils.DEFAULT_MAX_ZNODE_BYTES;

  @JsonProperty
  private Period taskShutdownLinkTimeout = new Period("PT1M");

  public Period getTaskAssignmentTimeout()
  {
    return taskAssignmentTimeout;
  }

  @JsonProperty
  public Period getTaskCleanupTimeout(){
    return taskCleanupTimeout;
  }

  public String getMinWorkerVersion()
  {
    return minWorkerVersion;
  }

  public int getMaxZnodeBytes()
  {
    return maxZnodeBytes;
  }

  public Period getTaskShutdownLinkTimeout()
  {
    return taskShutdownLinkTimeout;
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

    RemoteTaskRunnerConfig that = (RemoteTaskRunnerConfig) o;

    if (getMaxZnodeBytes() != that.getMaxZnodeBytes()) {
      return false;
    }
    if (!getTaskAssignmentTimeout().equals(that.getTaskAssignmentTimeout())) {
      return false;
    }
    if (!getTaskCleanupTimeout().equals(that.getTaskCleanupTimeout())) {
      return false;
    }
    if (!getMinWorkerVersion().equals(that.getMinWorkerVersion())) {
      return false;
    }
    return getTaskShutdownLinkTimeout().equals(that.getTaskShutdownLinkTimeout());

  }

  @Override
  public int hashCode()
  {
    int result = getTaskAssignmentTimeout().hashCode();
    result = 31 * result + getTaskCleanupTimeout().hashCode();
    result = 31 * result + getMinWorkerVersion().hashCode();
    result = 31 * result + getMaxZnodeBytes();
    result = 31 * result + getTaskShutdownLinkTimeout().hashCode();
    return result;
  }
}
