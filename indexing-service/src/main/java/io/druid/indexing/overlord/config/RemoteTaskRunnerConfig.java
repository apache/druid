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
public class RemoteTaskRunnerConfig extends WorkerTaskRunnerConfig
{
  @JsonProperty
  @NotNull
  private Period taskAssignmentTimeout = new Period("PT5M");

  @JsonProperty
  @NotNull
  private Period taskCleanupTimeout = new Period("PT15M");

  @JsonProperty
  @Min(10 * 1024)
  private int maxZnodeBytes = CuratorUtils.DEFAULT_MAX_ZNODE_BYTES;

  @JsonProperty
  private Period taskShutdownLinkTimeout = new Period("PT1M");

  @JsonProperty
  @Min(1)
  private int pendingTasksRunnerNumThreads = 3;

  public Period getTaskAssignmentTimeout()
  {
    return taskAssignmentTimeout;
  }

  @JsonProperty
  public Period getTaskCleanupTimeout(){
    return taskCleanupTimeout;
  }

  public int getMaxZnodeBytes()
  {
    return maxZnodeBytes;
  }

  public Period getTaskShutdownLinkTimeout()
  {
    return taskShutdownLinkTimeout;
  }


  public int getPendingTasksRunnerNumThreads()
  {
    return pendingTasksRunnerNumThreads;
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

    if (maxZnodeBytes != that.maxZnodeBytes) {
      return false;
    }
    if (pendingTasksRunnerNumThreads != that.pendingTasksRunnerNumThreads) {
      return false;
    }
    if (!taskAssignmentTimeout.equals(that.taskAssignmentTimeout)) {
      return false;
    }
    if (!taskCleanupTimeout.equals(that.taskCleanupTimeout)) {
      return false;
    }
    if (!getMinWorkerVersion().equals(that.getMinWorkerVersion())) {
      return false;
    }
    return taskShutdownLinkTimeout.equals(that.taskShutdownLinkTimeout);

  }

  @Override
  public int hashCode()
  {
    int result = taskAssignmentTimeout.hashCode();
    result = 31 * result + taskCleanupTimeout.hashCode();
    result = 31 * result + getMinWorkerVersion().hashCode();
    result = 31 * result + maxZnodeBytes;
    result = 31 * result + taskShutdownLinkTimeout.hashCode();
    result = 31 * result + pendingTasksRunnerNumThreads;
    return result;
  }

  @Override
  public String toString()
  {
    return "RemoteTaskRunnerConfig{" +
           "taskAssignmentTimeout=" + taskAssignmentTimeout +
           ", taskCleanupTimeout=" + taskCleanupTimeout +
           ", minWorkerVersion='" + getMinWorkerVersion() + '\'' +
           ", maxZnodeBytes=" + maxZnodeBytes +
           ", taskShutdownLinkTimeout=" + taskShutdownLinkTimeout +
           ", pendingTasksRunnerNumThreads=" + pendingTasksRunnerNumThreads +
           '}';
  }
}
