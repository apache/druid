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
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.ImmutableWorkerInfo;
import io.druid.indexing.overlord.config.WorkerTaskRunnerConfig;

import java.util.List;
import java.util.Set;

/**
 */
public class FillCapacityWithAffinityWorkerSelectStrategy extends FillCapacityWorkerSelectStrategy
{
  private final AffinityConfig affinityConfig;
  private final Set<String> affinityWorkerHosts = Sets.newHashSet();

  @JsonCreator
  public FillCapacityWithAffinityWorkerSelectStrategy(
      @JsonProperty("affinityConfig") AffinityConfig affinityConfig
  )
  {
    this.affinityConfig = affinityConfig;
    for (List<String> affinityWorkers : affinityConfig.getAffinity().values()) {
      for (String affinityWorker : affinityWorkers) {
        this.affinityWorkerHosts.add(affinityWorker);
      }
    }
  }

  @JsonProperty
  public AffinityConfig getAffinityConfig()
  {
    return affinityConfig;
  }

  @Override
  public Optional<ImmutableWorkerInfo> findWorkerForTask(
      final WorkerTaskRunnerConfig config,
      final ImmutableMap<String, ImmutableWorkerInfo> zkWorkers,
      final Task task
  )
  {
    // don't run other datasources on affinity workers; we only want our configured datasources to run on them
    ImmutableMap.Builder<String, ImmutableWorkerInfo> builder = new ImmutableMap.Builder<>();
    for (String workerHost : zkWorkers.keySet()) {
      if (!affinityWorkerHosts.contains(workerHost)) {
        builder.put(workerHost, zkWorkers.get(workerHost));
      }
    }
    ImmutableMap<String, ImmutableWorkerInfo> eligibleWorkers = builder.build();

    List<String> workerHosts = affinityConfig.getAffinity().get(task.getDataSource());
    if (workerHosts == null) {
      return super.findWorkerForTask(config, eligibleWorkers, task);
    }

    ImmutableMap.Builder<String, ImmutableWorkerInfo> affinityBuilder = new ImmutableMap.Builder<>();
    for (String workerHost : workerHosts) {
      ImmutableWorkerInfo zkWorker = zkWorkers.get(workerHost);
      if (zkWorker != null) {
        affinityBuilder.put(workerHost, zkWorker);
      }
    }
    ImmutableMap<String, ImmutableWorkerInfo> affinityWorkers = affinityBuilder.build();

    if (!affinityWorkers.isEmpty()) {
      Optional<ImmutableWorkerInfo> retVal = super.findWorkerForTask(config, affinityWorkers, task);
      if (retVal.isPresent()) {
        return retVal;
      }
    }

    return super.findWorkerForTask(config, eligibleWorkers, task);
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

    FillCapacityWithAffinityWorkerSelectStrategy that = (FillCapacityWithAffinityWorkerSelectStrategy) o;

    if (affinityConfig != null ? !affinityConfig.equals(that.affinityConfig) : that.affinityConfig != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return affinityConfig != null ? affinityConfig.hashCode() : 0;
  }
}

