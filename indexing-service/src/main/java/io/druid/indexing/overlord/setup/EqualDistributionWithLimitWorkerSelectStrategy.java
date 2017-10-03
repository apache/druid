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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.ImmutableWorkerInfo;
import io.druid.indexing.overlord.config.WorkerTaskRunnerConfig;


import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

public class EqualDistributionWithLimitWorkerSelectStrategy implements WorkerSelectStrategy
{
  private final LimitConfig limitConfig;

  @JsonCreator
  public EqualDistributionWithLimitWorkerSelectStrategy(
      @JsonProperty("limitConfig") LimitConfig limitConfig)
  {
    this.limitConfig = limitConfig;

  }

  @JsonProperty
  public LimitConfig getLimitConfig()
  {
    return limitConfig;
  }


  @Override
  public ImmutableWorkerInfo findWorkerForTask(
      final WorkerTaskRunnerConfig config,
      final ImmutableMap<String, ImmutableWorkerInfo> zkWorkers,
      final Task task
  )
  {

    Map<String, Integer> rest = new TreeMap<String, Integer>(
        Comparator.comparing(String::length).reversed().thenComparing(Comparator.naturalOrder())
    );
    Map<String, Integer> configs = limitConfig.getLimit();
    for (String taskIdPrefix: configs.keySet()) {
      rest.put(taskIdPrefix, configs.get(taskIdPrefix));
    }

    for (ImmutableWorkerInfo zkWorker: zkWorkers.values()) {
      for (String taskId: zkWorker.getRunningTasks()) {
        for (String taskIdPrefix: rest.keySet()) {
          if (taskId.startsWith(taskIdPrefix)) {
            rest.put(taskIdPrefix, rest.get(taskIdPrefix) - 1);
          }
        }
      }
    }
    String taskId = task.getId();
    for (String taskIdPrefix: rest.keySet()) {
      if (taskId.startsWith(taskIdPrefix)) {
        if (rest.getOrDefault(taskIdPrefix, 1) <= 0) {
          return null;
        }
      }
    }
    final TreeSet<ImmutableWorkerInfo> sortedWorkers = Sets.newTreeSet(
        Comparator.comparing(ImmutableWorkerInfo::getAvailableCapacity).reversed()
                .thenComparing(zkWorker -> zkWorker.getWorker().getVersion()));
    sortedWorkers.addAll(zkWorkers.values());
    final String minWorkerVer = config.getMinWorkerVersion();

    for (ImmutableWorkerInfo zkWorker : sortedWorkers) {
      if (zkWorker.canRunTask(task) && zkWorker.isValidVersion(minWorkerVer)) {
        return zkWorker;
      }
    }
    return null;
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

    EqualDistributionWithLimitWorkerSelectStrategy that = (EqualDistributionWithLimitWorkerSelectStrategy) o;

    if (limitConfig != null ? !limitConfig.equals(that.limitConfig) : that.limitConfig != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return limitConfig != null ? limitConfig.hashCode() : 0;
  }
}
