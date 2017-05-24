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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.ImmutableWorkerInfo;
import io.druid.indexing.overlord.config.WorkerTaskRunnerConfig;

import java.util.Comparator;
import java.util.TreeSet;

/**
 */
public class EqualDistributionWorkerSelectStrategy implements WorkerSelectStrategy
{
  @Override
  public Optional<ImmutableWorkerInfo> findWorkerForTask(
      WorkerTaskRunnerConfig config, ImmutableMap<String, ImmutableWorkerInfo> zkWorkers, Task task
  )
  {
    final TreeSet<ImmutableWorkerInfo> sortedWorkers = Sets.newTreeSet(
        Comparator.comparing(ImmutableWorkerInfo::getAvailableCapacity).reversed()
                                              .thenComparing(zkWorker -> zkWorker.getWorker().getVersion()));
    sortedWorkers.addAll(zkWorkers.values());
    final String minWorkerVer = config.getMinWorkerVersion();

    for (ImmutableWorkerInfo zkWorker : sortedWorkers) {
      if (zkWorker.canRunTask(task) && zkWorker.isValidVersion(minWorkerVer)) {
        return Optional.of(zkWorker);
      }
    }

    return Optional.absent();
  }
}
