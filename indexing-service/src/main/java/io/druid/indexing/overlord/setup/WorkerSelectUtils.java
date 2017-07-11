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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.ImmutableWorkerInfo;
import io.druid.indexing.overlord.config.WorkerTaskRunnerConfig;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class WorkerSelectUtils
{
  private WorkerSelectUtils()
  {
    // No instantiation.
  }

  /**
   * Helper for {@link WorkerSelectStrategy} implementations.
   *
   * @param allWorkers     map of all workers, in the style provided to {@link WorkerSelectStrategy}
   * @param affinityConfig affinity config, or null
   * @param workerSelector function that receives a list of eligible workers: version is high enough, worker can run
   *                       the task, and worker satisfies the affinity config. may return null.
   *
   * @return selected worker from "allWorkers", or null.
   */
  @Nullable
  public static ImmutableWorkerInfo selectWorker(
      final Task task,
      final Map<String, ImmutableWorkerInfo> allWorkers,
      final WorkerTaskRunnerConfig workerTaskRunnerConfig,
      @Nullable final AffinityConfig affinityConfig,
      final Function<ImmutableMap<String, ImmutableWorkerInfo>, ImmutableWorkerInfo> workerSelector
  )
  {
    // Workers that could potentially run this task, ignoring affinityConfig.
    final Map<String, ImmutableWorkerInfo> runnableWorkers = allWorkers
        .values()
        .stream()
        .filter(worker -> worker.canRunTask(task)
                          && worker.isValidVersion(workerTaskRunnerConfig.getMinWorkerVersion()))
        .collect(Collectors.toMap(w -> w.getWorker().getHost(), Function.identity()));

    if (affinityConfig == null) {
      return workerSelector.apply(ImmutableMap.copyOf(runnableWorkers));
    } else {
      // Workers not assigned to any affinity pool at all.
      final Map<String, ImmutableWorkerInfo> nonAffinityWorkers = runnableWorkers
          .entrySet()
          .stream()
          .filter(entry -> !affinityConfig.getAffinityWorkers().contains(entry.getKey()))
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      // Workers assigned to the affinity pool for our task.
      final List<String> dataSourceWorkers = affinityConfig.getAffinity().get(task.getDataSource());

      if (dataSourceWorkers == null) {
        // No affinity config for this dataSource; use non-affinity workers.
        return workerSelector.apply(ImmutableMap.copyOf(nonAffinityWorkers));
      } else {
        final Set<String> dataSourceWorkerHosts = ImmutableSet.copyOf(dataSourceWorkers);

        final Map<String, ImmutableWorkerInfo> dataSourceWorkerMap = runnableWorkers
            .entrySet()
            .stream()
            .filter(entry -> dataSourceWorkerHosts.contains(entry.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        final ImmutableWorkerInfo selected = workerSelector.apply(ImmutableMap.copyOf(dataSourceWorkerMap));

        if (selected != null) {
          return selected;
        } else if (affinityConfig.isStrong()) {
          return null;
        } else {
          // Weak affinity allows us to use nonAffinityWorkers for this dataSource, if no affinity workesr
          // are available.
          return workerSelector.apply(ImmutableMap.copyOf(nonAffinityWorkers));
        }
      }
    }
  }
}
