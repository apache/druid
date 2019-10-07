/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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

package org.apache.druid.indexing.overlord.setup;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.ImmutableWorkerInfo;
import org.apache.druid.indexing.overlord.config.WorkerTaskRunnerConfig;

import javax.annotation.Nullable;
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
    final Map<String, ImmutableWorkerInfo> runnableWorkers = getRunnableWorkers(task, allWorkers, workerTaskRunnerConfig);

    if (affinityConfig == null) {
      // All runnable workers are valid.
      return workerSelector.apply(ImmutableMap.copyOf(runnableWorkers));
    } else {
      // Workers assigned to the affinity pool for our task.
      final Set<String> dataSourceWorkers = affinityConfig.getAffinity().get(task.getDataSource());

      if (dataSourceWorkers == null) {
        // No affinity config for this dataSource; use non-affinity workers.
        return workerSelector.apply(getNonAffinityWorkers(affinityConfig, runnableWorkers));
      } else {
        // Get runnable, affinity workers.
        final ImmutableMap<String, ImmutableWorkerInfo> dataSourceWorkerMap =
            ImmutableMap.copyOf(Maps.filterKeys(runnableWorkers, dataSourceWorkers::contains));

        final ImmutableWorkerInfo selected = workerSelector.apply(dataSourceWorkerMap);

        if (selected != null) {
          return selected;
        } else if (affinityConfig.isStrong()) {
          return null;
        } else {
          // Weak affinity allows us to use nonAffinityWorkers for this dataSource, if no affinity workers
          // are available.
          return workerSelector.apply(getNonAffinityWorkers(affinityConfig, runnableWorkers));
        }
      }
    }
  }

  /**
   * Helper for {@link WorkerSelectStrategy} implementations.
   *
   * @param allWorkers     map of all workers, in the style provided to {@link WorkerSelectStrategy}
   * @param workerTierSpec worker tier spec, or null
   * @param workerSelector function that receives a list of eligible workers: version is high enough, worker can run
   *                       the task, and worker satisfies the worker tier spec. may return null.
   *
   * @return selected worker from "allWorkers", or null.
   */
  @Nullable
  public static ImmutableWorkerInfo selectWorker(
      final Task task,
      final Map<String, ImmutableWorkerInfo> allWorkers,
      final WorkerTaskRunnerConfig workerTaskRunnerConfig,
      @Nullable final WorkerTierSpec workerTierSpec,
      final Function<ImmutableMap<String, ImmutableWorkerInfo>, ImmutableWorkerInfo> workerSelector
  )
  {
    final Map<String, ImmutableWorkerInfo> runnableWorkers = getRunnableWorkers(task, allWorkers, workerTaskRunnerConfig);

    // select worker according to worker tier spec
    if (workerTierSpec != null) {
      final WorkerTierSpec.TierConfig tierConfig = workerTierSpec.getTierMap().get(task.getType());

      if (tierConfig != null) {
        final String defaultTier = tierConfig.getDefaultTier();
        final Map<String, String> tierAffinity = tierConfig.getTierAffinity();

        String preferredTier = tierAffinity.get(task.getDataSource());
        // If there is no preferred tier for the datasource, then using the defaultTier. However, the defaultTier
        // may be null too, so we need to do one more null check (see below).
        preferredTier = preferredTier == null ? defaultTier : preferredTier;

        if (preferredTier != null) {
          // select worker from preferred tier
          final ImmutableMap<String, ImmutableWorkerInfo> tierWorkers = getTierWorkers(preferredTier, runnableWorkers);
          final ImmutableWorkerInfo selected = workerSelector.apply(tierWorkers);

          if (selected != null) {
            return selected;
          } else if (workerTierSpec.isStrong()) {
            return null;
          }
        }
      }
    }

    // select worker from all runnable workers by default
    return workerSelector.apply(ImmutableMap.copyOf(runnableWorkers));
  }

  // Get workers that could potentially run this task, ignoring affinityConfig/workerTierSpec.
  private static Map<String, ImmutableWorkerInfo> getRunnableWorkers(
      final Task task,
      final Map<String, ImmutableWorkerInfo> allWorkers,
      final WorkerTaskRunnerConfig workerTaskRunnerConfig
  )
  {
    return allWorkers.values()
                     .stream()
                     .filter(worker -> worker.canRunTask(task)
                                       && worker.isValidVersion(workerTaskRunnerConfig.getMinWorkerVersion()))
                     .collect(Collectors.toMap(w -> w.getWorker().getHost(), Function.identity()));
  }

  /**
   * Return workers belong to this tier.
   *
   * @param tier worker tier name
   * @param workerMap  map of worker hostname to worker info
   *
   * @return map of worker hostname to worker info
   */
  private static ImmutableMap<String, ImmutableWorkerInfo> getTierWorkers(
      final String tier,
      final Map<String, ImmutableWorkerInfo> workerMap
  )
  {
    return ImmutableMap.copyOf(
        Maps.filterValues(workerMap, workerInfo -> workerInfo.getWorker().getTier().equals(tier))
    );
  }

  /**
   * Return workers not assigned to any affinity pool at all.
   *
   * @param affinityConfig affinity config
   * @param workerMap      map of worker hostname to worker info
   *
   * @return map of worker hostname to worker info
   */
  private static ImmutableMap<String, ImmutableWorkerInfo> getNonAffinityWorkers(
      final AffinityConfig affinityConfig,
      final Map<String, ImmutableWorkerInfo> workerMap
  )
  {
    return ImmutableMap.copyOf(
        Maps.filterKeys(
            workerMap,
            workerHost -> !affinityConfig.getAffinityWorkers().contains(workerHost)
        )
    );
  }
}
