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
import org.apache.druid.indexing.overlord.CategoryCapacityInfo;
import org.apache.druid.indexing.overlord.ImmutableWorkerInfo;
import org.apache.druid.indexing.overlord.config.WorkerTaskRunnerConfig;
import org.apache.druid.indexing.worker.Worker;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
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
   * @param workerCategorySpec worker category spec, or null
   * @param workerSelector function that receives a list of eligible workers: version is high enough, worker can run
   *                       the task, and worker satisfies the worker category spec. may return null.
   *
   * @return selected worker from "allWorkers", or null.
   */
  @Nullable
  public static ImmutableWorkerInfo selectWorker(
      final Task task,
      final Map<String, ImmutableWorkerInfo> allWorkers,
      final WorkerTaskRunnerConfig workerTaskRunnerConfig,
      @Nullable final WorkerCategorySpec workerCategorySpec,
      final Function<ImmutableMap<String, ImmutableWorkerInfo>, ImmutableWorkerInfo> workerSelector
  )
  {
    final Map<String, ImmutableWorkerInfo> runnableWorkers = getRunnableWorkers(task, allWorkers, workerTaskRunnerConfig);

    // select worker according to worker category spec
    if (workerCategorySpec != null) {
      final WorkerCategorySpec.CategoryConfig categoryConfig = workerCategorySpec.getCategoryMap().get(task.getType());

      if (categoryConfig != null) {
        final String defaultCategory = categoryConfig.getDefaultCategory();
        final Map<String, String> categoryAffinity = categoryConfig.getCategoryAffinity();

        String preferredCategory = categoryAffinity.get(task.getDataSource());
        // If there is no preferred category for the datasource, then using the defaultCategory. However, the defaultCategory
        // may be null too, so we need to do one more null check (see below).
        preferredCategory = preferredCategory == null ? defaultCategory : preferredCategory;

        if (preferredCategory != null) {
          // select worker from preferred category
          final ImmutableMap<String, ImmutableWorkerInfo> categoryWorkers = getCategoryWorkers(preferredCategory, runnableWorkers);
          final ImmutableWorkerInfo selected = workerSelector.apply(categoryWorkers);

          if (selected != null) {
            return selected;
          } else if (workerCategorySpec.isStrong()) {
            return null;
          }
        }
      }
    }

    // select worker from all runnable workers by default
    return workerSelector.apply(ImmutableMap.copyOf(runnableWorkers));
  }

  @Nullable
  public static ImmutableMap<String, CategoryCapacityInfo> getWorkerCategoryCapacity(
      Collection<ImmutableWorkerInfo> workers,
      @Nullable final WorkerCategorySpec workerCategorySpec
  )
  {
    if (workerCategorySpec != null) {
      final Map<String, CategoryCapacityInfo> categoryCapacityMap = new HashMap<>();
      final Map<String, Integer> categoryToCapacityMap = workers.stream()
                                                                .map(ImmutableWorkerInfo::getWorker)
                                                                .collect(Collectors.groupingBy(
                                                                    Worker::getCategory,
                                                                    Collectors.summingInt(Worker::getCapacity)
                                                                ));
      for (Map.Entry<String, WorkerCategorySpec.CategoryConfig> entry : workerCategorySpec.getCategoryMap()
                                                                                          .entrySet()) {
        String taskType = entry.getKey();
        String category = entry.getValue().getDefaultCategory();
        if (!categoryCapacityMap.containsKey(category)) {
          final List<String> taskTypes = new ArrayList<>();
          taskTypes.add(taskType);
          final CategoryCapacityInfo categoryCapacityInfo = new CategoryCapacityInfo(
              taskTypes,
              categoryToCapacityMap.get(category)
          );
          categoryCapacityMap.put(category, categoryCapacityInfo);
        } else {
          final CategoryCapacityInfo categoryCapacityInfo = categoryCapacityMap.get(category);
          if (!categoryCapacityInfo.getTaskTypeList().contains(taskType)) {
            categoryCapacityInfo.getTaskTypeList().add(taskType);
          }
        }
      }
      return ImmutableMap.copyOf(categoryCapacityMap);
    }
    return null;
  }

  // Get workers that could potentially run this task, ignoring affinityConfig/workerCategorySpec.
  private static Map<String, ImmutableWorkerInfo> getRunnableWorkers(
      final Task task,
      final Map<String, ImmutableWorkerInfo> allWorkers,
      final WorkerTaskRunnerConfig workerTaskRunnerConfig
  )
  {
    return allWorkers.values()
                     .stream()
                     .filter(worker -> worker.canRunTask(task, workerTaskRunnerConfig.getParallelIndexTaskSlotRatio())
                                       && worker.isValidVersion(workerTaskRunnerConfig.getMinWorkerVersion()))
                     .collect(Collectors.toMap(w -> w.getWorker().getHost(), Function.identity()));
  }

  /**
   * Return workers belong to this category.
   *
   * @param category worker category name
   * @param workerMap  map of worker hostname to worker info
   *
   * @return map of worker hostname to worker info
   */
  private static ImmutableMap<String, ImmutableWorkerInfo> getCategoryWorkers(
      final String category,
      final Map<String, ImmutableWorkerInfo> workerMap
  )
  {
    return ImmutableMap.copyOf(
        Maps.filterValues(workerMap, workerInfo -> workerInfo.getWorker().getCategory().equals(category))
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
