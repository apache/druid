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

package org.apache.druid.indexing.overlord.autoscaling;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.ImmutableWorkerInfo;
import org.apache.druid.indexing.overlord.TaskRunnerWorkItem;
import org.apache.druid.indexing.overlord.WorkerTaskRunner;
import org.apache.druid.indexing.overlord.setup.CategoriedWorkerBehaviorConfig;
import org.apache.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import org.apache.druid.indexing.overlord.setup.WorkerCategorySpec;
import org.apache.druid.indexing.overlord.setup.WorkerSelectUtils;
import org.apache.druid.indexing.worker.Worker;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

/**
 *
 */
public class SimpleWorkerProvisioningStrategy extends AbstractWorkerProvisioningStrategy
{
  public static final Integer TARGET_WORKER_DEFAULT = -1;
  private static final EmittingLogger log = new EmittingLogger(SimpleWorkerProvisioningStrategy.class);

  private final SimpleWorkerProvisioningConfig config;
  private final Supplier<WorkerBehaviorConfig> workerConfigRef;

  @Inject
  public SimpleWorkerProvisioningStrategy(
      SimpleWorkerProvisioningConfig config,
      Supplier<WorkerBehaviorConfig> workerConfigRef,
      ProvisioningSchedulerConfig provisioningSchedulerConfig
  )
  {
    this(
        config,
        workerConfigRef,
        provisioningSchedulerConfig,
        () -> ScheduledExecutors.fixed(1, "SimpleResourceManagement-manager--%d")
    );
  }

  public SimpleWorkerProvisioningStrategy(
      SimpleWorkerProvisioningConfig config,
      Supplier<WorkerBehaviorConfig> workerConfigRef,
      ProvisioningSchedulerConfig provisioningSchedulerConfig,
      Supplier<ScheduledExecutorService> execFactory
  )
  {
    super(provisioningSchedulerConfig, execFactory);
    this.config = config;
    this.workerConfigRef = workerConfigRef;
  }

  @Override
  public Provisioner makeProvisioner(WorkerTaskRunner runner)
  {
    return new SimpleProvisioner(runner);
  }

  private class SimpleProvisioner implements Provisioner
  {
    private final WorkerTaskRunner runner;
    private final ScalingStats scalingStats = new ScalingStats(config.getNumEventsToTrack());

    private final Map<String, Set<String>> currentlyProvisioningMap = new HashMap<>();
    private final Map<String, Set<String>> currentlyTerminatingMap = new HashMap<>();

    private final Map<String, Integer> targetWorkerCountMap = new HashMap<>();
    private final Map<String, DateTime> lastProvisionTimeMap = new HashMap<>();
    private final Map<String, DateTime> lastTerminateTimeMap = new HashMap<>();

    SimpleProvisioner(WorkerTaskRunner runner)
    {
      this.runner = runner;
    }

    private Map<String, List<TaskRunnerWorkItem>> groupTasksByCategories(
        Collection<? extends TaskRunnerWorkItem> pendingTasks,
        WorkerTaskRunner runner,
        WorkerCategorySpec workerCategorySpec
    )
    {
      Collection<Task> pendingTasksPayload = runner.getPendingTaskPayloads();
      Map<String, List<Task>> taskPayloadsById = pendingTasksPayload.stream()
                                                                    .collect(Collectors.groupingBy(Task::getId));

      return pendingTasks.stream().collect(Collectors.groupingBy(task -> {
        List<Task> taskPayloads = taskPayloadsById.get(task.getTaskId());
        if (taskPayloads == null || taskPayloads.isEmpty()) {
          return CategoriedWorkerBehaviorConfig.DEFAULT_AUTOSCALER_CATEGORY;
        }
        return WorkerSelectUtils.getTaskCategory(
            taskPayloads.get(0),
            workerCategorySpec,
            CategoriedWorkerBehaviorConfig.DEFAULT_AUTOSCALER_CATEGORY
        );
      }));
    }

    @Override
    public synchronized boolean doProvision()
    {
      Collection<? extends TaskRunnerWorkItem> pendingTasks = runner.getPendingTasks();
      log.debug("Pending tasks: %d %s", pendingTasks.size(), pendingTasks);
      Collection<ImmutableWorkerInfo> workers = runner.getWorkers();
      log.debug("Workers: %d %s", workers.size(), workers);
      boolean didProvision = false;
      final CategoriedWorkerBehaviorConfig workerConfig = ProvisioningUtil.getCategoriedWorkerBehaviorConfig(
          workerConfigRef,
          "provision"
      );
      if (workerConfig == null) {
        log.info("No worker config found. Skip provisioning.");
        return false;
      }

      WorkerCategorySpec workerCategorySpec = ProvisioningUtil.getWorkerCategorySpec(workerConfig);

      // Group tasks by categories
      Map<String, List<TaskRunnerWorkItem>> tasksByCategories = groupTasksByCategories(
          pendingTasks,
          runner,
          workerCategorySpec
      );

      Map<String, List<ImmutableWorkerInfo>> workersByCategories = workers.stream().collect(Collectors.groupingBy(
          immutableWorkerInfo -> immutableWorkerInfo.getWorker().getCategory())
      );

      // Merge categories of tasks and workers
      Set<String> allCategories = new HashSet<>(tasksByCategories.keySet());
      allCategories.addAll(workersByCategories.keySet());

      log.debug(
          "Pending Tasks of %d categories (%s), Workers of %d categories (%s). %d common categories: %s",
          tasksByCategories.size(),
          tasksByCategories.keySet(),
          workersByCategories.size(),
          workersByCategories.keySet(),
          allCategories.size(),
          allCategories
      );

      if (allCategories.isEmpty()) {
        // Likely empty categories means initialization.
        // Just try to spinup required amount of workers of each non empty autoscalers
        return initAutoscalers(workerConfig);
      }

      Map<String, AutoScaler> autoscalersByCategory = ProvisioningUtil.mapAutoscalerByCategory(workerConfig.getAutoScalers());

      for (String category : allCategories) {
        List<? extends TaskRunnerWorkItem> categoryTasks = tasksByCategories.getOrDefault(
            category,
            Collections.emptyList()
        );
        AutoScaler categoryAutoscaler = ProvisioningUtil.getAutoscalerByCategory(category, autoscalersByCategory);

        if (categoryAutoscaler == null) {
          log.error("No autoScaler available, cannot execute doProvision for workers of category %s", category);
          continue;
        }
        // Correct category name by selected autoscaler
        category = ProvisioningUtil.getAutoscalerCategory(categoryAutoscaler);

        List<ImmutableWorkerInfo> categoryWorkers = workersByCategories.getOrDefault(category, Collections.emptyList());
        currentlyProvisioningMap.putIfAbsent(category, new HashSet<>());
        Set<String> currentlyProvisioning = this.currentlyProvisioningMap.get(category);
        currentlyTerminatingMap.putIfAbsent(category, new HashSet<>());
        Set<String> currentlyTerminating = this.currentlyTerminatingMap.get(category);

        didProvision = doProvision(
            category,
            categoryWorkers,
            categoryTasks,
            currentlyProvisioning,
            currentlyTerminating,
            categoryAutoscaler
        ) || didProvision;
      }

      return didProvision;
    }

    private boolean doProvision(
        String category,
        Collection<ImmutableWorkerInfo> workers,
        Collection<? extends TaskRunnerWorkItem> pendingTasks,
        Set<String> currentlyProvisioning,
        Set<String> currentlyTerminating,
        AutoScaler<?> autoScaler
    )
    {
      boolean didProvision = false;

      final Predicate<ImmutableWorkerInfo> isValidWorker = ProvisioningUtil.createValidWorkerPredicate(config);
      final int currValidWorkers = Collections2.filter(workers, isValidWorker).size();

      final List<String> workerNodeIds = autoScaler.ipToIdLookup(
          Lists.newArrayList(
              Iterables.transform(
                  workers,
                  input -> input.getWorker().getIp()
              )
          )
      );
      currentlyProvisioning.removeAll(workerNodeIds);

      updateTargetWorkerCount(autoScaler, pendingTasks, workers, category, currentlyProvisioning, currentlyTerminating);
      int targetWorkerCount = targetWorkerCountMap.getOrDefault(category, TARGET_WORKER_DEFAULT);

      int want = targetWorkerCount - (currValidWorkers + currentlyProvisioning.size());
      log.info("Want workers: %d", want);
      while (want > 0) {
        final AutoScalingData provisioned = autoScaler.provision();
        final List<String> newNodes;
        if (provisioned == null || (newNodes = provisioned.getNodeIds()).isEmpty()) {
          log.warn("NewNodes is empty, returning from provision loop");
          break;
        } else {
          currentlyProvisioning.addAll(newNodes);
          lastProvisionTimeMap.put(category, DateTimes.nowUtc());
          scalingStats.addProvisionEvent(provisioned);
          want -= provisioned.getNodeIds().size();
          didProvision = true;
        }
      }

      if (!currentlyProvisioning.isEmpty()) {
        DateTime lastProvisionTime = lastProvisionTimeMap.getOrDefault(category, DateTimes.nowUtc());
        Duration durSinceLastProvision = new Duration(lastProvisionTime, DateTimes.nowUtc());
        log.info("%s provisioning. Current wait time: %s", currentlyProvisioning, durSinceLastProvision);
        if (durSinceLastProvision.isLongerThan(config.getMaxScalingDuration().toStandardDuration())) {
          log.makeAlert("Worker node provisioning taking too long!")
             .addData("millisSinceLastProvision", durSinceLastProvision.getMillis())
             .addData("provisioningCount", currentlyProvisioning.size())
             .emit();

          autoScaler.terminateWithIds(Lists.newArrayList(currentlyProvisioning));
          currentlyProvisioning.clear();
        }
      }

      return didProvision;
    }

    @Override
    public synchronized boolean doTerminate()
    {
      Collection<ImmutableWorkerInfo> workers = runner.getWorkers();
      Collection<? extends TaskRunnerWorkItem> pendingTasks = runner.getPendingTasks();
      final CategoriedWorkerBehaviorConfig workerConfig = ProvisioningUtil.getCategoriedWorkerBehaviorConfig(
          workerConfigRef,
          "terminate"
      );
      if (workerConfig == null) {
        log.info("No worker config found. Skip terminating.");
        return false;
      }

      boolean didTerminate = false;

      WorkerCategorySpec workerCategorySpec = ProvisioningUtil.getWorkerCategorySpec(workerConfig);

      // Group tasks by categories
      Map<String, List<TaskRunnerWorkItem>> pendingTasksByCategories = groupTasksByCategories(
          pendingTasks,
          runner,
          workerCategorySpec
      );

      Map<String, List<ImmutableWorkerInfo>> workersByCategories = workers.stream().collect(Collectors.groupingBy(
          immutableWorkerInfo -> immutableWorkerInfo.getWorker().getCategory())
      );

      Set<String> allCategories = workersByCategories.keySet();
      log.debug(
          "Workers of %d categories: %s",
          workersByCategories.size(),
          allCategories
      );

      Map<String, AutoScaler> autoscalersByCategory = ProvisioningUtil.mapAutoscalerByCategory(workerConfig.getAutoScalers());

      for (String category : allCategories) {
        AutoScaler categoryAutoscaler = ProvisioningUtil.getAutoscalerByCategory(category, autoscalersByCategory);

        if (categoryAutoscaler == null) {
          log.error("No autoScaler available, cannot execute doTerminate for workers of category %s", category);
          continue;
        }

        // Correct category name by selected autoscaler
        category = ProvisioningUtil.getAutoscalerCategory(categoryAutoscaler);
        List<ImmutableWorkerInfo> categoryWorkers = workersByCategories.getOrDefault(category, Collections.emptyList());
        currentlyProvisioningMap.putIfAbsent(category, new HashSet<>());
        Set<String> currentlyProvisioning = this.currentlyProvisioningMap.get(category);
        currentlyTerminatingMap.putIfAbsent(category, new HashSet<>());
        Set<String> currentlyTerminating = this.currentlyTerminatingMap.get(category);
        List<? extends TaskRunnerWorkItem> categoryPendingTasks = pendingTasksByCategories.getOrDefault(
            category,
            Collections.emptyList()
        );

        didTerminate = doTerminate(
            category,
            categoryWorkers,
            currentlyProvisioning,
            currentlyTerminating,
            categoryAutoscaler,
            categoryPendingTasks
        ) || didTerminate;
      }

      return didTerminate;
    }

    private boolean doTerminate(
        String category,
        Collection<ImmutableWorkerInfo> workers,
        Set<String> currentlyProvisioning,
        Set<String> currentlyTerminating,
        AutoScaler<?> autoScaler,
        List<? extends TaskRunnerWorkItem> pendingTasks
    )
    {
      boolean didTerminate = false;

      Collection<Worker> lazyWorkers = ProvisioningUtil.getWorkersOfCategory(runner.getLazyWorkers(), category);
      final Set<String> workerNodeIds = Sets.newHashSet(
          autoScaler.ipToIdLookup(
              Lists.newArrayList(
                  Iterables.transform(
                      lazyWorkers,
                      Worker::getIp
                  )
              )
          )
      );

      currentlyTerminating.retainAll(workerNodeIds);

      updateTargetWorkerCount(autoScaler, pendingTasks, workers, category, currentlyProvisioning, currentlyTerminating);
      int targetWorkerCount = targetWorkerCountMap.getOrDefault(category, TARGET_WORKER_DEFAULT);

      if (currentlyTerminating.isEmpty()) {

        final int excessWorkers = (workers.size() + currentlyProvisioning.size()) - targetWorkerCount;
        if (excessWorkers > 0) {
          final Predicate<ImmutableWorkerInfo> isLazyWorker = ProvisioningUtil.createLazyWorkerPredicate(config);
          final Collection<String> laziestWorkerIps =
              Collections2.transform(
                  runner.markWorkersLazy(isLazyWorker, excessWorkers),
                  new Function<Worker, String>()
                  {
                    @Override
                    public String apply(Worker worker)
                    {
                      return worker.getIp();
                    }
                  }
              );
          if (laziestWorkerIps.isEmpty()) {
            log.info("Wanted to terminate %,d workers, but couldn't find any lazy ones!", excessWorkers);
          } else {
            log.info(
                "Terminating %,d workers (wanted %,d): %s",
                laziestWorkerIps.size(),
                excessWorkers,
                Joiner.on(", ").join(laziestWorkerIps)
            );

            final AutoScalingData terminated = autoScaler.terminate(ImmutableList.copyOf(laziestWorkerIps));
            if (terminated != null) {
              currentlyTerminating.addAll(terminated.getNodeIds());
              lastTerminateTimeMap.put(category, DateTimes.nowUtc());
              scalingStats.addTerminateEvent(terminated);
              didTerminate = true;
            }
          }
        }
      } else {
        DateTime lastTerminateTime = lastTerminateTimeMap.getOrDefault(category, DateTimes.nowUtc());
        Duration durSinceLastTerminate = new Duration(lastTerminateTime, DateTimes.nowUtc());

        log.info("%s terminating. Current wait time: %s", currentlyTerminating, durSinceLastTerminate);

        if (durSinceLastTerminate.isLongerThan(config.getMaxScalingDuration().toStandardDuration())) {
          log.makeAlert("Worker node termination taking too long!")
             .addData("millisSinceLastTerminate", durSinceLastTerminate.getMillis())
             .addData("terminatingCount", currentlyTerminating.size())
             .emit();

          currentlyTerminating.clear();
        }
      }

      return didTerminate;
    }


    private void updateTargetWorkerCount(
        final AutoScaler autoScaler,
        final Collection<? extends TaskRunnerWorkItem> pendingTasks,
        final Collection<ImmutableWorkerInfo> zkWorkers,
        String category,
        Set<String> currentlyProvisioning,
        Set<String> currentlyTerminating
    )
    {
      final Collection<ImmutableWorkerInfo> validWorkers = Collections2.filter(
          zkWorkers,
          ProvisioningUtil.createValidWorkerPredicate(config)
      );
      final Predicate<ImmutableWorkerInfo> isLazyWorker = ProvisioningUtil.createLazyWorkerPredicate(config);
      final int minWorkerCount = autoScaler.getMinNumWorkers();
      final int maxWorkerCount = autoScaler.getMaxNumWorkers();

      if (minWorkerCount > maxWorkerCount) {
        log.error("Huh? minWorkerCount[%d] > maxWorkerCount[%d]. I give up!", minWorkerCount, maxWorkerCount);
        return;
      }

      int targetWorkerCount = targetWorkerCountMap.getOrDefault(category, TARGET_WORKER_DEFAULT);

      if (targetWorkerCount < 0) {
        // Initialize to size of current worker pool, subject to pool size limits
        targetWorkerCount = Math.max(
            Math.min(
                zkWorkers.size(),
                maxWorkerCount
            ),
            minWorkerCount
        );
        log.info(
            "Starting with a target of %,d workers (current = %,d, min = %,d, max = %,d).",
            targetWorkerCount,
            validWorkers.size(),
            minWorkerCount,
            maxWorkerCount
        );
      }

      final boolean notTakingActions = currentlyProvisioning.isEmpty()
                                       && currentlyTerminating.isEmpty();
      final boolean shouldScaleUp = notTakingActions
                                    && validWorkers.size() >= targetWorkerCount
                                    && targetWorkerCount < maxWorkerCount
                                    && (hasTaskPendingBeyondThreshold(pendingTasks)
                                        || targetWorkerCount < minWorkerCount);
      final boolean shouldScaleDown = notTakingActions
                                      && validWorkers.size() == targetWorkerCount
                                      && targetWorkerCount > minWorkerCount
                                      && Iterables.any(validWorkers, isLazyWorker);
      if (shouldScaleUp) {
        targetWorkerCount = Math.max(targetWorkerCount + 1, minWorkerCount);
        log.info(
            "I think we should scale up to %,d workers (current = %,d, min = %,d, max = %,d).",
            targetWorkerCount,
            validWorkers.size(),
            minWorkerCount,
            maxWorkerCount
        );
      } else if (shouldScaleDown) {
        targetWorkerCount = Math.min(targetWorkerCount - 1, maxWorkerCount);
        log.info(
            "I think we should scale down to %,d workers (current = %,d, min = %,d, max = %,d).",
            targetWorkerCount,
            validWorkers.size(),
            minWorkerCount,
            maxWorkerCount
        );
      } else {
        log.info(
            "Our target is %,d workers, and I'm okay with that (current = %,d, min = %,d, max = %,d).",
            targetWorkerCount,
            validWorkers.size(),
            minWorkerCount,
            maxWorkerCount
        );
      }

      targetWorkerCountMap.put(category, targetWorkerCount);
    }

    private boolean hasTaskPendingBeyondThreshold(Collection<? extends TaskRunnerWorkItem> pendingTasks)
    {
      long now = System.currentTimeMillis();
      for (TaskRunnerWorkItem pendingTask : pendingTasks) {
        final Duration durationSinceInsertion = new Duration(pendingTask.getQueueInsertionTime().getMillis(), now);
        final Duration timeoutDuration = config.getPendingTaskTimeout().toStandardDuration();
        if (durationSinceInsertion.isEqual(timeoutDuration) || durationSinceInsertion.isLongerThan(timeoutDuration)) {
          return true;
        }
      }
      return false;
    }

    @Override
    public ScalingStats getStats()
    {
      return scalingStats;
    }

    private boolean initAutoscalers(CategoriedWorkerBehaviorConfig workerConfig)
    {
      boolean didProvision = false;
      for (AutoScaler autoScaler : workerConfig.getAutoScalers()) {
        String category = ProvisioningUtil.getAutoscalerCategory(autoScaler);
        didProvision = initAutoscaler(autoScaler, category) || didProvision;
      }
      return didProvision;
    }

    private boolean initAutoscaler(AutoScaler autoScaler, String category)
    {
      currentlyProvisioningMap.putIfAbsent(category, new HashSet<>());
      Set<String> currentlyProvisioning = this.currentlyProvisioningMap.get(category);
      currentlyTerminatingMap.putIfAbsent(category, new HashSet<>());
      Set<String> currentlyTerminating = this.currentlyTerminatingMap.get(category);
      return doProvision(
          category,
          Collections.emptyList(),
          Collections.emptyList(),
          currentlyProvisioning,
          currentlyTerminating,
          autoScaler
      );
    }
  }
}
