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

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.ImmutableWorkerInfo;
import org.apache.druid.indexing.overlord.WorkerTaskRunner;
import org.apache.druid.indexing.overlord.config.WorkerTaskRunnerConfig;
import org.apache.druid.indexing.overlord.setup.CategoriedWorkerBehaviorConfig;
import org.apache.druid.indexing.overlord.setup.CategoriedWorkerSelectStrategy;
import org.apache.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import org.apache.druid.indexing.overlord.setup.WorkerCategorySpec;
import org.apache.druid.indexing.overlord.setup.WorkerSelectStrategy;
import org.apache.druid.indexing.overlord.setup.WorkerSelectUtils;
import org.apache.druid.indexing.worker.Worker;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

@JsonTypeName("categoriedTaskBased")
public class CategoriedProvisioningStrategy extends AbstractWorkerProvisioningStrategy
{
  private static final String SCHEME = "http";
  private static final EmittingLogger log = new EmittingLogger(CategoriedProvisioningStrategy.class);

  private final CategoriedProvisioningConfig config;
  private final Supplier<WorkerBehaviorConfig> workerConfigRef;

  @Nullable
  private static CategoriedWorkerBehaviorConfig getCategoriedWorkerBehaviorConfig(
      Supplier<WorkerBehaviorConfig> workerConfigRef,
      String action,
      EmittingLogger log
  )
  {
    final WorkerBehaviorConfig workerBehaviorConfig = workerConfigRef.get();
    if (workerBehaviorConfig == null) {
      log.error("No workerConfig available, cannot %s workers.", action);
      return null;
    }
    if (!(workerBehaviorConfig instanceof CategoriedWorkerBehaviorConfig)) {
      log.error(
          "Only CategoriedWorkerBehaviorConfig is supported as WorkerBehaviorConfig, [%s] given, cannot %s workers",
          workerBehaviorConfig,
          action
      );
      return null;
    }
    final CategoriedWorkerBehaviorConfig workerConfig = (CategoriedWorkerBehaviorConfig) workerBehaviorConfig;
    if (!(workerConfig.getSelectStrategy() instanceof CategoriedWorkerSelectStrategy)) {
      log.error("Select strategy %s is not supported", workerConfig.getSelectStrategy());
      return null;
    }

    if (workerConfig.getAutoScalers() == null || workerConfig.getAutoScalers().isEmpty()) {
      log.error("At least one autoscaler should be specified.");
      return null;
    }

    return workerConfig;
  }

  @Inject
  public CategoriedProvisioningStrategy(
      CategoriedProvisioningConfig config,
      Supplier<WorkerBehaviorConfig> workerConfigRef,
      ProvisioningSchedulerConfig provisioningSchedulerConfig
  )
  {
    this(
        config,
        workerConfigRef,
        provisioningSchedulerConfig,
        () -> ScheduledExecutors.fixed(1, "CategoriedProvisioning-manager--%d")
    );
  }

  public CategoriedProvisioningStrategy(
      CategoriedProvisioningConfig config,
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
  protected Provisioner makeProvisioner(WorkerTaskRunner runner)
  {
    return new CategoriedProvisioner(runner);
  }

  private class CategoriedProvisioner implements Provisioner
  {
    private final WorkerTaskRunner runner;
    private final ScalingStats scalingStats = new ScalingStats(config.getNumEventsToTrack());

    private final Map<String, Set<String>> currentlyProvisioning = new HashMap<>();
    private final Map<String, Set<String>> currentlyTerminating = new HashMap<>();

    private DateTime lastProvisionTime = DateTimes.nowUtc();
    private DateTime lastTerminateTime = DateTimes.nowUtc();

    private CategoriedProvisioner(WorkerTaskRunner runner)
    {
      this.runner = runner;
    }

    @Override
    public boolean doTerminate()
    {
      Collection<ImmutableWorkerInfo> zkWorkers = runner.getWorkers();
      log.debug("Workers: %d [%s]", zkWorkers.size(), zkWorkers);
      final CategoriedWorkerBehaviorConfig workerConfig = getCategoriedWorkerBehaviorConfig(
          workerConfigRef,
          "terminate",
          log
      );
      if (workerConfig == null) {
        return false;
      }

      boolean didTerminate = false;

      Map<String, List<ImmutableWorkerInfo>> workersByCategories = zkWorkers.stream().collect(Collectors.groupingBy(
          immutableWorkerInfo -> immutableWorkerInfo.getWorker().getCategory())
      );

      Set<String> allCategories = workersByCategories.keySet();
      log.debug(
          "Workers of %d categories: %s",
          workersByCategories.size(),
          allCategories
      );

      Map<String, AutoScaler> autoscalersByCategory = mapAutoscalerByCategory(workerConfig.getAutoScalers());

      for (String category : allCategories) {
        Set<String> currentlyProvisioning = this.currentlyProvisioning.getOrDefault(category, Collections.emptySet());
        log.info(
            "Currently provisioning of category %s: %d %s",
            category,
            currentlyProvisioning.size(),
            currentlyProvisioning
        );
        if (!currentlyProvisioning.isEmpty()) {
          log.debug("Already provisioning nodes of category %s, Not Terminating any nodes.", category);
          return false;
        }

        List<ImmutableWorkerInfo> categoryWorkers = workersByCategories.getOrDefault(category, Collections.emptyList());
        currentlyTerminating.putIfAbsent(category, new HashSet<>());
        Set<String> currentlyTerminating = this.currentlyTerminating.get(category);
        AutoScaler groupAutoscaler = getCategoryAutoscaler(category, autoscalersByCategory);

        didTerminate = doTerminate(
            category,
            categoryWorkers,
            currentlyTerminating,
            groupAutoscaler
        ) || didTerminate;
      }

      return didTerminate;
    }

    private boolean doTerminate(
        String category,
        Collection<ImmutableWorkerInfo> zkWorkers,
        Set<String> currentlyTerminating,
        AutoScaler autoScaler
    )
    {
      if (autoScaler == null) {
        log.error("No autoScaler available, cannot execute doTerminate for workers of category %s", category);
        return false;
      }

      boolean didTerminate = false;
      final Collection<String> workerNodeIds = getWorkerNodeIDs(runner.getLazyWorkers(), autoScaler);
      log.debug(
          "Currently terminating of category %s: %d %s",
          category,
          currentlyTerminating.size(),
          currentlyTerminating
      );
      currentlyTerminating.retainAll(workerNodeIds);
      log.debug(
          "Currently terminating of category %s among WorkerNodeIds: %d %s",
          category,
          currentlyTerminating.size(),
          currentlyTerminating
      );

      if (currentlyTerminating.isEmpty()) {
        final int maxWorkersToTerminate = maxWorkersToTerminate(zkWorkers, autoScaler);
        log.info("Max workers to terminate of category %s: %d", category, maxWorkersToTerminate);
        final Predicate<ImmutableWorkerInfo> isLazyWorker = ProvisioningUtil.createLazyWorkerPredicate(config);
        final Collection<String> laziestWorkerIps =
            Collections2.transform(
                runner.markWorkersLazy(isLazyWorker, maxWorkersToTerminate),
                Worker::getIp
            );
        log.info("Laziest worker ips of category %s: %d %s", category, laziestWorkerIps.size(), laziestWorkerIps);
        if (laziestWorkerIps.isEmpty()) {
          log.debug("Found no lazy workers for category %s", category);
        } else {
          log.info(
              "Terminating %,d lazy workers of category %s: %s",
              laziestWorkerIps.size(),
              category,
              Joiner.on(", ").join(laziestWorkerIps)
          );

          final AutoScalingData terminated = autoScaler.terminate(ImmutableList.copyOf(laziestWorkerIps));
          if (terminated != null) {
            log.info(
                "Terminated of category %s: %d %s",
                category,
                terminated.getNodeIds().size(),
                terminated.getNodeIds()
            );
            currentlyTerminating.addAll(terminated.getNodeIds());
            lastTerminateTime = DateTimes.nowUtc();
            scalingStats.addTerminateEvent(terminated);
            didTerminate = true;
          }
        }
      } else {
        Duration durSinceLastTerminate = new Duration(lastTerminateTime, DateTimes.nowUtc());

        log.info(
            "%s terminating of category %s. Current wait time: %s",
            currentlyTerminating,
            category,
            durSinceLastTerminate
        );

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

    @Override
    public boolean doProvision()
    {
      Collection<Task> pendingTasks = runner.getPendingTaskPayloads();
      log.debug("Pending tasks: %d %s", pendingTasks.size(), pendingTasks);
      Collection<ImmutableWorkerInfo> workers = runner.getWorkers();
      log.debug("Workers: %d %s", workers.size(), workers);
      boolean didProvision = false;
      final CategoriedWorkerBehaviorConfig workerConfig = getCategoriedWorkerBehaviorConfig(
          workerConfigRef,
          "provision",
          log
      );
      if (workerConfig == null) {
        log.info("No worker config found. Skip provisioning.");
        return false;
      }

      WorkerCategorySpec workerCategorySpec = getWorkerCategorySpec(workerConfig);

      // Group tasks by categories
      Map<String, List<Task>> tasksByCategories = pendingTasks.stream().collect(Collectors.groupingBy(
          task -> WorkerSelectUtils.getTaskCategory(
              task,
              workerCategorySpec,
              CategoriedWorkerBehaviorConfig.DEFAULT_AUTOSCALER_CATEGORY
          )
      ));

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
        // Likely empty categories means initialization. Just try to spinup required amount of workers of each non empty autoscalers
        for (AutoScaler autoScaler : workerConfig.getAutoScalers()) {
          String category = autoScaler.getCategory();
          didProvision = initAutoscaler(autoScaler, category, workerConfig) || didProvision;
        }
        return didProvision;
      }

      Map<String, AutoScaler> autoscalersByCategory = mapAutoscalerByCategory(workerConfig.getAutoScalers());

      for (String category : allCategories) {
        List<Task> categoryTasks = tasksByCategories.getOrDefault(category, Collections.emptyList());
        List<ImmutableWorkerInfo> categoryWorkers = workersByCategories.getOrDefault(category, Collections.emptyList());
        currentlyProvisioning.putIfAbsent(category, new HashSet<>());
        Set<String> currentlyProvisioning = this.currentlyProvisioning.get(category);
        AutoScaler groupAutoscaler = getCategoryAutoscaler(category, autoscalersByCategory);

        didProvision = doProvision(
            category,
            categoryWorkers,
            categoryTasks,
            workerConfig,
            currentlyProvisioning,
            groupAutoscaler
        ) || didProvision;
      }

      return didProvision;
    }

    private boolean doProvision(
        String category,
        Collection<ImmutableWorkerInfo> workers,
        Collection<Task> pendingTasks,
        CategoriedWorkerBehaviorConfig workerConfig,
        Set<String> currentlyProvisioning,
        AutoScaler autoScaler
    )
    {
      boolean didProvision = false;

      if (autoScaler == null) {
        log.error("No autoScaler available, cannot execute doProvision for workers of category %s", category);
        return false;
      }

      final Collection<String> workerNodeIds = getWorkerNodeIDs(
          Collections2.transform(
              workers,
              ImmutableWorkerInfo::getWorker
          ),
          autoScaler
      );
      log.info("Currently provisioning: %d %s", currentlyProvisioning.size(), currentlyProvisioning);
      currentlyProvisioning.removeAll(workerNodeIds);
      log.debug(
          "Currently provisioning without WorkerNodeIds: %d %s",
          currentlyProvisioning.size(),
          currentlyProvisioning
      );

      if (currentlyProvisioning.isEmpty()) {
        int workersToProvision = getScaleUpNodeCount(
            runner.getConfig(),
            workerConfig,
            pendingTasks,
            workers,
            autoScaler
        );
        log.info("Workers to provision: %d", workersToProvision);
        while (workersToProvision > 0) {
          final AutoScalingData provisioned = autoScaler.provision();
          final List<String> newNodes;
          if (provisioned == null || (newNodes = provisioned.getNodeIds()).isEmpty()) {
            log.warn("NewNodes is empty, returning from provision loop");
            break;
          } else {
            log.info("Provisioned: %d [%s]", provisioned.getNodeIds().size(), provisioned.getNodeIds());
            currentlyProvisioning.addAll(newNodes);
            lastProvisionTime = DateTimes.nowUtc();
            scalingStats.addProvisionEvent(provisioned);
            workersToProvision -= provisioned.getNodeIds().size();
            didProvision = true;
          }
        }
      } else {
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
    public ScalingStats getStats()
    {
      return scalingStats;
    }

    private Collection<String> getWorkerNodeIDs(Collection<Worker> workers, AutoScaler<?> autoScaler)
    {
      List<String> ips = new ArrayList<>(workers.size());
      for (Worker worker : workers) {
        ips.add(worker.getIp());
      }
      List<String> workerNodeIds = autoScaler.ipToIdLookup(ips);
      log.info("WorkerNodeIds: %d %s", workerNodeIds.size(), workerNodeIds);
      return workerNodeIds;
    }

    private int getScaleUpNodeCount(
        final WorkerTaskRunnerConfig remoteTaskRunnerConfig,
        final CategoriedWorkerBehaviorConfig workerConfig,
        final Collection<Task> pendingTasks,
        final Collection<ImmutableWorkerInfo> workers,
        AutoScaler autoScaler
    )
    {
      final int minWorkerCount = autoScaler.getMinNumWorkers();
      final int maxWorkerCount = autoScaler.getMaxNumWorkers();
      log.info("Min/max workers: %d/%d", minWorkerCount, maxWorkerCount);
      final int currValidWorkers = getCurrValidWorkers(workers);

      // If there are no worker, spin up minWorkerCount (or 1 if minWorkerCount is 0), we cannot determine the exact capacity here to fulfill the need
      // since we are not aware of the expectedWorkerCapacity.
      int moreWorkersNeeded = currValidWorkers == 0 ? Math.max(minWorkerCount, 1) : getWorkersNeededToAssignTasks(
          remoteTaskRunnerConfig,
          workerConfig,
          pendingTasks,
          workers
      );
      log.debug("More workers needed: %d", moreWorkersNeeded);

      int want = Math.max(
          minWorkerCount - currValidWorkers,
          // Additional workers needed to reach minWorkerCount
          Math.min(config.getMaxScalingStep(), moreWorkersNeeded)
          // Additional workers needed to run current pending tasks
      );
      log.info("Want workers: %d", want);

      if (want > 0 && currValidWorkers >= maxWorkerCount) {
        log.warn(
            "Unable to provision more workers. Current workerCount[%d] maximum workerCount[%d].",
            currValidWorkers,
            maxWorkerCount
        );
        return 0;
      }
      want = Math.min(want, maxWorkerCount - currValidWorkers);
      return want;
    }

    private int getWorkersNeededToAssignTasks(
        final WorkerTaskRunnerConfig workerTaskRunnerConfig,
        final CategoriedWorkerBehaviorConfig workerConfig,
        final Collection<Task> pendingTasks,
        final Collection<ImmutableWorkerInfo> workers
    )
    {
      final Collection<ImmutableWorkerInfo> validWorkers = Collections2.filter(
          workers,
          ProvisioningUtil.createValidWorkerPredicate(config)
      );
      log.debug("Valid workers: %d %s", validWorkers.size(), validWorkers);

      Map<String, ImmutableWorkerInfo> workersMap = new HashMap<>();
      for (ImmutableWorkerInfo worker : validWorkers) {
        workersMap.put(worker.getWorker().getHost(), worker);
      }
      WorkerSelectStrategy workerSelectStrategy = workerConfig.getSelectStrategy();
      int need = 0;
      int capacity = getExpectedWorkerCapacity(workers);
      log.info("Expected worker capacity: %d", capacity);

      // Simulate assigning tasks to dummy workers using configured workerSelectStrategy
      // the number of additional workers needed to assign all the pending tasks is noted
      for (Task task : pendingTasks) {
        final ImmutableWorkerInfo selectedWorker = workerSelectStrategy.findWorkerForTask(
            workerTaskRunnerConfig,
            ImmutableMap.copyOf(workersMap),
            task
        );
        final ImmutableWorkerInfo workerRunningTask;
        if (selectedWorker != null) {
          workerRunningTask = selectedWorker;
          log.debug("Worker[%s] able to take the task[%s]", task, workerRunningTask);
        } else {
          // None of the existing worker can run this task, we need to provision one worker for it.
          // create a dummy worker and try to simulate assigning task to it.
          workerRunningTask = createDummyWorker(
              SCHEME,
              "dummy" + need,
              capacity,
              workerTaskRunnerConfig.getMinWorkerVersion()
          );
          log.debug("Need more workers, creating a dummy worker[%s]", workerRunningTask);
          need++;
        }
        // Update map with worker running task
        workersMap.put(workerRunningTask.getWorker().getHost(), workerWithTask(workerRunningTask, task));
      }
      return need;
    }

    private int getCurrValidWorkers(Collection<ImmutableWorkerInfo> workers)
    {
      final Predicate<ImmutableWorkerInfo> isValidWorker = ProvisioningUtil.createValidWorkerPredicate(config);
      final int currValidWorkers = Collections2.filter(workers, isValidWorker).size();
      log.debug("Current valid workers: %d", currValidWorkers);
      return currValidWorkers;
    }

    private int getExpectedWorkerCapacity(final Collection<ImmutableWorkerInfo> workers)
    {
      int size = workers.size();
      if (size == 0) {
        // No existing workers assume capacity per worker as 1
        return 1;
      } else {
        // Assume all workers have same capacity
        return workers.iterator().next().getWorker().getCapacity();
      }
    }

    private ImmutableWorkerInfo createDummyWorker(String scheme, String host, int capacity, String version)
    {
      return new ImmutableWorkerInfo(
          new Worker(scheme, host, "-2", capacity, version, WorkerConfig.DEFAULT_CATEGORY),
          0,
          new HashSet<>(),
          new HashSet<>(),
          DateTimes.nowUtc()
      );
    }

    private ImmutableWorkerInfo workerWithTask(ImmutableWorkerInfo immutableWorker, Task task)
    {
      return new ImmutableWorkerInfo(
          immutableWorker.getWorker(),
          immutableWorker.getCurrCapacityUsed() + 1,
          Sets.union(
              immutableWorker.getAvailabilityGroups(),
              Sets.newHashSet(
                  task.getTaskResource()
                      .getAvailabilityGroup()
              )
          ),
          Sets.union(
              immutableWorker.getRunningTasks(),
              Sets.newHashSet(
                  task.getId()
              )
          ),
          DateTimes.nowUtc()
      );
    }

    private int maxWorkersToTerminate(Collection<ImmutableWorkerInfo> zkWorkers, AutoScaler autoScaler)
    {
      final int currValidWorkers = getCurrValidWorkers(zkWorkers);
      final int invalidWorkers = zkWorkers.size() - currValidWorkers;
      final int minWorkers = autoScaler.getMinNumWorkers();
      log.info("Min workers: %d", minWorkers);

      // Max workers that can be terminated
      // All invalid workers + any lazy workers above minCapacity
      return invalidWorkers + Math.max(
          0,
          Math.min(
              config.getMaxScalingStep(),
              currValidWorkers - minWorkers
          )
      );
    }

    private boolean initAutoscaler(AutoScaler autoScaler, String category, CategoriedWorkerBehaviorConfig workerConfig)
    {
      currentlyProvisioning.putIfAbsent(
          category,
          new HashSet<>()
      );
      Set<String> currentlyProvisioning = this.currentlyProvisioning.get(category);
      return doProvision(
          category,
          Collections.emptyList(),
          Collections.emptyList(),
          workerConfig,
          currentlyProvisioning,
          autoScaler
      );
    }

    @Nullable
    private AutoScaler getCategoryAutoscaler(String category, Map<String, AutoScaler> autoscalersByCategory)
    {
      AutoScaler autoScaler = autoscalersByCategory.get(category);
      boolean isStrongAssignment = !autoscalersByCategory.containsKey(CategoriedWorkerBehaviorConfig.DEFAULT_AUTOSCALER_CATEGORY);

      if (autoScaler == null && isStrongAssignment) {
        log.warn(
            "No autoscaler found for category %s. Tasks of this category will not be assigned to default autoscaler because of strong affinity.",
            category
        );
        return null;
      }
      return autoScaler == null
             ? autoscalersByCategory.get(CategoriedWorkerBehaviorConfig.DEFAULT_AUTOSCALER_CATEGORY)
             : autoScaler;
    }

    private Map<String, AutoScaler> mapAutoscalerByCategory(List<AutoScaler> autoScalers)
    {
      Map<String, AutoScaler> result = autoScalers.stream().collect(Collectors.groupingBy(
          autoScaler -> autoScaler.getCategory() == null
                        ? CategoriedWorkerBehaviorConfig.DEFAULT_AUTOSCALER_CATEGORY
                        : autoScaler.getCategory(),
          Collectors.collectingAndThen(Collectors.toList(), values -> values.get(0))
      ));

      if (result.size() != autoScalers.size()) {
        log.warn(
            "Probably autoscalers with duplicated categories were defined. The first instance of each duplicate category will be used.");
      }

      return result;
    }

    @Nullable
    private WorkerCategorySpec getWorkerCategorySpec(CategoriedWorkerBehaviorConfig workerConfig)
    {
      if (workerConfig != null && workerConfig.getSelectStrategy() != null) {
        WorkerSelectStrategy selectStrategy = workerConfig.getSelectStrategy();
        if (selectStrategy instanceof CategoriedWorkerSelectStrategy) {
          return ((CategoriedWorkerSelectStrategy) selectStrategy).getWorkerCategorySpec();
        }
      }
      return null;
    }
  }
}
