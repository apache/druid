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

package io.druid.indexing.overlord.autoscaling;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.emitter.EmittingLogger;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.ImmutableWorkerInfo;
import io.druid.indexing.overlord.WorkerTaskRunner;
import io.druid.indexing.overlord.config.WorkerTaskRunnerConfig;
import io.druid.indexing.overlord.setup.DefaultWorkerBehaviorConfig;
import io.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import io.druid.indexing.overlord.setup.WorkerSelectStrategy;
import io.druid.indexing.worker.Worker;
import io.druid.java.util.common.DateTimes;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

/**
 */
public class PendingTaskBasedWorkerProvisioningStrategy extends AbstractWorkerProvisioningStrategy
{
  private static final EmittingLogger log = new EmittingLogger(PendingTaskBasedWorkerProvisioningStrategy.class);

  private static final String SCHEME = "http";

  @Nullable
  static DefaultWorkerBehaviorConfig getDefaultWorkerBehaviorConfig(
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
    if (!(workerBehaviorConfig instanceof DefaultWorkerBehaviorConfig)) {
      log.error(
          "Only DefaultWorkerBehaviorConfig is supported as WorkerBehaviorConfig, [%s] given, cannot %s workers",
          workerBehaviorConfig,
          action
      );
      return null;
    }
    final DefaultWorkerBehaviorConfig workerConfig = (DefaultWorkerBehaviorConfig) workerBehaviorConfig;
    if (workerConfig.getAutoScaler() == null) {
      log.error("No autoScaler available, cannot %s workers", action);
      return null;
    }
    return workerConfig;
  }

  private final PendingTaskBasedWorkerProvisioningConfig config;
  private final Supplier<WorkerBehaviorConfig> workerConfigRef;

  @Inject
  public PendingTaskBasedWorkerProvisioningStrategy(
      PendingTaskBasedWorkerProvisioningConfig config,
      Supplier<WorkerBehaviorConfig> workerConfigRef,
      ProvisioningSchedulerConfig provisioningSchedulerConfig
  )
  {
    this(
        config,
        workerConfigRef,
        provisioningSchedulerConfig,
        new Supplier<ScheduledExecutorService>()
        {
          @Override
          public ScheduledExecutorService get()
          {
            return ScheduledExecutors.fixed(1, "PendingTaskBasedWorkerProvisioning-manager--%d");
          }
        }
    );
  }

  public PendingTaskBasedWorkerProvisioningStrategy(
      PendingTaskBasedWorkerProvisioningConfig config,
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
    return new PendingProvisioner(runner);
  }

  private class PendingProvisioner implements Provisioner
  {
    private final WorkerTaskRunner runner;
    private final ScalingStats scalingStats = new ScalingStats(config.getNumEventsToTrack());

    private final Set<String> currentlyProvisioning = Sets.newHashSet();
    private final Set<String> currentlyTerminating = Sets.newHashSet();

    private DateTime lastProvisionTime = DateTimes.nowUtc();
    private DateTime lastTerminateTime = lastProvisionTime;

    private PendingProvisioner(WorkerTaskRunner runner)
    {
      this.runner = runner;
    }

    @Override
    public synchronized boolean doProvision()
    {
      Collection<Task> pendingTasks = runner.getPendingTaskPayloads();
      log.debug("Pending tasks: %d %s", pendingTasks.size(), pendingTasks);
      Collection<ImmutableWorkerInfo> workers = runner.getWorkers();
      log.debug("Workers: %d %s", workers.size(), workers);
      boolean didProvision = false;
      final DefaultWorkerBehaviorConfig workerConfig = getDefaultWorkerBehaviorConfig(workerConfigRef, "provision", log);
      if (workerConfig == null) {
        return false;
      }

      final Collection<String> workerNodeIds = getWorkerNodeIDs(
          Collections2.transform(
              workers,
              new Function<ImmutableWorkerInfo, Worker>()
              {
                @Override
                public Worker apply(ImmutableWorkerInfo input)
                {
                  return input.getWorker();
                }
              }
          ),
          workerConfig
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
            workers
        );
        log.info("Workers to provision: %d", workersToProvision);
        while (workersToProvision > 0) {
          final AutoScalingData provisioned = workerConfig.getAutoScaler().provision();
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

          workerConfig.getAutoScaler().terminateWithIds(Lists.newArrayList(currentlyProvisioning));
          currentlyProvisioning.clear();
        }
      }

      return didProvision;
    }

    private Collection<String> getWorkerNodeIDs(Collection<Worker> workers, DefaultWorkerBehaviorConfig workerConfig)
    {
      List<String> ips = new ArrayList<>(workers.size());
      for (Worker worker : workers) {
        ips.add(worker.getIp());
      }
      List<String> workerNodeIds = workerConfig.getAutoScaler().ipToIdLookup(ips);
      log.info("WorkerNodeIds: %d %s", workerNodeIds.size(), workerNodeIds);
      return workerNodeIds;
    }

    private int getScaleUpNodeCount(
        final WorkerTaskRunnerConfig remoteTaskRunnerConfig,
        final DefaultWorkerBehaviorConfig workerConfig,
        final Collection<Task> pendingTasks,
        final Collection<ImmutableWorkerInfo> workers
    )
    {
      final int minWorkerCount = workerConfig.getAutoScaler().getMinNumWorkers();
      final int maxWorkerCount = workerConfig.getAutoScaler().getMaxNumWorkers();
      log.info("Min/max workers: %d/%d", minWorkerCount, maxWorkerCount);
      final int currValidWorkers = getCurrValidWorkers(workers);

      // If there are no worker, spin up minWorkerCount, we cannot determine the exact capacity here to fulfill the need
      // since we are not aware of the expectedWorkerCapacity.
      int moreWorkersNeeded = currValidWorkers == 0 ? minWorkerCount : getWorkersNeededToAssignTasks(
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
        final DefaultWorkerBehaviorConfig workerConfig,
        final Collection<Task> pendingTasks,
        final Collection<ImmutableWorkerInfo> workers
    )
    {
      final Collection<ImmutableWorkerInfo> validWorkers = Collections2.filter(
          workers,
          ProvisioningUtil.createValidWorkerPredicate(config)
      );
      log.debug("Valid workers: %d %s", validWorkers.size(), validWorkers);

      Map<String, ImmutableWorkerInfo> workersMap = Maps.newHashMap();
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

    @Override
    public synchronized boolean doTerminate()
    {
      Collection<ImmutableWorkerInfo> zkWorkers = runner.getWorkers();
      log.debug("Workers: %d [%s]", zkWorkers.size(), zkWorkers);
      final DefaultWorkerBehaviorConfig workerConfig = getDefaultWorkerBehaviorConfig(workerConfigRef, "terminate", log);
      if (workerConfig == null) {
        return false;
      }

      log.info("Currently provisioning: %d %s", currentlyProvisioning.size(), currentlyProvisioning);
      if (!currentlyProvisioning.isEmpty()) {
        log.debug("Already provisioning nodes, Not Terminating any nodes.");
        return false;
      }

      boolean didTerminate = false;
      final Collection<String> workerNodeIds = getWorkerNodeIDs(runner.getLazyWorkers(), workerConfig);
      log.debug("Currently terminating: %d %s", currentlyTerminating.size(), currentlyTerminating);
      currentlyTerminating.retainAll(workerNodeIds);
      log.debug(
          "Currently terminating among WorkerNodeIds: %d %s",
          currentlyTerminating.size(),
          currentlyTerminating
      );

      if (currentlyTerminating.isEmpty()) {
        final int maxWorkersToTerminate = maxWorkersToTerminate(zkWorkers, workerConfig);
        log.info("Max workers to terminate: %d", maxWorkersToTerminate);
        final Predicate<ImmutableWorkerInfo> isLazyWorker = ProvisioningUtil.createLazyWorkerPredicate(config);
        final Collection<String> laziestWorkerIps =
            Collections2.transform(
                runner.markWorkersLazy(isLazyWorker, maxWorkersToTerminate),
                new Function<Worker, String>()
                {
                  @Override
                  public String apply(Worker zkWorker)
                  {
                    return zkWorker.getIp();
                  }
                }
            );
        log.info("Laziest worker ips: %d %s", laziestWorkerIps.size(), laziestWorkerIps);
        if (laziestWorkerIps.isEmpty()) {
          log.debug("Found no lazy workers");
        } else {
          log.info(
              "Terminating %,d lazy workers: %s",
              laziestWorkerIps.size(),
              Joiner.on(", ").join(laziestWorkerIps)
          );

          final AutoScalingData terminated = workerConfig.getAutoScaler()
                                                         .terminate(ImmutableList.copyOf(laziestWorkerIps));
          if (terminated != null) {
            log.info("Terminated: %d %s", terminated.getNodeIds().size(), terminated.getNodeIds());
            currentlyTerminating.addAll(terminated.getNodeIds());
            lastTerminateTime = DateTimes.nowUtc();
            scalingStats.addTerminateEvent(terminated);
            didTerminate = true;
          }
        }
      } else {
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

    @Override
    public ScalingStats getStats()
    {
      return scalingStats;
    }
  }

  private int maxWorkersToTerminate(Collection<ImmutableWorkerInfo> zkWorkers, DefaultWorkerBehaviorConfig workerConfig)
  {
    final int currValidWorkers = getCurrValidWorkers(zkWorkers);
    final int invalidWorkers = zkWorkers.size() - currValidWorkers;
    final int minWorkers = workerConfig.getAutoScaler().getMinNumWorkers();
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

  private int getCurrValidWorkers(Collection<ImmutableWorkerInfo> workers)
  {
    final Predicate<ImmutableWorkerInfo> isValidWorker = ProvisioningUtil.createValidWorkerPredicate(config);
    final int currValidWorkers = Collections2.filter(workers, isValidWorker).size();
    log.debug("Current valid workers: %d", currValidWorkers);
    return currValidWorkers;
  }

  private static int getExpectedWorkerCapacity(final Collection<ImmutableWorkerInfo> workers)
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

  private static ImmutableWorkerInfo workerWithTask(ImmutableWorkerInfo immutableWorker, Task task)
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

  private static ImmutableWorkerInfo createDummyWorker(String scheme, String host, int capacity, String version)
  {
    return new ImmutableWorkerInfo(
        new Worker(scheme, host, "-2", capacity, version),
        0,
        Sets.<String>newHashSet(),
        Sets.<String>newHashSet(),
        DateTimes.nowUtc()
    );
  }
}
