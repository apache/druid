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
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.ImmutableWorkerInfo;
import io.druid.indexing.overlord.WorkerTaskRunner;
import io.druid.indexing.overlord.config.WorkerTaskRunnerConfig;
import io.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import io.druid.indexing.overlord.setup.WorkerSelectStrategy;
import io.druid.indexing.worker.Worker;
import io.druid.java.util.common.concurrent.ScheduledExecutorFactory;

import org.joda.time.DateTime;
import org.joda.time.Duration;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

/**
 */
public class PendingTaskBasedWorkerResourceManagementStrategy extends AbstractWorkerResourceManagementStrategy
{
  private static final EmittingLogger log = new EmittingLogger(PendingTaskBasedWorkerResourceManagementStrategy.class);

  private final PendingTaskBasedWorkerResourceManagementConfig config;
  private final Supplier<WorkerBehaviorConfig> workerConfigRef;
  private final ScalingStats scalingStats;

  private final Object lock = new Object();
  private final Set<String> currentlyProvisioning = Sets.newHashSet();
  private final Set<String> currentlyTerminating = Sets.newHashSet();

  private DateTime lastProvisionTime = new DateTime();
  private DateTime lastTerminateTime = new DateTime();

  @Inject
  public PendingTaskBasedWorkerResourceManagementStrategy(
      PendingTaskBasedWorkerResourceManagementConfig config,
      Supplier<WorkerBehaviorConfig> workerConfigRef,
      ResourceManagementSchedulerConfig resourceManagementSchedulerConfig,
      ScheduledExecutorFactory factory
  )
  {
    this(
        config,
        workerConfigRef,
        resourceManagementSchedulerConfig,
        factory.create(1, "PendingTaskBasedResourceManagement-manager--%d")
    );
  }

  public PendingTaskBasedWorkerResourceManagementStrategy(
      PendingTaskBasedWorkerResourceManagementConfig config,
      Supplier<WorkerBehaviorConfig> workerConfigRef,
      ResourceManagementSchedulerConfig resourceManagementSchedulerConfig,
      ScheduledExecutorService exec
  )
  {
    super(resourceManagementSchedulerConfig, exec);
    this.config = config;
    this.workerConfigRef = workerConfigRef;
    this.scalingStats = new ScalingStats(config.getNumEventsToTrack());
  }

  @Override
  public boolean doProvision(WorkerTaskRunner runner)
  {
    Collection<Task> pendingTasks = runner.getPendingTaskPayloads();
    Collection<ImmutableWorkerInfo> workers = runner.getWorkers();
    synchronized (lock) {
      boolean didProvision = false;
      final WorkerBehaviorConfig workerConfig = workerConfigRef.get();
      if (workerConfig == null || workerConfig.getAutoScaler() == null) {
        log.error("No workerConfig available, cannot provision new workers.");
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
      currentlyProvisioning.removeAll(workerNodeIds);
      if (currentlyProvisioning.isEmpty()) {
        int want = getScaleUpNodeCount(
            runner.getConfig(),
            workerConfig,
            pendingTasks,
            workers
        );
        while (want > 0) {
          final AutoScalingData provisioned = workerConfig.getAutoScaler().provision();
          final List<String> newNodes = provisioned == null ? ImmutableList.<String>of() : provisioned.getNodeIds();
          if (newNodes.isEmpty()) {
            log.warn("NewNodes is empty, returning from provision loop");
            break;
          } else {
            currentlyProvisioning.addAll(newNodes);
            lastProvisionTime = new DateTime();
            scalingStats.addProvisionEvent(provisioned);
            want -= provisioned.getNodeIds().size();
            didProvision = true;
          }
        }
      } else {
        Duration durSinceLastProvision = new Duration(lastProvisionTime, new DateTime());
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
  }

  private static Collection<String> getWorkerNodeIDs(Collection<Worker> workers, WorkerBehaviorConfig workerConfig)
  {
    return workerConfig.getAutoScaler().ipToIdLookup(
        Lists.newArrayList(
            Iterables.transform(
                workers,
                new Function<Worker, String>()
                {
                  @Override
                  public String apply(Worker input)
                  {
                    return input.getIp();
                  }
                }
            )
        )
    );
  }

  int getScaleUpNodeCount(
      final WorkerTaskRunnerConfig remoteTaskRunnerConfig,
      final WorkerBehaviorConfig workerConfig,
      final Collection<Task> pendingTasks,
      final Collection<ImmutableWorkerInfo> workers
  )
  {
    final int minWorkerCount = workerConfig.getAutoScaler().getMinNumWorkers();
    final int maxWorkerCount = workerConfig.getAutoScaler().getMaxNumWorkers();
    final Predicate<ImmutableWorkerInfo> isValidWorker = ResourceManagementUtil.createValidWorkerPredicate(config);
    final int currValidWorkers = Collections2.filter(workers, isValidWorker).size();

    // If there are no worker, spin up minWorkerCount, we cannot determine the exact capacity here to fulfill the need since
    // we are not aware of the expectedWorkerCapacity.
    int moreWorkersNeeded = currValidWorkers == 0 ? minWorkerCount : getWorkersNeededToAssignTasks(
        remoteTaskRunnerConfig,
        workerConfig,
        pendingTasks,
        workers
    );

    int want = Math.max(
        minWorkerCount - currValidWorkers,
        // Additional workers needed to reach minWorkerCount
        Math.min(config.getMaxScalingStep(), moreWorkersNeeded)
        // Additional workers needed to run current pending tasks
    );

    if (want > 0 && currValidWorkers >= maxWorkerCount) {
      log.warn("Unable to provision more workers. Current workerCount[%d] maximum workerCount[%d].", currValidWorkers, maxWorkerCount);
      return 0;
    }
    want = Math.min(want, maxWorkerCount - currValidWorkers);
    return want;
  }

  int getWorkersNeededToAssignTasks(
      final WorkerTaskRunnerConfig workerTaskRunnerConfig,
      final WorkerBehaviorConfig workerConfig,
      final Collection<Task> pendingTasks,
      final Collection<ImmutableWorkerInfo> workers
  )
  {
    final Collection<ImmutableWorkerInfo> validWorkers = Collections2.filter(
        workers,
        ResourceManagementUtil.createValidWorkerPredicate(config)
    );

    Map<String, ImmutableWorkerInfo> workersMap = Maps.newHashMap();
    for (ImmutableWorkerInfo worker : validWorkers) {
      workersMap.put(worker.getWorker().getHost(), worker);
    }
    WorkerSelectStrategy workerSelectStrategy = workerConfig.getSelectStrategy();
    int need = 0;
    int capacity = getExpectedWorkerCapacity(workers);

    // Simulate assigning tasks to dummy workers using configured workerSelectStrategy
    // the number of additional workers needed to assign all the pending tasks is noted
    for (Task task : pendingTasks) {
      Optional<ImmutableWorkerInfo> selectedWorker = workerSelectStrategy.findWorkerForTask(
          workerTaskRunnerConfig,
          ImmutableMap.copyOf(workersMap),
          task
      );
      final ImmutableWorkerInfo workerRunningTask;
      if (selectedWorker.isPresent()) {
        workerRunningTask = selectedWorker.get();
      } else {
        // None of the existing worker can run this task, we need to provision one worker for it.
        // create a dummy worker and try to simulate assigning task to it.
        workerRunningTask = createDummyWorker("dummy" + need, capacity, workerTaskRunnerConfig.getMinWorkerVersion());
        need++;
      }
      // Update map with worker running task
      workersMap.put(workerRunningTask.getWorker().getHost(), workerWithTask(workerRunningTask, task));
    }
    return need;
  }

  @Override
  public boolean doTerminate(WorkerTaskRunner runner)
  {
    Collection<ImmutableWorkerInfo> zkWorkers = runner.getWorkers();
    synchronized (lock) {
      final WorkerBehaviorConfig workerConfig = workerConfigRef.get();
      if (workerConfig == null) {
        log.warn("No workerConfig available, cannot terminate workers.");
        return false;
      }

      if (!currentlyProvisioning.isEmpty()) {
        log.debug("Already provisioning nodes, Not Terminating any nodes.");
        return false;
      }

      boolean didTerminate = false;
      final Collection<String> workerNodeIds = getWorkerNodeIDs(runner.getLazyWorkers(), workerConfig);
      final Set<String> stillExisting = Sets.newHashSet();
      for (String s : currentlyTerminating) {
        if (workerNodeIds.contains(s)) {
          stillExisting.add(s);
        }
      }
      currentlyTerminating.clear();
      currentlyTerminating.addAll(stillExisting);

      if (currentlyTerminating.isEmpty()) {
        final int maxWorkersToTerminate = maxWorkersToTerminate(zkWorkers, workerConfig);
        final Predicate<ImmutableWorkerInfo> isLazyWorker = ResourceManagementUtil.createLazyWorkerPredicate(config);
        final List<String> laziestWorkerIps =
            Lists.newArrayList(
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
                )
            );
        if (laziestWorkerIps.isEmpty()) {
          log.debug("Found no lazy workers");
        } else {
          log.info(
              "Terminating %,d lazy workers: %s",
              laziestWorkerIps.size(),
              Joiner.on(", ").join(laziestWorkerIps)
          );

          final AutoScalingData terminated = workerConfig.getAutoScaler().terminate(laziestWorkerIps);
          if (terminated != null) {
            currentlyTerminating.addAll(terminated.getNodeIds());
            lastTerminateTime = new DateTime();
            scalingStats.addTerminateEvent(terminated);
            didTerminate = true;
          }
        }
      } else {
        Duration durSinceLastTerminate = new Duration(lastTerminateTime, new DateTime());

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
  }

  private int maxWorkersToTerminate(Collection<ImmutableWorkerInfo> zkWorkers, WorkerBehaviorConfig workerConfig)
  {
    final Predicate<ImmutableWorkerInfo> isValidWorker = ResourceManagementUtil.createValidWorkerPredicate(config);
    final int currValidWorkers = Collections2.filter(zkWorkers, isValidWorker).size();
    final int invalidWorkers = zkWorkers.size() - currValidWorkers;
    final int minWorkers = workerConfig.getAutoScaler().getMinNumWorkers();

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

  @Override
  public ScalingStats getStats()
  {
    return scalingStats;
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
        DateTime.now()
    );
  }

  private static ImmutableWorkerInfo createDummyWorker(String host, int capacity, String version)
  {
    return new ImmutableWorkerInfo(
        new Worker(host, "-2", capacity, version),
        0,
        Sets.<String>newHashSet(),
        Sets.<String>newHashSet(),
        DateTime.now()
    );
  }
}
