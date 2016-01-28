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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.concurrent.ScheduledExecutorFactory;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.emitter.EmittingLogger;
import io.druid.granularity.PeriodGranularity;
import io.druid.indexing.overlord.RemoteTaskRunner;
import io.druid.indexing.overlord.TaskRunnerWorkItem;
import io.druid.indexing.overlord.ZkWorker;
import io.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Period;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

/**
 */
public class SimpleResourceManagementStrategy implements ResourceManagementStrategy<RemoteTaskRunner>
{
  private static final EmittingLogger log = new EmittingLogger(SimpleResourceManagementStrategy.class);

  private final ResourceManagementSchedulerConfig resourceManagementSchedulerConfig;
  private final ScheduledExecutorService exec;

  private volatile boolean started = false;

  private final SimpleResourceManagementConfig config;
  private final Supplier<WorkerBehaviorConfig> workerConfigRef;
  private final ScalingStats scalingStats;

  private final Object lock = new Object();
  private final Set<String> currentlyProvisioning = Sets.newHashSet();
  private final Set<String> currentlyTerminating = Sets.newHashSet();

  private int targetWorkerCount = -1;
  private DateTime lastProvisionTime = new DateTime();
  private DateTime lastTerminateTime = new DateTime();

  @Inject
  public SimpleResourceManagementStrategy(
      SimpleResourceManagementConfig config,
      Supplier<WorkerBehaviorConfig> workerConfigRef,
      ResourceManagementSchedulerConfig resourceManagementSchedulerConfig,
      ScheduledExecutorFactory factory
  )
  {
    this(
        config,
        workerConfigRef,
        resourceManagementSchedulerConfig,
        factory.create(1, "SimpleResourceManagement-manager--%d")
    );
  }

  public SimpleResourceManagementStrategy(
      SimpleResourceManagementConfig config,
      Supplier<WorkerBehaviorConfig> workerConfigRef,
      ResourceManagementSchedulerConfig resourceManagementSchedulerConfig,
      ScheduledExecutorService exec
  )
  {
    this.config = config;
    this.workerConfigRef = workerConfigRef;
    this.scalingStats = new ScalingStats(config.getNumEventsToTrack());
    this.resourceManagementSchedulerConfig = resourceManagementSchedulerConfig;
    this.exec = exec;
  }

  boolean doProvision(RemoteTaskRunner runner)
  {
    Collection<? extends TaskRunnerWorkItem> pendingTasks = runner.getPendingTasks();
    Collection<ZkWorker> zkWorkers = getWorkers(runner);
    synchronized (lock) {
      boolean didProvision = false;
      final WorkerBehaviorConfig workerConfig = workerConfigRef.get();
      if (workerConfig == null || workerConfig.getAutoScaler() == null) {
        log.warn("No workerConfig available, cannot provision new workers.");
        return false;
      }
      final Predicate<ZkWorker> isValidWorker = createValidWorkerPredicate(config);
      final int currValidWorkers = Collections2.filter(zkWorkers, isValidWorker).size();

      final List<String> workerNodeIds = workerConfig.getAutoScaler().ipToIdLookup(
          Lists.newArrayList(
              Iterables.<ZkWorker, String>transform(
                  zkWorkers,
                  new Function<ZkWorker, String>()
                  {
                    @Override
                    public String apply(ZkWorker input)
                    {
                      return input.getWorker().getIp();
                    }
                  }
              )
          )
      );
      currentlyProvisioning.removeAll(workerNodeIds);

      updateTargetWorkerCount(workerConfig, pendingTasks, zkWorkers);

      int want = targetWorkerCount - (currValidWorkers + currentlyProvisioning.size());
      while (want > 0) {
        final AutoScalingData provisioned = workerConfig.getAutoScaler().provision();
        final List<String> newNodes;
        if (provisioned == null || (newNodes = provisioned.getNodeIds()).isEmpty()) {
          break;
        } else {
          currentlyProvisioning.addAll(newNodes);
          lastProvisionTime = new DateTime();
          scalingStats.addProvisionEvent(provisioned);
          want -= provisioned.getNodeIds().size();
          didProvision = true;
        }
      }

      if (!currentlyProvisioning.isEmpty()) {
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

  boolean doTerminate(RemoteTaskRunner runner)
  {
    Collection<? extends TaskRunnerWorkItem> pendingTasks = runner.getPendingTasks();
    synchronized (lock) {
      final WorkerBehaviorConfig workerConfig = workerConfigRef.get();
      if (workerConfig == null) {
        log.warn("No workerConfig available, cannot terminate workers.");
        return false;
      }

      boolean didTerminate = false;
      final Set<String> workerNodeIds = Sets.newHashSet(
          workerConfig.getAutoScaler().ipToIdLookup(
              Lists.newArrayList(
                  Iterables.transform(
                      runner.getLazyWorkers(),
                      new Function<ZkWorker, String>()
                      {
                        @Override
                        public String apply(ZkWorker input)
                        {
                          return input.getWorker().getIp();
                        }
                      }
                  )
              )
          )
      );

      final Set<String> stillExisting = Sets.newHashSet();
      for (String s : currentlyTerminating) {
        if (workerNodeIds.contains(s)) {
          stillExisting.add(s);
        }
      }
      currentlyTerminating.clear();
      currentlyTerminating.addAll(stillExisting);

      Collection<ZkWorker> workers = getWorkers(runner);
      updateTargetWorkerCount(workerConfig, pendingTasks, workers);

      if (currentlyTerminating.isEmpty()) {

        final int excessWorkers = (workers.size() + currentlyProvisioning.size()) - targetWorkerCount;
        if (excessWorkers > 0) {
          final Predicate<ZkWorker> isLazyWorker = createLazyWorkerPredicate(config);
          final List<String> laziestWorkerIps =
              Lists.transform(
                  runner.markWorkersLazy(isLazyWorker, excessWorkers),
                  new Function<ZkWorker, String>()
                  {
                    @Override
                    public String apply(ZkWorker zkWorker)
                    {
                      return zkWorker.getWorker().getIp();
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

            final AutoScalingData terminated = workerConfig.getAutoScaler().terminate(laziestWorkerIps);
            if (terminated != null) {
              currentlyTerminating.addAll(terminated.getNodeIds());
              lastTerminateTime = new DateTime();
              scalingStats.addTerminateEvent(terminated);
              didTerminate = true;
            }
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

  @Override
  public void startManagement(final RemoteTaskRunner runner)
  {
    synchronized (lock) {
      if (started) {
        return;
      }

      log.info("Started Resource Management Scheduler");

      ScheduledExecutors.scheduleAtFixedRate(
          exec,
          resourceManagementSchedulerConfig.getProvisionPeriod().toStandardDuration(),
          new Runnable()
          {
            @Override
            public void run()
            {
              doProvision(runner);
            }
          }
      );

      // Schedule termination of worker nodes periodically
      Period period = resourceManagementSchedulerConfig.getTerminatePeriod();
      PeriodGranularity granularity = new PeriodGranularity(
          period,
          resourceManagementSchedulerConfig.getOriginTime(),
          null
      );
      final long startTime = granularity.next(granularity.truncate(new DateTime().getMillis()));

      ScheduledExecutors.scheduleAtFixedRate(
          exec,
          new Duration(System.currentTimeMillis(), startTime),
          resourceManagementSchedulerConfig.getTerminatePeriod().toStandardDuration(),
          new Runnable()
          {
            @Override
            public void run()
            {
              doTerminate(runner);
            }
          }
      );

      started = true;

    }
  }

  @Override
  public void stopManagement()
  {
    synchronized (lock) {
      if (!started) {
        return;
      }
      log.info("Stopping Resource Management Scheduler");
      exec.shutdown();
      started = false;
    }
  }

  @Override
  public ScalingStats getStats()
  {
    return scalingStats;
  }

  private static Predicate<ZkWorker> createLazyWorkerPredicate(
      final SimpleResourceManagementConfig config
  )
  {
    final Predicate<ZkWorker> isValidWorker = createValidWorkerPredicate(config);

    return new Predicate<ZkWorker>()
    {
      @Override
      public boolean apply(ZkWorker worker)
      {
        final boolean itHasBeenAWhile = System.currentTimeMillis() - worker.getLastCompletedTaskTime().getMillis()
                                        >= config.getWorkerIdleTimeout().toStandardDuration().getMillis();
        return itHasBeenAWhile || !isValidWorker.apply(worker);
      }
    };
  }

  private static Predicate<ZkWorker> createValidWorkerPredicate(
      final SimpleResourceManagementConfig config
  )
  {
    return new Predicate<ZkWorker>()
    {
      @Override
      public boolean apply(ZkWorker zkWorker)
      {
        final String minVersion = config.getWorkerVersion();
        if (minVersion == null) {
          throw new ISE("No minVersion found! It should be set in your runtime properties or configuration database.");
        }
        return zkWorker.isValidVersion(minVersion);
      }
    };
  }

  private void updateTargetWorkerCount(
      final WorkerBehaviorConfig workerConfig,
      final Collection<? extends TaskRunnerWorkItem> pendingTasks,
      final Collection<ZkWorker> zkWorkers
  )
  {
    synchronized (lock) {
      final Collection<ZkWorker> validWorkers = Collections2.filter(
          zkWorkers,
          createValidWorkerPredicate(config)
      );
      final Predicate<ZkWorker> isLazyWorker = createLazyWorkerPredicate(config);
      final int minWorkerCount = workerConfig.getAutoScaler().getMinNumWorkers();
      final int maxWorkerCount = workerConfig.getAutoScaler().getMaxNumWorkers();

      if (minWorkerCount > maxWorkerCount) {
        log.error("Huh? minWorkerCount[%d] > maxWorkerCount[%d]. I give up!", minWorkerCount, maxWorkerCount);
        return;
      }

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
    }
  }

  private boolean hasTaskPendingBeyondThreshold(Collection<? extends TaskRunnerWorkItem> pendingTasks)
  {
    synchronized (lock) {
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
  }

  public Collection<ZkWorker> getWorkers(RemoteTaskRunner runner)
  {
    return runner.getWorkers();
  }
}
