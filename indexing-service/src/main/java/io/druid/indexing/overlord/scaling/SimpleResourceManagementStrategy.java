/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.indexing.overlord.scaling;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.Collections2;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.emitter.EmittingLogger;
import io.druid.indexing.overlord.RemoteTaskRunnerWorkItem;
import io.druid.indexing.overlord.TaskRunnerWorkItem;
import io.druid.indexing.overlord.ZkWorker;
import io.druid.indexing.overlord.setup.WorkerSetupData;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 */
public class SimpleResourceManagementStrategy implements ResourceManagementStrategy
{
  private static final EmittingLogger log = new EmittingLogger(SimpleResourceManagementStrategy.class);

  private final AutoScalingStrategy autoScalingStrategy;
  private final SimpleResourceManagementConfig config;
  private final Supplier<WorkerSetupData> workerSetupDataRef;
  private final ScalingStats scalingStats;

  private final Object lock = new Object();
  private final Set<String> currentlyProvisioning = Sets.newHashSet();
  private final Set<String> currentlyTerminating = Sets.newHashSet();

  private int targetWorkerCount = -1;
  private DateTime lastProvisionTime = new DateTime();
  private DateTime lastTerminateTime = new DateTime();

  @Inject
  public SimpleResourceManagementStrategy(
      AutoScalingStrategy autoScalingStrategy,
      SimpleResourceManagementConfig config,
      Supplier<WorkerSetupData> workerSetupDataRef
  )
  {
    this.autoScalingStrategy = autoScalingStrategy;
    this.config = config;
    this.workerSetupDataRef = workerSetupDataRef;
    this.scalingStats = new ScalingStats(config.getNumEventsToTrack());
  }

  @Override
  public boolean doProvision(Collection<RemoteTaskRunnerWorkItem> pendingTasks, Collection<ZkWorker> zkWorkers)
  {
    synchronized (lock) {
      boolean didProvision = false;
      final WorkerSetupData workerSetupData = workerSetupDataRef.get();
      if (workerSetupData == null) {
        log.warn("No workerSetupData available, cannot provision new workers.");
        return false;
      }
      final Predicate<ZkWorker> isValidWorker = createValidWorkerPredicate(config, workerSetupData);
      final int currValidWorkers = Collections2.filter(zkWorkers, isValidWorker).size();

      final List<String> workerNodeIds = autoScalingStrategy.ipToIdLookup(
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

      updateTargetWorkerCount(workerSetupData, pendingTasks, zkWorkers);

      int want = targetWorkerCount - (currValidWorkers + currentlyProvisioning.size());
      while (want > 0) {
        final AutoScalingData provisioned = autoScalingStrategy.provision();
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

          List<String> nodeIps = autoScalingStrategy.idToIpLookup(Lists.newArrayList(currentlyProvisioning));
          autoScalingStrategy.terminate(nodeIps);
          currentlyProvisioning.clear();
        }
      }

      return didProvision;
    }
  }

  @Override
  public boolean doTerminate(Collection<RemoteTaskRunnerWorkItem> pendingTasks, Collection<ZkWorker> zkWorkers)
  {
    synchronized (lock) {
      final WorkerSetupData workerSetupData = workerSetupDataRef.get();
      if (workerSetupData == null) {
        log.warn("No workerSetupData available, cannot terminate workers.");
        return false;
      }

      boolean didTerminate = false;
      final Set<String> workerNodeIds = Sets.newHashSet(
          autoScalingStrategy.ipToIdLookup(
              Lists.newArrayList(
                  Iterables.transform(
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

      updateTargetWorkerCount(workerSetupData, pendingTasks, zkWorkers);

      final Predicate<ZkWorker> isLazyWorker = createLazyWorkerPredicate(config, workerSetupData);
      if (currentlyTerminating.isEmpty()) {
        final int excessWorkers = (zkWorkers.size() + currentlyProvisioning.size()) - targetWorkerCount;
        if (excessWorkers > 0) {
          final List<String> laziestWorkerIps =
              FluentIterable.from(zkWorkers)
                            .filter(isLazyWorker)
                            .limit(excessWorkers)
                            .transform(
                                new Function<ZkWorker, String>()
                                {
                                  @Override
                                  public String apply(ZkWorker zkWorker)
                                  {
                                    return zkWorker.getWorker().getIp();
                                  }
                                }
                            )
                            .toList();

          if (laziestWorkerIps.isEmpty()) {
            log.info("Wanted to terminate %,d workers, but couldn't find any lazy ones!", excessWorkers);
          } else {
            log.info(
                "Terminating %,d workers (wanted %,d): %s",
                laziestWorkerIps.size(),
                excessWorkers,
                Joiner.on(", ").join(laziestWorkerIps)
            );

            final AutoScalingData terminated = autoScalingStrategy.terminate(laziestWorkerIps);
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
  public ScalingStats getStats()
  {
    return scalingStats;
  }

  private static Predicate<ZkWorker> createLazyWorkerPredicate(
      final SimpleResourceManagementConfig config,
      final WorkerSetupData workerSetupData
  )
  {
    final Predicate<ZkWorker> isValidWorker = createValidWorkerPredicate(config, workerSetupData);

    return new Predicate<ZkWorker>()
    {
      @Override
      public boolean apply(ZkWorker worker)
      {
        final boolean itHasBeenAWhile = System.currentTimeMillis() - worker.getLastCompletedTaskTime().getMillis()
                                        >= config.getWorkerIdleTimeout().toStandardDuration().getMillis();
        return worker.getRunningTasks().isEmpty() && (itHasBeenAWhile || !isValidWorker.apply(worker));
      }
    };
  }

  private static Predicate<ZkWorker> createValidWorkerPredicate(
      final SimpleResourceManagementConfig config,
      final WorkerSetupData workerSetupData
  )
  {
    return new Predicate<ZkWorker>()
    {
      @Override
      public boolean apply(ZkWorker zkWorker)
      {
        final String minVersion = workerSetupData.getMinVersion() != null
                                  ? workerSetupData.getMinVersion()
                                  : config.getWorkerVersion();
        if (minVersion == null) {
          throw new ISE("No minVersion found! It should be set in your runtime properties or configuration database.");
        }
        return zkWorker.isValidVersion(minVersion);
      }
    };
  }

  private void updateTargetWorkerCount(
      final WorkerSetupData workerSetupData,
      final Collection<RemoteTaskRunnerWorkItem> pendingTasks,
      final Collection<ZkWorker> zkWorkers
  )
  {
    synchronized (lock) {
      final Collection<ZkWorker> validWorkers = Collections2.filter(
          zkWorkers,
          createValidWorkerPredicate(config, workerSetupData)
      );
      final Predicate<ZkWorker> isLazyWorker = createLazyWorkerPredicate(config, workerSetupData);

      if (targetWorkerCount < 0) {
        // Initialize to size of current worker pool, subject to pool size limits
        targetWorkerCount = Math.max(
            Math.min(
                zkWorkers.size(),
                workerSetupData.getMaxNumWorkers()
            ),
            workerSetupData.getMinNumWorkers()
        );
        log.info(
            "Starting with a target of %,d workers (current = %,d, min = %,d, max = %,d).",
            targetWorkerCount,
            validWorkers.size(),
            workerSetupData.getMinNumWorkers(),
            workerSetupData.getMaxNumWorkers()
        );
      }

      final boolean atSteadyState = currentlyProvisioning.isEmpty()
                                    && currentlyTerminating.isEmpty()
                                    && validWorkers.size() == targetWorkerCount;
      final boolean shouldScaleUp = atSteadyState
                                    && hasTaskPendingBeyondThreshold(pendingTasks)
                                    && targetWorkerCount < workerSetupData.getMaxNumWorkers();
      final boolean shouldScaleDown = atSteadyState
                                      && Iterables.any(validWorkers, isLazyWorker)
                                      && targetWorkerCount > workerSetupData.getMinNumWorkers();
      if (shouldScaleUp) {
        targetWorkerCount++;
        log.info(
            "I think we should scale up to %,d workers (current = %,d, min = %,d, max = %,d).",
            targetWorkerCount,
            validWorkers.size(),
            workerSetupData.getMinNumWorkers(),
            workerSetupData.getMaxNumWorkers()
        );
      } else if (shouldScaleDown) {
        targetWorkerCount--;
        log.info(
            "I think we should scale down to %,d workers (current = %,d, min = %,d, max = %,d).",
            targetWorkerCount,
            validWorkers.size(),
            workerSetupData.getMinNumWorkers(),
            workerSetupData.getMaxNumWorkers()
        );
      } else {
        log.info(
            "Our target is %,d workers, and I'm okay with that (current = %,d, min = %,d, max = %,d).",
            targetWorkerCount,
            validWorkers.size(),
            workerSetupData.getMinNumWorkers(),
            workerSetupData.getMaxNumWorkers()
        );
      }
    }
  }

  private boolean hasTaskPendingBeyondThreshold(Collection<RemoteTaskRunnerWorkItem> pendingTasks)
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
}
