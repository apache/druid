/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.indexing.coordinator.scaling;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.druid.indexing.coordinator.RemoteTaskRunnerWorkItem;
import com.metamx.druid.indexing.coordinator.TaskRunnerWorkItem;
import com.metamx.druid.indexing.coordinator.ZkWorker;
import com.metamx.druid.indexing.coordinator.setup.WorkerSetupData;
import com.metamx.emitter.EmittingLogger;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class SimpleResourceManagementStrategy implements ResourceManagementStrategy
{
  private static final EmittingLogger log = new EmittingLogger(SimpleResourceManagementStrategy.class);

  private final AutoScalingStrategy autoScalingStrategy;
  private final SimpleResourceManagmentConfig config;
  private final AtomicReference<WorkerSetupData> workerSetupdDataRef;
  private final ScalingStats scalingStats;

  private final ConcurrentSkipListSet<String> currentlyProvisioning = new ConcurrentSkipListSet<String>();
  private final ConcurrentSkipListSet<String> currentlyTerminating = new ConcurrentSkipListSet<String>();

  private volatile DateTime lastProvisionTime = new DateTime();
  private volatile DateTime lastTerminateTime = new DateTime();

  public SimpleResourceManagementStrategy(
      AutoScalingStrategy autoScalingStrategy,
      SimpleResourceManagmentConfig config,
      AtomicReference<WorkerSetupData> workerSetupdDataRef
  )
  {
    this.autoScalingStrategy = autoScalingStrategy;
    this.config = config;
    this.workerSetupdDataRef = workerSetupdDataRef;
    this.scalingStats = new ScalingStats(config.getNumEventsToTrack());
  }

  @Override
  public boolean doProvision(Collection<RemoteTaskRunnerWorkItem> pendingTasks, Collection<ZkWorker> zkWorkers)
  {
    if (zkWorkers.size() >= workerSetupdDataRef.get().getMaxNumWorkers()) {
      log.info(
          "Cannot scale anymore. Num workers = %d, Max num workers = %d",
          zkWorkers.size(),
          workerSetupdDataRef.get().getMaxNumWorkers()
      );
      return false;
    }

    List<String> workerNodeIds = autoScalingStrategy.ipToIdLookup(
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
    );

    currentlyProvisioning.removeAll(workerNodeIds);
    boolean nothingProvisioning = currentlyProvisioning.isEmpty();

    if (nothingProvisioning) {
      if (hasTaskPendingBeyondThreshold(pendingTasks)) {
        AutoScalingData provisioned = autoScalingStrategy.provision();

        if (provisioned != null) {
          currentlyProvisioning.addAll(provisioned.getNodeIds());
          lastProvisionTime = new DateTime();
          scalingStats.addProvisionEvent(provisioned);

          return true;
        }
      }
    } else {
      Duration durSinceLastProvision = new Duration(lastProvisionTime, new DateTime());

      log.info(
          "%s still provisioning. Wait for all provisioned nodes to complete before requesting new worker. Current wait time: %s",
          currentlyProvisioning,
          durSinceLastProvision
      );

      if (durSinceLastProvision.isLongerThan(config.getMaxScalingDuration())) {
        log.makeAlert("Worker node provisioning taking too long!")
           .addData("millisSinceLastProvision", durSinceLastProvision.getMillis())
           .addData("provisioningCount", currentlyProvisioning.size())
           .emit();

        List<String> nodeIps = autoScalingStrategy.idToIpLookup(Lists.newArrayList(currentlyProvisioning));
        autoScalingStrategy.terminate(nodeIps);
        currentlyProvisioning.clear();
      }
    }

    return false;
  }

  @Override
  public boolean doTerminate(Collection<RemoteTaskRunnerWorkItem> pendingTasks, Collection<ZkWorker> zkWorkers)
  {
    Set<String> workerNodeIds = Sets.newHashSet(
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

    Set<String> stillExisting = Sets.newHashSet();
    for (String s : currentlyTerminating) {
      if (workerNodeIds.contains(s)) {
        stillExisting.add(s);
      }
    }
    currentlyTerminating.clear();
    currentlyTerminating.addAll(stillExisting);
    boolean nothingTerminating = currentlyTerminating.isEmpty();

    if (nothingTerminating) {
      final int minNumWorkers = workerSetupdDataRef.get().getMinNumWorkers();
      if (zkWorkers.size() <= minNumWorkers) {
        log.info("Only [%d <= %d] nodes in the cluster, not terminating anything.", zkWorkers.size(), minNumWorkers);
        return false;
      }

      List<ZkWorker> thoseLazyWorkers = Lists.newArrayList(
          FunctionalIterable
              .create(zkWorkers)
              .filter(
                  new Predicate<ZkWorker>()
                  {
                    @Override
                    public boolean apply(ZkWorker input)
                    {
                      return input.getRunningTasks().isEmpty()
                             && System.currentTimeMillis() - input.getLastCompletedTaskTime().getMillis()
                                >= config.getMaxWorkerIdleTimeMillisBeforeDeletion();
                    }
                  }
              )
      );

      int maxPossibleNodesTerminated = zkWorkers.size() - minNumWorkers;
      int numNodesToTerminate = Math.min(maxPossibleNodesTerminated, thoseLazyWorkers.size());
      if (numNodesToTerminate <= 0) {
        log.info("Found no nodes to terminate.");
        return false;
      }

      AutoScalingData terminated = autoScalingStrategy.terminate(
          Lists.transform(
              thoseLazyWorkers.subList(0, numNodesToTerminate),
              new Function<ZkWorker, String>()
              {
                @Override
                public String apply(ZkWorker input)
                {
                  return input.getWorker().getIp();
                }
              }
          )
      );

      if (terminated != null) {
        currentlyTerminating.addAll(terminated.getNodeIds());
        lastTerminateTime = new DateTime();
        scalingStats.addTerminateEvent(terminated);

        return true;
      }
    } else {
      Duration durSinceLastTerminate = new Duration(lastTerminateTime, new DateTime());

      log.info(
          "%s still terminating. Wait for all nodes to terminate before trying again.",
          currentlyTerminating
      );

      if (durSinceLastTerminate.isLongerThan(config.getMaxScalingDuration())) {
        log.makeAlert("Worker node termination taking too long!")
           .addData("millisSinceLastTerminate", durSinceLastTerminate.getMillis())
           .addData("terminatingCount", currentlyTerminating.size())
           .emit();

        currentlyTerminating.clear();
      }
    }

    return false;
  }

  @Override
  public ScalingStats getStats()
  {
    return scalingStats;
  }

  private boolean hasTaskPendingBeyondThreshold(Collection<RemoteTaskRunnerWorkItem> pendingTasks)
  {
    long now = System.currentTimeMillis();
    for (TaskRunnerWorkItem pendingTask : pendingTasks) {
      if (new Duration(pendingTask.getQueueInsertionTime().getMillis(), now).isEqual(config.getMaxPendingTaskDuration())
          ||
          new Duration(
              pendingTask.getQueueInsertionTime().getMillis(), now
          ).isLongerThan(config.getMaxPendingTaskDuration())) {
        return true;
      }
    }
    return false;
  }
}
