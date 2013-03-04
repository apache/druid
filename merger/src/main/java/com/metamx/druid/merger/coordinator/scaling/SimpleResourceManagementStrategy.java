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

package com.metamx.druid.merger.coordinator.scaling;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.druid.merger.coordinator.TaskRunnerWorkItem;
import com.metamx.druid.merger.coordinator.ZkWorker;
import com.metamx.druid.merger.coordinator.setup.WorkerSetupManager;
import com.metamx.emitter.EmittingLogger;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 */
public class SimpleResourceManagementStrategy implements ResourceManagementStrategy
{
  private static final EmittingLogger log = new EmittingLogger(SimpleResourceManagementStrategy.class);

  private final AutoScalingStrategy autoScalingStrategy;
  private final SimpleResourceManagmentConfig config;
  private final WorkerSetupManager workerSetupManager;
  private final ScalingStats scalingStats;

  private final ConcurrentSkipListSet<String> currentlyProvisioning = new ConcurrentSkipListSet<String>();
  private final ConcurrentSkipListSet<String> currentlyTerminating = new ConcurrentSkipListSet<String>();

  private volatile DateTime lastProvisionTime = new DateTime();
  private volatile DateTime lastTerminateTime = new DateTime();

  public SimpleResourceManagementStrategy(
      AutoScalingStrategy autoScalingStrategy,
      SimpleResourceManagmentConfig config,
      WorkerSetupManager workerSetupManager
  )
  {
    this.autoScalingStrategy = autoScalingStrategy;
    this.config = config;
    this.workerSetupManager = workerSetupManager;
    this.scalingStats = new ScalingStats(config.getNumEventsToTrack());
  }

  @Override
  public boolean doProvision(Collection<TaskRunnerWorkItem> pendingTasks, Collection<ZkWorker> zkWorkers)
  {
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
      Duration durSinceLastProvision = new Duration(new DateTime(), lastProvisionTime);
      if (durSinceLastProvision.isLongerThan(config.getMaxScalingDuration())) {
        log.makeAlert("Worker node provisioning taking too long")
           .addData("millisSinceLastProvision", durSinceLastProvision.getMillis())
           .addData("provisioningCount", currentlyProvisioning.size())
           .emit();
      }

      log.info(
          "%s still provisioning. Wait for all provisioned nodes to complete before requesting new worker.",
          currentlyProvisioning
      );
    }

    return false;
  }

  @Override
  public boolean doTerminate(Collection<TaskRunnerWorkItem> pendingTasks, Collection<ZkWorker> zkWorkers)
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
      final int minNumWorkers = workerSetupManager.getWorkerSetupData().getMinNumWorkers();
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

      AutoScalingData terminated = autoScalingStrategy.terminate(
          Lists.transform(
              thoseLazyWorkers.subList(minNumWorkers, thoseLazyWorkers.size()),
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
      Duration durSinceLastTerminate = new Duration(new DateTime(), lastTerminateTime);
      if (durSinceLastTerminate.isLongerThan(config.getMaxScalingDuration())) {
        log.makeAlert("Worker node termination taking too long")
           .addData("millisSinceLastTerminate", durSinceLastTerminate.getMillis())
           .addData("terminatingCount", currentlyTerminating.size())
           .emit();
      }

      log.info(
          "%s still terminating. Wait for all nodes to terminate before trying again.",
          currentlyTerminating
      );
    }

    return false;
  }

  @Override
  public ScalingStats getStats()
  {
    return scalingStats;
  }

  private boolean hasTaskPendingBeyondThreshold(Collection<TaskRunnerWorkItem> pendingTasks)
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
