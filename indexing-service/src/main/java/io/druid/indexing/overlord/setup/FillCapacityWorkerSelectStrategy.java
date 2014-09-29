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

package io.druid.indexing.overlord.setup;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.ZkWorker;
import io.druid.indexing.overlord.config.RemoteTaskRunnerConfig;

import java.util.Comparator;
import java.util.Map;
import java.util.TreeSet;

/**
 */
public class FillCapacityWorkerSelectStrategy implements WorkerSelectStrategy
{
  private static final EmittingLogger log = new EmittingLogger(FillCapacityWorkerSelectStrategy.class);

  private final RemoteTaskRunnerConfig config;

  @Inject
  public FillCapacityWorkerSelectStrategy(RemoteTaskRunnerConfig config)
  {
    this.config = config;
  }

  public Optional<ZkWorker> findWorkerForTask(final Map<String, ZkWorker> zkWorkers, final Task task)
  {
    TreeSet<ZkWorker> sortedWorkers = Sets.newTreeSet(
        new Comparator<ZkWorker>()
        {
          @Override
          public int compare(
              ZkWorker zkWorker, ZkWorker zkWorker2
          )
          {
            int retVal = Ints.compare(zkWorker2.getCurrCapacityUsed(), zkWorker.getCurrCapacityUsed());
            if (retVal == 0) {
              retVal = zkWorker.getWorker().getHost().compareTo(zkWorker2.getWorker().getHost());
            }

            return retVal;
          }
        }
    );
    sortedWorkers.addAll(zkWorkers.values());
    final String minWorkerVer = config.getMinWorkerVersion();

    for (ZkWorker zkWorker : sortedWorkers) {
      if (zkWorker.canRunTask(task) && zkWorker.isValidVersion(minWorkerVer)) {
        return Optional.of(zkWorker);
      }
    }
    log.debug("Worker nodes %s do not have capacity to run any more tasks!", zkWorkers.values());

    return Optional.absent();
  }
}
