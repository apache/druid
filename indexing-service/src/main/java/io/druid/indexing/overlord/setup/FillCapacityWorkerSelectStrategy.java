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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.ImmutableZkWorker;
import io.druid.indexing.overlord.config.RemoteTaskRunnerConfig;

import java.util.Comparator;
import java.util.TreeSet;

/**
 */
public class FillCapacityWorkerSelectStrategy implements WorkerSelectStrategy
{
  @Override
  public Optional<ImmutableZkWorker> findWorkerForTask(
      final RemoteTaskRunnerConfig config,
      final ImmutableMap<String, ImmutableZkWorker> zkWorkers,
      final Task task
  )
  {
    TreeSet<ImmutableZkWorker> sortedWorkers = Sets.newTreeSet(
        new Comparator<ImmutableZkWorker>()
        {
          @Override
          public int compare(
              ImmutableZkWorker zkWorker, ImmutableZkWorker zkWorker2
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

    for (ImmutableZkWorker zkWorker : sortedWorkers) {
      if (zkWorker.canRunTask(task) && zkWorker.isValidVersion(minWorkerVer)) {
        return Optional.of(zkWorker);
      }
    }

    return Optional.absent();
  }
}
