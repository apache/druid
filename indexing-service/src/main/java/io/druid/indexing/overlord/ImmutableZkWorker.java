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

package io.druid.indexing.overlord;

import com.google.common.collect.ImmutableSet;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.worker.Worker;

import java.util.Set;

/**
 * A snapshot of a {@link io.druid.indexing.overlord.ZkWorker}
 */
public class ImmutableZkWorker
{
  private final Worker worker;
  private final int currCapacityUsed;
  private final ImmutableSet<String> availabilityGroups;

  public ImmutableZkWorker(Worker worker, int currCapacityUsed, Set<String> availabilityGroups)
  {
    this.worker = worker;
    this.currCapacityUsed = currCapacityUsed;
    this.availabilityGroups = ImmutableSet.copyOf(availabilityGroups);
  }

  public Worker getWorker()
  {
    return worker;
  }

  public int getCurrCapacityUsed()
  {
    return currCapacityUsed;
  }

  public Set<String> getAvailabilityGroups()
  {
    return availabilityGroups;
  }

  public boolean isValidVersion(String minVersion)
  {
    return worker.getVersion().compareTo(minVersion) >= 0;
  }

  public boolean canRunTask(Task task)
  {
    return (worker.getCapacity() - getCurrCapacityUsed() >= task.getTaskResource().getRequiredCapacity()
            && !getAvailabilityGroups().contains(task.getTaskResource().getAvailabilityGroup()));
  }
}
