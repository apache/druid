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
