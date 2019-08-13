/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.client.indexing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Set;

/**
 * Should be synchronized with org.apache.druid.indexing.overlord.ImmutableWorkerInfo
 */
public class IndexingWorkerInfo
{
  private final IndexingWorker worker;
  private final int currCapacityUsed;
  private final Set<String> availabilityGroups;
  private final Collection<String> runningTasks;
  private final DateTime lastCompletedTaskTime;
  private final DateTime blacklistedUntil;

  @JsonCreator
  public IndexingWorkerInfo(
      @JsonProperty("worker") IndexingWorker worker,
      @JsonProperty("currCapacityUsed") int currCapacityUsed,
      @JsonProperty("availabilityGroups") Set<String> availabilityGroups,
      @JsonProperty("runningTasks") Collection<String> runningTasks,
      @JsonProperty("lastCompletedTaskTime") DateTime lastCompletedTaskTime,
      @JsonProperty("blacklistedUntil") @Nullable DateTime blacklistedUntil
  )
  {
    this.worker = worker;
    this.currCapacityUsed = currCapacityUsed;
    this.availabilityGroups = availabilityGroups;
    this.runningTasks = runningTasks;
    this.lastCompletedTaskTime = lastCompletedTaskTime;
    this.blacklistedUntil = blacklistedUntil;
  }

  @JsonProperty("worker")
  public IndexingWorker getWorker()
  {
    return worker;
  }

  @JsonProperty("currCapacityUsed")
  public int getCurrCapacityUsed()
  {
    return currCapacityUsed;
  }

  @JsonProperty("availabilityGroups")
  public Set<String> getAvailabilityGroups()
  {
    return availabilityGroups;
  }

  public int getAvailableCapacity()
  {
    return getWorker().getCapacity() - getCurrCapacityUsed();
  }

  @JsonProperty("runningTasks")
  public Collection<String> getRunningTasks()
  {
    return runningTasks;
  }

  @JsonProperty("lastCompletedTaskTime")
  public DateTime getLastCompletedTaskTime()
  {
    return lastCompletedTaskTime;
  }

  @JsonProperty
  public DateTime getBlacklistedUntil()
  {
    return blacklistedUntil;
  }
}
