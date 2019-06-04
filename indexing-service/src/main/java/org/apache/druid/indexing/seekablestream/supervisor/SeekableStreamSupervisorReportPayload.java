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

package org.apache.druid.indexing.seekablestream.supervisor;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManager;
import org.apache.druid.java.util.common.IAE;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class SeekableStreamSupervisorReportPayload<PartitionIdType, SequenceOffsetType>
{
  private final String dataSource;
  private final String stream;
  private final int partitions;
  private final int replicas;
  private final long durationSeconds;
  private final List<TaskReportData> activeTasks;
  private final List<TaskReportData> publishingTasks;
  private final Map<PartitionIdType, SequenceOffsetType> latestOffsets;
  private final Map<PartitionIdType, SequenceOffsetType> minimumLag;
  private final Long aggregateLag;
  private final DateTime offsetsLastUpdated;
  private final boolean suspended;
  private final boolean healthy;
  private final SupervisorStateManager.State state;
  private final SupervisorStateManager.State detailedState;
  private final List<SupervisorStateManager.ExceptionEvent> recentErrors;

  public SeekableStreamSupervisorReportPayload(
      String dataSource,
      String stream,
      int partitions,
      int replicas,
      long durationSeconds,
      @Nullable Map<PartitionIdType, SequenceOffsetType> latestOffsets,
      @Nullable Map<PartitionIdType, SequenceOffsetType> minimumLag,
      @Nullable Long aggregateLag,
      @Nullable DateTime offsetsLastUpdated,
      boolean suspended,
      boolean healthy,
      SupervisorStateManager.State state,
      SupervisorStateManager.State detailedState,
      List<SupervisorStateManager.ExceptionEvent> recentErrors
  )
  {
    this.dataSource = dataSource;
    this.stream = stream;
    this.partitions = partitions;
    this.replicas = replicas;
    this.durationSeconds = durationSeconds;
    this.activeTasks = new ArrayList<>();
    this.publishingTasks = new ArrayList<>();
    this.latestOffsets = latestOffsets;
    this.minimumLag = minimumLag;
    this.aggregateLag = aggregateLag;
    this.offsetsLastUpdated = offsetsLastUpdated;
    this.suspended = suspended;
    this.healthy = healthy;
    this.state = state;
    this.detailedState = detailedState;
    this.recentErrors = recentErrors;
  }

  public void addTask(TaskReportData data)
  {
    if (data.getType().equals(TaskReportData.TaskType.ACTIVE)) {
      activeTasks.add(data);
    } else if (data.getType().equals(TaskReportData.TaskType.PUBLISHING)) {
      publishingTasks.add(data);
    } else {
      throw new IAE("Unknown task type [%s]", data.getType().name());
    }
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public String getStream()
  {
    return stream;
  }

  @JsonProperty
  public int getPartitions()
  {
    return partitions;
  }

  @JsonProperty
  public int getReplicas()
  {
    return replicas;
  }

  @JsonProperty
  public boolean isSuspended()
  {
    return suspended;
  }

  @JsonProperty
  public boolean isHealthy()
  {
    return healthy;
  }

  @JsonProperty
  public long getDurationSeconds()
  {
    return durationSeconds;
  }

  @JsonProperty
  public List<? extends TaskReportData> getActiveTasks()
  {
    return activeTasks;
  }

  @JsonProperty
  public List<? extends TaskReportData> getPublishingTasks()
  {
    return publishingTasks;
  }

  @JsonProperty
  public Map<PartitionIdType, SequenceOffsetType> getLatestOffsets()
  {
    return latestOffsets;
  }

  @JsonProperty
  public Map<PartitionIdType, SequenceOffsetType> getMinimumLag()
  {
    return minimumLag;
  }

  @JsonProperty
  public Long getAggregateLag()
  {
    return aggregateLag;
  }

  @JsonProperty
  public DateTime getOffsetsLastUpdated()
  {
    return offsetsLastUpdated;
  }

  @JsonProperty
  public SupervisorStateManager.State getState()
  {
    return state;
  }

  @JsonProperty
  public SupervisorStateManager.State getDetailedState()
  {
    return detailedState;
  }

  @JsonProperty
  public List<SupervisorStateManager.ExceptionEvent> getRecentErrors()
  {
    return recentErrors;
  }
}
