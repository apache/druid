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

package org.apache.druid.indexing.SeekableStream.supervisor;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.IAE;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

abstract public class SeekableStreamSupervisorReportPayload<T1, T2>
{
  private final String dataSource;
  private final String id;
  private final int partitions;
  private final int replicas;
  private final long durationSeconds;
  private final List<SeekableStreamTaskReportData> activeTasks;
  private final List<SeekableStreamTaskReportData> publishingTasks;
  private final Map<T1, T2> latestOffsets;
  private final Map<T1, T2> minimumLag;
  private final Long aggregateLag;
  private final DateTime offsetsLastUpdated;

  public SeekableStreamSupervisorReportPayload(
      String dataSource,
      String id,
      int partitions,
      int replicas,
      long durationSeconds,
      @Nullable Map<T1, T2> latestOffsets,
      @Nullable Map<T1, T2> minimumLag,
      @Nullable Long aggregateLag,
      @Nullable DateTime offsetsLastUpdated
  )
  {
    this.dataSource = dataSource;
    this.id = id;
    this.partitions = partitions;
    this.replicas = replicas;
    this.durationSeconds = durationSeconds;
    this.activeTasks = new ArrayList<>();
    this.publishingTasks = new ArrayList<>();
    this.latestOffsets = latestOffsets;
    this.minimumLag = minimumLag;
    this.aggregateLag = aggregateLag;
    this.offsetsLastUpdated = offsetsLastUpdated;
  }

  public void addTask(SeekableStreamTaskReportData data)
  {
    if (data.getType().equals(SeekableStreamTaskReportData.TaskType.ACTIVE)) {
      activeTasks.add(data);
    } else if (data.getType().equals(SeekableStreamTaskReportData.TaskType.PUBLISHING)) {
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

  protected String getId()
  {
    return id;
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
  public long getDurationSeconds()
  {
    return durationSeconds;
  }

  @JsonProperty
  public List<? extends SeekableStreamTaskReportData> getActiveTasks()
  {
    return activeTasks;
  }

  @JsonProperty
  public List<? extends SeekableStreamTaskReportData> getPublishingTasks()
  {
    return publishingTasks;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Map<T1, T2> getLatestOffsets()
  {
    return latestOffsets;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Map<T1, T2> getMinimumLag()
  {
    return minimumLag;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Long getAggregateLag()
  {
    return aggregateLag;
  }

  @JsonProperty
  public DateTime getOffsetsLastUpdated()
  {
    return offsetsLastUpdated;
  }

  @Override
  abstract public String toString();
}
