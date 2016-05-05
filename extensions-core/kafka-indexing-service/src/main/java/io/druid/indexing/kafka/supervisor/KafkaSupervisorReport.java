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

package io.druid.indexing.kafka.supervisor;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import io.druid.indexing.overlord.supervisor.SupervisorReport;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;

public class KafkaSupervisorReport extends SupervisorReport
{
  public class TaskReportData
  {
    private final String id;
    private final Map<Integer, Long> startingOffsets;
    private final Map<Integer, Long> currentOffsets;
    private final DateTime startTime;
    private final Long remainingSeconds;

    public TaskReportData(
        String id,
        Map<Integer, Long> startingOffsets,
        Map<Integer, Long> currentOffsets,
        DateTime startTime,
        Long remainingSeconds
    )
    {
      this.id = id;
      this.startingOffsets = startingOffsets;
      this.currentOffsets = currentOffsets;
      this.startTime = startTime;
      this.remainingSeconds = remainingSeconds;
    }

    @JsonProperty
    public String getId()
    {
      return id;
    }

    @JsonProperty
    public Map<Integer, Long> getStartingOffsets()
    {
      return startingOffsets;
    }

    @JsonProperty
    public Map<Integer, Long> getCurrentOffsets()
    {
      return currentOffsets;
    }

    @JsonProperty
    public DateTime getStartTime()
    {
      return startTime;
    }

    @JsonProperty
    public Long getRemainingSeconds()
    {
      return remainingSeconds;
    }

    @Override
    public String toString()
    {
      return "{" +
             "id='" + id + '\'' +
             (startingOffsets != null ? ", startingOffsets=" + startingOffsets : "") +
             (currentOffsets != null ? ", currentOffsets=" + currentOffsets : "") +
             ", startTime=" + startTime +
             ", remainingSeconds=" + remainingSeconds +
             '}';
    }
  }

  public class KafkaSupervisorReportPayload
  {
    private final String dataSource;
    private final String topic;
    private final Integer partitions;
    private final Integer replicas;
    private final Long durationSeconds;
    private final List<TaskReportData> activeTasks;
    private final List<TaskReportData> publishingTasks;

    public KafkaSupervisorReportPayload(
        String dataSource,
        String topic,
        Integer partitions,
        Integer replicas,
        Long durationSeconds
    )
    {
      this.dataSource = dataSource;
      this.topic = topic;
      this.partitions = partitions;
      this.replicas = replicas;
      this.durationSeconds = durationSeconds;
      this.activeTasks = Lists.newArrayList();
      this.publishingTasks = Lists.newArrayList();
    }

    @JsonProperty
    public String getDataSource()
    {
      return dataSource;
    }

    @JsonProperty
    public String getTopic()
    {
      return topic;
    }

    @JsonProperty
    public Integer getPartitions()
    {
      return partitions;
    }

    @JsonProperty
    public Integer getReplicas()
    {
      return replicas;
    }

    @JsonProperty
    public Long getDurationSeconds()
    {
      return durationSeconds;
    }

    @JsonProperty
    public List<TaskReportData> getActiveTasks()
    {
      return activeTasks;
    }

    @JsonProperty
    public List<TaskReportData> getPublishingTasks()
    {
      return publishingTasks;
    }

    @Override
    public String toString()
    {
      return "{" +
             "dataSource='" + dataSource + '\'' +
             ", topic='" + topic + '\'' +
             ", partitions=" + partitions +
             ", replicas=" + replicas +
             ", durationSeconds=" + durationSeconds +
             ", active=" + activeTasks +
             ", publishing=" + publishingTasks +
             '}';
    }
  }

  private final KafkaSupervisorReportPayload payload;

  public KafkaSupervisorReport(
      String dataSource,
      DateTime generationTime,
      String topic,
      Integer partitions,
      Integer replicas,
      Long durationSeconds
  )
  {
    super(dataSource, generationTime);
    this.payload = new KafkaSupervisorReportPayload(dataSource, topic, partitions, replicas, durationSeconds);
  }

  @Override
  public Object getPayload()
  {
    return payload;
  }

  public void addActiveTask(
      String id,
      Map<Integer, Long> startingOffsets,
      Map<Integer, Long> currentOffsets,
      DateTime startTime,
      Long remainingSeconds
  )
  {
    payload.activeTasks.add(new TaskReportData(id, startingOffsets, currentOffsets, startTime, remainingSeconds));
  }

  public void addPublishingTask(
      String id,
      Map<Integer, Long> startingOffsets,
      Map<Integer, Long> currentOffsets,
      DateTime startTime,
      Long remainingSeconds
  )
  {
    payload.publishingTasks.add(new TaskReportData(id, startingOffsets, currentOffsets, startTime, remainingSeconds));
  }

  @Override
  public String toString()
  {
    return "{" +
           "id='" + getId() + '\'' +
           ", generationTime=" + getGenerationTime() +
           ", payload=" + payload +
           '}';
  }
}
