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
import io.druid.java.util.common.IAE;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;

public class KafkaSupervisorReport extends SupervisorReport
{
  public static class KafkaSupervisorReportPayload
  {
    private final String dataSource;
    private final String topic;
    private final Integer partitions;
    private final Integer replicas;
    private final Long durationSeconds;
    private final List<TaskReportData> activeTasks;
    private final List<TaskReportData> publishingTasks;

    private Map<Integer, Long> latestOffsets;
    private Map<Integer, Long> minimumLag;
    private Long aggregateLag;

    public KafkaSupervisorReportPayload(
        String dataSource,
        String topic,
        Integer partitions,
        Integer replicas,
        Long durationSeconds,
        Map<Integer, Long> latestOffsets,
        Map<Integer, Long> minimumLag,
        Long aggregateLag
    )
    {
      this.dataSource = dataSource;
      this.topic = topic;
      this.partitions = partitions;
      this.replicas = replicas;
      this.durationSeconds = durationSeconds;
      this.activeTasks = Lists.newArrayList();
      this.publishingTasks = Lists.newArrayList();

      this.latestOffsets = latestOffsets;
      this.minimumLag = minimumLag;
      this.aggregateLag = aggregateLag;
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

    @JsonProperty
    public Map<Integer, Long> getLatestOffsets()
    {
      return latestOffsets;
    }

    @JsonProperty
    public Map<Integer, Long> getMinimumLag()
    {
      return minimumLag;
    }

    @JsonProperty
    public Long getAggregateLag()
    {
      return aggregateLag;
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
             (latestOffsets != null ? ", latestOffsets=" + latestOffsets : "") +
             (minimumLag != null ? ", minimumLag=" + minimumLag : "") +
             (aggregateLag != null ? ", aggregateLag=" + aggregateLag : "") +
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
    this.payload = new KafkaSupervisorReportPayload(
        dataSource,
        topic,
        partitions,
        replicas,
        durationSeconds,
        null,
        null,
        null
    );
  }

  @Override
  public Object getPayload()
  {
    return payload;
  }

  public void addTask(TaskReportData data)
  {
    if (data.getType().equals(TaskReportData.TaskType.ACTIVE)) {
      payload.activeTasks.add(data);
    } else if (data.getType().equals(TaskReportData.TaskType.PUBLISHING)) {
      payload.publishingTasks.add(data);
    } else {
      throw new IAE("Unknown task type [%s]", data.getType().name());
    }
  }

  public void setLatestOffsets(Map<Integer, Long> latestOffsets)
  {
    payload.latestOffsets = latestOffsets;
  }

  public void setMinimumLag(Map<Integer, Long> minimumLag)
  {
    payload.minimumLag = minimumLag;
  }

  public void setAggregateLag(Long aggregateLag)
  {
    payload.aggregateLag = aggregateLag;
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
