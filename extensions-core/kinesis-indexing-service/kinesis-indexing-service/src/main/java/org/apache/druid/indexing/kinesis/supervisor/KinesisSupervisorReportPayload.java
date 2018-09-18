package org.apache.druid.indexing.kinesis.supervisor;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.IAE;

import java.util.List;

public class KinesisSupervisorReportPayload
{
  private final String dataSource;
  private final String stream;
  private final Integer partitions;
  private final Integer replicas;
  private final Long durationSeconds;
  private final List<TaskReportData> activeTasks;
  private final List<TaskReportData> publishingTasks;
  private final boolean suspended;

  public KinesisSupervisorReportPayload(
      String dataSource,
      String stream,
      Integer partitions,
      Integer replicas,
      Long durationSeconds,
      boolean suspended
  )
  {
    this.dataSource = dataSource;
    this.stream = stream;
    this.partitions = partitions;
    this.replicas = replicas;
    this.durationSeconds = durationSeconds;
    this.activeTasks = Lists.newArrayList();
    this.publishingTasks = Lists.newArrayList();
    this.suspended = suspended;
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
  public boolean getSuspended()
  {
    return suspended;
  }

  @Override
  public String toString()
  {
    return "{" +
           "dataSource='" + dataSource + '\'' +
           ", stream='" + stream + '\'' +
           ", partitions=" + partitions +
           ", replicas=" + replicas +
           ", durationSeconds=" + durationSeconds +
           ", active=" + activeTasks +
           ", publishing=" + publishingTasks +
           ", suspended=" + suspended +
           '}';
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
}