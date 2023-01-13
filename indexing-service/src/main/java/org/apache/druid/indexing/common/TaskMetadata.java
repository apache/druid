package org.apache.druid.indexing.common;

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Objects;

public class TaskMetadata
{
  private final String taskId;
  private final String groupId;
  private final String datasourceName;
  private final String supervisorId;
  private final Map<String, Object> taskMetadataMap;

  public TaskMetadata(String taskId, String groupId, String datasourceName, String supervisorId)
  {
    this.taskId = taskId;
    this.groupId = groupId;
    this.datasourceName = datasourceName;
    this.supervisorId = supervisorId;
    this.taskMetadataMap = ImmutableMap.of(
        "taskId", taskId,
        "groupId", groupId,
        "datasourceName", datasourceName,
        "supervisorId", supervisorId);
  }

  public String getTaskId()
  {
    return taskId;
  }

  public String getGroupId()
  {
    return groupId;
  }

  public String getDatasourceName()
  {
    return datasourceName;
  }

  public String getSupervisorId()
  {
    return supervisorId;
  }

  public Map<String, Object> toMap() {
    return taskMetadataMap;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TaskMetadata that = (TaskMetadata) o;
    return Objects.equals(taskId, that.taskId)
           && Objects.equals(groupId, that.groupId)
           && Objects.equals(datasourceName, that.datasourceName)
           && Objects.equals(supervisorId, that.supervisorId);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(taskId, groupId, datasourceName, supervisorId);
  }
}
