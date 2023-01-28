package org.apache.druid.indexing.common;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class TaskMetadata
{
  private final String taskId;
  private final String groupId;
  private final String dataSource;

  public TaskMetadata(String taskId, String groupId, String dataSource)
  {
    this.taskId = taskId;
    this.groupId = groupId;
    this.dataSource = dataSource;
  }

  public String getTaskId()
  {
    return taskId;
  }

  public String getGroupId()
  {
    return groupId;
  }

  public String getDataSource()
  {
    return dataSource;
  }

  public Map<String, Object> toMap() {
    return ImmutableMap.of("taskId",taskId, "groupId", groupId, "dataSource",dataSource);
  }
}
