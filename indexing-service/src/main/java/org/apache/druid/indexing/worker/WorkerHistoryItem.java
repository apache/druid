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

package org.apache.druid.indexing.worker;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "taskUpdate", value = WorkerHistoryItem.TaskUpdate.class),
    @JsonSubTypes.Type(name = "taskRemoval", value = WorkerHistoryItem.TaskRemoval.class),
    @JsonSubTypes.Type(name = "metadata", value = WorkerHistoryItem.Metadata.class)
})
public interface WorkerHistoryItem
{
  class TaskUpdate implements WorkerHistoryItem
  {
    private final TaskAnnouncement taskAnnouncement;

    @JsonCreator
    public TaskUpdate(
        @JsonProperty("taskAnnouncement") TaskAnnouncement taskAnnouncement
    )
    {
      this.taskAnnouncement = taskAnnouncement;
    }

    @JsonProperty
    public TaskAnnouncement getTaskAnnouncement()
    {
      return taskAnnouncement;
    }

    @Override
    public String toString()
    {
      return "TaskUpdate{" +
             "taskAnnouncement=" + taskAnnouncement +
             '}';
    }
  }

  class TaskRemoval implements WorkerHistoryItem
  {
    private final String taskId;

    @JsonCreator
    public TaskRemoval(
        @JsonProperty("taskId") String taskId
    )
    {
      this.taskId = taskId;
    }

    @JsonProperty
    public String getTaskId()
    {
      return taskId;
    }

    @Override
    public String toString()
    {
      return "TaskRemoval{" +
             "taskId='" + taskId + '\'' +
             '}';
    }
  }

  class Metadata implements WorkerHistoryItem
  {
    private final boolean disabled;

    @JsonCreator
    public Metadata(@JsonProperty("disabled") boolean disabled)
    {
      this.disabled = disabled;
    }

    @JsonProperty
    public boolean isDisabled()
    {
      return disabled;
    }

    @Override
    public String toString()
    {
      return "Metadata{" +
             "disabled=" + disabled +
             '}';
    }
  }
}
