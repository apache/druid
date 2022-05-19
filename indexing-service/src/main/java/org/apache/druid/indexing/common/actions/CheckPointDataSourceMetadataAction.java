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

package org.apache.druid.indexing.common.actions;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.seekablestream.SeekableStreamDataSourceMetadata;
import org.apache.druid.indexing.seekablestream.SeekableStreamStartSequenceNumbers;

import javax.annotation.Nullable;
import java.util.Objects;

public class CheckPointDataSourceMetadataAction implements TaskAction<Boolean>
{
  private final String supervisorId;
  private final int taskGroupId;
  private final SeekableStreamDataSourceMetadata checkpointMetadata;

  public CheckPointDataSourceMetadataAction(
      @JsonProperty("supervisorId") String supervisorId,
      @JsonProperty("taskGroupId") Integer taskGroupId,
      @JsonProperty("previousCheckPoint") @Nullable @Deprecated SeekableStreamDataSourceMetadata previousCheckPoint,
      @JsonProperty("checkpointMetadata") @Nullable SeekableStreamDataSourceMetadata checkpointMetadata
  )
  {
    this.supervisorId = Preconditions.checkNotNull(supervisorId, "supervisorId");
    this.taskGroupId = Preconditions.checkNotNull(taskGroupId, "taskGroupId");
    this.checkpointMetadata = checkpointMetadata == null ? previousCheckPoint : checkpointMetadata;

    Preconditions.checkNotNull(this.checkpointMetadata, "checkpointMetadata");
    // checkpointMetadata must be SeekableStreamStartSequenceNumbers because it's the start sequence numbers of the
    // sequence currently being checkpointed
    Preconditions.checkArgument(
        this.checkpointMetadata.getSeekableStreamSequenceNumbers() instanceof SeekableStreamStartSequenceNumbers,
        "checkpointMetadata must be SeekableStreamStartSequenceNumbers"
    );
  }

  @JsonProperty
  public String getSupervisorId()
  {
    return supervisorId;
  }

  @Nullable
  @JsonProperty
  public Integer getTaskGroupId()
  {
    return taskGroupId;
  }

  // For backwards compatibility
  @JsonProperty
  private SeekableStreamDataSourceMetadata getPreviousCheckPoint()
  {
    return checkpointMetadata;
  }

  // For backwards compatibility
  @JsonProperty
  private SeekableStreamDataSourceMetadata getCurrentCheckPoint()
  {
    return checkpointMetadata;
  }

  /**
   * This method is for backwards compatibility to add the missing property (sequenceName) in serialized JSON,
   * so rolling-updates from older versions are compatible, a dummy value is returned since the value is not
   * used in any production code as long as the json property is present
   *
   * TODO : this should be removed when we don't need rolling-update compatibility with version 0.15 or earlier anymore
   *
   * @return dummy value
   */
  @JsonProperty("sequenceName")
  private String getBaseSequenceName()
  {
    return "dummy";
  }

  @JsonProperty
  public SeekableStreamDataSourceMetadata getCheckpointMetadata()
  {
    return checkpointMetadata;
  }

  @Override
  public TypeReference<Boolean> getReturnTypeReference()
  {
    return new TypeReference<Boolean>()
    {
    };
  }

  @Override
  public Boolean perform(Task task, TaskActionToolbox toolbox)
  {
    return toolbox.getSupervisorManager().checkPointDataSourceMetadata(
        supervisorId,
        taskGroupId,
        checkpointMetadata
    );
  }

  @Override
  public boolean isAudited()
  {
    return true;
  }

  @Override
  public String toString()
  {
    return "CheckPointDataSourceMetadataAction{" +
           "supervisorId='" + supervisorId + '\'' +
           ", taskGroupId='" + taskGroupId + '\'' +
           ", checkpointMetadata=" + checkpointMetadata +
           '}';
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

    CheckPointDataSourceMetadataAction that = (CheckPointDataSourceMetadataAction) o;
    if (!supervisorId.equals(that.supervisorId)) {
      return false;
    }
    if (taskGroupId != that.taskGroupId) {
      return false;
    }

    if (!Objects.equals(checkpointMetadata, that.checkpointMetadata)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        supervisorId,
        taskGroupId,
        checkpointMetadata
    );
  }

}
