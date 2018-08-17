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

package io.druid.indexing.common.actions;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.DataSourceMetadata;

import java.io.IOException;
import javax.annotation.Nullable;

public class CheckPointDataSourceMetadataAction implements TaskAction<Boolean>
{
  private final String supervisorId;
  @Nullable
  private final Integer taskGroupId;
  @Deprecated
  private final String baseSequenceName;
  private final DataSourceMetadata previousCheckPoint;
  private final DataSourceMetadata currentCheckPoint;

  public CheckPointDataSourceMetadataAction(
      @JsonProperty("supervisorId") String supervisorId,
      @JsonProperty("taskGroupId") @Nullable Integer taskGroupId, // nullable for backward compatibility,
      @JsonProperty("sequenceName") @Deprecated String baseSequenceName, // old version would use this
      @JsonProperty("previousCheckPoint") DataSourceMetadata previousCheckPoint,
      @JsonProperty("currentCheckPoint") DataSourceMetadata currentCheckPoint
  )
  {
    this.supervisorId = Preconditions.checkNotNull(supervisorId, "supervisorId");
    this.taskGroupId = taskGroupId;
    this.baseSequenceName = Preconditions.checkNotNull(baseSequenceName, "sequenceName");
    this.previousCheckPoint = Preconditions.checkNotNull(previousCheckPoint, "previousCheckPoint");
    this.currentCheckPoint = Preconditions.checkNotNull(currentCheckPoint, "currentCheckPoint");
  }

  @JsonProperty
  public String getSupervisorId()
  {
    return supervisorId;
  }

  @Deprecated
  @JsonProperty("sequenceName")
  public String getBaseSequenceName()
  {
    return baseSequenceName;
  }

  @Nullable
  @JsonProperty
  public Integer getTaskGroupId()
  {
    return taskGroupId;
  }

  @JsonProperty
  public DataSourceMetadata getPreviousCheckPoint()
  {
    return previousCheckPoint;
  }

  @JsonProperty
  public DataSourceMetadata getCurrentCheckPoint()
  {
    return currentCheckPoint;
  }

  @Override
  public TypeReference<Boolean> getReturnTypeReference()
  {
    return new TypeReference<Boolean>()
    {
    };
  }

  @Override
  public Boolean perform(
      Task task, TaskActionToolbox toolbox
  ) throws IOException
  {
    return toolbox.getSupervisorManager().checkPointDataSourceMetadata(
        supervisorId,
        taskGroupId,
        baseSequenceName,
        previousCheckPoint,
        currentCheckPoint
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
           ", baseSequenceName='" + baseSequenceName + '\'' +
           ", taskGroupId='" + taskGroupId + '\'' +
           ", previousCheckPoint=" + previousCheckPoint +
           ", currentCheckPoint=" + currentCheckPoint +
           '}';
  }
}
