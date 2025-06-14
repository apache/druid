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
import org.apache.druid.common.config.Configs;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.DataSourceMetadata;

import javax.annotation.Nullable;

public class ResetDataSourceMetadataAction implements TaskAction<Boolean>
{
  private final String supervisorId;
  private final String dataSource;
  private final DataSourceMetadata resetMetadata;

  public ResetDataSourceMetadataAction(
      @JsonProperty("supervisorId") @Nullable String supervisorId,
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("resetMetadata") DataSourceMetadata resetMetadata
  )
  {
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource cannot be null");
    this.supervisorId = Preconditions.checkNotNull(
        Configs.valueOrDefault(supervisorId, dataSource),
        "supervisorId cannot be null"
    );
    this.resetMetadata = resetMetadata;
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public String getSupervisorId()
  {
    return supervisorId;
  }

  @JsonProperty
  public DataSourceMetadata getResetMetadata()
  {
    return resetMetadata;
  }

  @Override
  public TypeReference<Boolean> getReturnTypeReference()
  {
    return new TypeReference<>() {};
  }

  @Override
  public Boolean perform(Task task, TaskActionToolbox toolbox)
  {
    return toolbox.getSupervisorManager().resetSupervisor(supervisorId, resetMetadata);
  }

  @Override
  public String toString()
  {
    return "ResetDataSourceMetadataAction{" +
           "dataSource='" + dataSource + '\'' +
           ", supervisorId='" + supervisorId + '\'' +
           ", resetMetadata=" + resetMetadata +
           '}';
  }
}
