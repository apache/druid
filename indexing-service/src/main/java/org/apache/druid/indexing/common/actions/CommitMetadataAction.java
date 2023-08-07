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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.SegmentPublishResult;

import javax.annotation.Nullable;

/**
 * A stream ingestion task didn't ingest any rows and created no segments (e.g., all records were unparseable),
 * but still needs to update metadata with the progress that the task made.
 */
public class CommitMetadataAction implements TaskAction<SegmentPublishResult>
{
  @Nullable
  private final DataSourceMetadata startMetadata;
  @Nullable
  private final DataSourceMetadata endMetadata;
  @Nullable
  private final String dataSource;

  public static CommitMetadataAction create(
      String dataSource,
      DataSourceMetadata startMetadata,
      DataSourceMetadata endMetadata
  )
  {
    return new CommitMetadataAction(startMetadata, endMetadata, dataSource);
  }

  @JsonCreator
  private CommitMetadataAction(
      @JsonProperty("startMetadata") @Nullable DataSourceMetadata startMetadata,
      @JsonProperty("endMetadata") @Nullable DataSourceMetadata endMetadata,
      @JsonProperty("dataSource") @Nullable String dataSource
  )
  {
    this.startMetadata = startMetadata;
    this.endMetadata = endMetadata;
    this.dataSource = dataSource;
  }

  @JsonProperty
  @Nullable
  public DataSourceMetadata getStartMetadata()
  {
    return startMetadata;
  }

  @JsonProperty
  @Nullable
  public DataSourceMetadata getEndMetadata()
  {
    return endMetadata;
  }

  @JsonProperty
  @Nullable
  public String getDataSource()
  {
    return dataSource;
  }

  @Override
  public TypeReference<SegmentPublishResult> getReturnTypeReference()
  {
    return new TypeReference<SegmentPublishResult>()
    {
    };
  }

  /**
   * Performs some sanity checks and publishes the given segments.
   */
  @Override
  public SegmentPublishResult perform(Task task, TaskActionToolbox toolbox)
  {
    try {
      return toolbox.getIndexerMetadataStorageCoordinator().commitMetadataOnly(
          dataSource,
          startMetadata,
          endMetadata
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean isAudited()
  {
    return true;
  }

  @Override
  public String toString()
  {
    return "CommitMetadataAction{" +
           "startMetadata=" + startMetadata +
           ", endMetadata=" + endMetadata +
           ", dataSource='" + dataSource +
           '}';
  }
}
