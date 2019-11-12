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

import java.util.Set;

/**
 * This action is used by tasks to inform the supervisor of closed shards when the task did not ingest any data.
 * Normally the task communicates the current offsets of the partitions it read when it publishes segments. If
 * no data was ingested (no publishing), a separate message is needed so that the supervisor knows about
 * shards that have been closed.
 *
 * The closedPartitions are represented as strings in this action, although the tasks and supervisors have a generic
 * PartitionIdType. Currently only Kinesis supports shard closing, and it uses String partition IDs. As a future change
 * the tasks/supervisors could have a method added that converts from string partition IDs to their native partition
 * ID type.
 */
public class SeekableStreamPartitionsClosedNotifyAction implements TaskAction<Boolean>
{
  private final String dataSource;
  private final Set<String> closedShards;

  @JsonCreator
  public SeekableStreamPartitionsClosedNotifyAction(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("closedShards") Set<String> closedPartitions
  )
  {
    this.dataSource = dataSource;
    this.closedShards = closedPartitions;
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
      Task task,
      TaskActionToolbox toolbox
  )
  {
    return toolbox.getSupervisorManager().updateClosedShards(dataSource, closedShards);
  }

  @Override
  public boolean isAudited()
  {
    return true;
  }

  @JsonProperty
  public Set<String> getClosedShards()
  {
    return closedShards;
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }
}
