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

package org.apache.druid.indexing.overlord.supervisor;

import org.apache.druid.error.DruidException;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.LagStats;

import java.util.List;

/**
 * An interface for managing supervisors that handle stream-based ingestion tasks.
 * <p>
 * This interface extends {@link Supervisor} and adds additional functionality for managing
 * the lifecycle of stream-based ingestion tasks.
 * </p>
 */
public interface StreamSupervisor extends Supervisor
{
  /**
   * Reset offsets with provided dataSource metadata. The resulting stored offsets should be a union of existing checkpointed
   * offsets with provided offsets.
   * @param resetDataSourceMetadata required datasource metadata with offsets to reset.
   * @throws DruidException if any metadata attribute doesn't match the supervisor's state.
   */
  void resetOffsets(DataSourceMetadata resetDataSourceMetadata);

  /**
   * The definition of checkpoint is not very strict as currently it does not affect data or control path.
   * On this call Supervisor can potentially checkpoint data processed so far to some durable storage
   * for example - Kafka/Kinesis Supervisor uses this to merge and handoff segments containing at least the data
   * represented by {@param currentCheckpoint} DataSourceMetadata
   *
   * @param taskGroupId        unique Identifier to figure out for which sequence to do checkpointing
   * @param checkpointMetadata metadata for the sequence to currently checkpoint
   */
  void checkpoint(int taskGroupId, DataSourceMetadata checkpointMetadata);

  /**
   * Computes maxLag, totalLag and avgLag
   */
  LagStats computeLagStats();

  int getActiveTaskGroupsCount();

  /**
   * Marks the given task groups as ready for segment hand-off irrespective of the task run times.
   * In the subsequent run, the supervisor initiates segment publish and hand-off for these task groups and rolls over their tasks.
   * taskGroupIds that are not valid or not actively reading are simply ignored.
   */
  default void handoffTaskGroupsEarly(List<Integer> taskGroupIds)
  {
    throw new UnsupportedOperationException("Supervisor does not have the feature to handoff task groups early implemented");
  }
}
