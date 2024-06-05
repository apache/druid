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

package org.apache.druid.indexing.seekablestream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.metadata.PendingSegmentRecord;
import org.apache.druid.segment.incremental.ParseExceptionReport;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public interface SeekableStreamIndexTaskClient<PartitionIdType, SequenceOffsetType>
{
  TypeReference<List<ParseExceptionReport>> TYPE_REFERENCE_LIST_PARSE_EXCEPTION_REPORT =
      new TypeReference<List<ParseExceptionReport>>() {};

  /**
   * Retrieve current task checkpoints.
   *
   * Task-side is {@link SeekableStreamIndexTaskRunner#getCheckpointsHTTP}.
   *
   * @param id    task id
   * @param retry whether to retry on failure
   */
  ListenableFuture<TreeMap<Integer, Map<PartitionIdType, SequenceOffsetType>>> getCheckpointsAsync(
      String id,
      boolean retry
  );

  /**
   * Stop a task. Retries on failure. Returns true if the task was stopped, or false if it was not stopped, perhaps
   * due to an error.
   *
   * Task-side is {@link SeekableStreamIndexTaskRunner#stop}.
   *
   * @param id      task id
   * @param publish whether to publish already-processed data before stopping
   */
  ListenableFuture<Boolean> stopAsync(String id, boolean publish);

  /**
   * Resume a task after a call to {@link #pauseAsync}. Retries on failure. Returns true if the task was
   * resumed, or false if it was not resumed, perhaps due to an error.
   *
   * Task-side is {@link SeekableStreamIndexTaskRunner#resumeHTTP}.
   *
   * @param id task id
   */
  ListenableFuture<Boolean> resumeAsync(String id);

  /**
   * Get the time a task actually started executing. May be later than the start time reported by the task runner,
   * since there is some delay between a task being scheduled and it actually starting to execute.
   *
   * Returns a future that resolves to null if the task has not yet started up.
   *
   * Task-side is {@link SeekableStreamIndexTaskRunner#getStartTime}.
   *
   * @param id task id
   */
  ListenableFuture<DateTime> getStartTimeAsync(String id);

  /**
   * Pause a task.
   *
   * Calls {@link SeekableStreamIndexTaskRunner#pauseHTTP} task-side to do the initial pause, then uses
   * {@link SeekableStreamIndexTaskRunner#getStatusHTTP} in a loop to wait for the task to pause, then uses
   * {@link SeekableStreamIndexTaskRunner#getCurrentOffsets} to retrieve the post-pause offsets.
   *
   * @param id task id
   */
  ListenableFuture<Map<PartitionIdType, SequenceOffsetType>> pauseAsync(String id);

  /**
   * Set end offsets for a task. Retries on failure.
   *
   * Task-side is {@link SeekableStreamIndexTaskRunner#setEndOffsetsHTTP}.
   *
   * @param id         task id
   * @param endOffsets the end offsets
   * @param finalize   whether these are the final offsets for a task (true) or an incremental checkpoint (false)
   */
  ListenableFuture<Boolean> setEndOffsetsAsync(
      String id,
      Map<PartitionIdType, SequenceOffsetType> endOffsets,
      boolean finalize
  );

  /**
   * Retrieve current offsets for a task.
   *
   * Task-side is {@link SeekableStreamIndexTaskRunner#getCurrentOffsets}.
   *
   * @param id    task id
   * @param retry whether to retry on failure
   */
  ListenableFuture<Map<PartitionIdType, SequenceOffsetType>> getCurrentOffsetsAsync(String id, boolean retry);

  /**
   * Retrieve ending offsets for a task. Retries on failure.
   *
   * Task-side is {@link SeekableStreamIndexTaskRunner#getEndOffsetsHTTP}.
   *
   * @param id task id
   */
  ListenableFuture<Map<PartitionIdType, SequenceOffsetType>> getEndOffsetsAsync(String id);

  /**
   * Get processing statistics for a task. Retries on failure.
   *
   * Task-side is {@link SeekableStreamIndexTaskRunner#getRowStats}.
   *
   * @param id task id
   */
  ListenableFuture<Map<String, Object>> getMovingAveragesAsync(String id);

  /**
   * Get parse errors for a task. Retries on failure.
   *
   * Task-side is {@link SeekableStreamIndexTaskRunner#getUnparseableEvents}.
   *
   * @param id task id
   */
  ListenableFuture<List<ParseExceptionReport>> getParseErrorsAsync(String id);

  /**
   * Get current status for a task. Retries on failure.
   *
   * Returns {@link SeekableStreamIndexTaskRunner.Status#NOT_STARTED} if the task has not yet started.
   *
   * Task-side is {@link SeekableStreamIndexTaskRunner#getStatusHTTP}.
   *
   * @param id task id
   */
  ListenableFuture<SeekableStreamIndexTaskRunner.Status> getStatusAsync(String id);

  /**
   * Update the task state to redirect queries for later versions to the root pending segment.
   * The task also announces that it is serving the segments belonging to the subsequent versions.
   * The update is processed only if the task is serving the original pending segment.
   *
   * @param taskId               - task id
   * @param pendingSegmentRecord - the ids belonging to the versions to which the root segment needs to be updated
   * @return true if the update succeeds
   */
  ListenableFuture<Boolean> registerNewVersionOfPendingSegmentAsync(
      String taskId,
      PendingSegmentRecord pendingSegmentRecord
  );

  Class<PartitionIdType> getPartitionType();

  Class<SequenceOffsetType> getSequenceType();

  void close();
}


