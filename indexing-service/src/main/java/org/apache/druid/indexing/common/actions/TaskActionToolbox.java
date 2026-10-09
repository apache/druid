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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.GlobalTaskLockbox;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.TaskRunnerFactory;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.supervisor.SupervisorManager;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;

import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiFunction;

public class TaskActionToolbox
{
  private final GlobalTaskLockbox taskLockbox;
  private final TaskStorage taskStorage;
  private final SegmentAllocationQueue segmentAllocationQueue;
  private final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
  private final ServiceEmitter emitter;
  private final SupervisorManager supervisorManager;
  private final ObjectMapper jsonMapper;
  private final ScheduledExecutorService actionExec;
  private Optional<TaskRunnerFactory> factory = Optional.absent();

  @Inject
  public TaskActionToolbox(
      GlobalTaskLockbox taskLockbox,
      TaskStorage taskStorage,
      IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
      SegmentAllocationQueue segmentAllocationQueue,
      ServiceEmitter emitter,
      SupervisorManager supervisorManager,
      @Json ObjectMapper jsonMapper,
      ScheduledExecutorFactory scheduledExecutorFactory
  )
  {
    this.taskLockbox = taskLockbox;
    this.taskStorage = taskStorage;
    this.indexerMetadataStorageCoordinator = indexerMetadataStorageCoordinator;
    this.emitter = emitter;
    this.supervisorManager = supervisorManager;
    this.jsonMapper = jsonMapper;
    this.segmentAllocationQueue = segmentAllocationQueue;

    // This executor is currently used only for delayed segment publish actions.
    // 4 threads are enough since each publish operation is expected to be fast.
    this.actionExec = scheduledExecutorFactory.create(4, "TaskActionToolbox-%s");
  }

  public TaskActionToolbox(
      GlobalTaskLockbox taskLockbox,
      TaskStorage taskStorage,
      IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
      ServiceEmitter emitter,
      SupervisorManager supervisorManager,
      @Json ObjectMapper jsonMapper
  )
  {
    this(
        taskLockbox,
        taskStorage,
        indexerMetadataStorageCoordinator,
        null,
        emitter,
        supervisorManager,
        jsonMapper,
        ScheduledExecutors::fixed
    );
  }

  public GlobalTaskLockbox getTaskLockbox()
  {
    return taskLockbox;
  }

  public TaskStorage getTaskStorage()
  {
    return taskStorage;
  }

  public IndexerMetadataStorageCoordinator getIndexerMetadataStorageCoordinator()
  {
    return indexerMetadataStorageCoordinator;
  }

  public ServiceEmitter getEmitter()
  {
    return emitter;
  }

  public SupervisorManager getSupervisorManager()
  {
    return supervisorManager;
  }

  public ObjectMapper getJsonMapper()
  {
    return jsonMapper;
  }

  @Inject(optional = true)
  public void setTaskRunnerFactory(TaskRunnerFactory factory)
  {
    this.factory = Optional.of(factory);
  }

  public Optional<TaskRunner> getTaskRunner()
  {
    if (factory.isPresent()) {
      return Optional.of(factory.get().get());
    }
    return Optional.absent();
  }

  public SegmentAllocationQueue getSegmentAllocationQueue()
  {
    return segmentAllocationQueue;
  }

  public boolean canBatchSegmentAllocation()
  {
    return segmentAllocationQueue != null && segmentAllocationQueue.isEnabled();
  }

  /**
   * Performs the segment publish action when the given task is unblocked for publish.
   * A streaming task must wait for previously created tasks that are
   * yet to publish offsets for an overlapping set of partitions.
   */
  public ListenableFuture<SegmentPublishResult> publishSegmentsWhenReady(
      Task task,
      String supervisorId,
      DataSourceMetadata startMetadata,
      BiFunction<Task, TaskActionToolbox, SegmentPublishResult> publishAction
  )
  {
    // First try publishing synchronously
    try {
      final SegmentPublishResult firstAttemptResult = publishAction.apply(task, this);
      if (firstAttemptResult.isSuccess()
          || !firstAttemptResult.isOffsetMismatch()
          || !firstAttemptResult.isRetryable()) {
        return Futures.immediateFuture(firstAttemptResult);
      }
    }
    catch (Exception e) {
      return Futures.immediateFailedFuture(e);
    }

    // Try publishing later if the failure was due to offset mismatch
    final ListenableFuture<Boolean> taskReadyToPublishFuture = supervisorManager.isTaskReadyToPublishSegments(
        supervisorId,
        task.getId(),
        startMetadata
    );

    return Futures.transform(
        taskReadyToPublishFuture,
        readyToPublish -> {
          if (Boolean.TRUE.equals(readyToPublish)) {
            final SegmentPublishResult result = publishAction.apply(task, this);
            if (result.isOffsetMismatch() && !result.isSuccess() && result.isRetryable()) {
              // Do not retry offset mismatch failures since task is already
              // unblocked for publish and retrying will not fix the mismatch.
              return SegmentPublishResult.fail(result.getErrorMsg());
            } else {
              return result;
            }
          } else {
            return SegmentPublishResult.retryableFailure("Task is not ready to publish yet");
          }
        },
        actionExec
    );
  }
}
