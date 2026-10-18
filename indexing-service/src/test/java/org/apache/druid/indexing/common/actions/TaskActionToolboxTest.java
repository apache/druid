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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.ForkingTaskRunner;
import org.apache.druid.indexing.overlord.ForkingTaskRunnerFactory;
import org.apache.druid.indexing.overlord.ObjectMetadata;
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.indexing.overlord.supervisor.SupervisorManager;
import org.apache.druid.server.coordinator.simulate.BlockingExecutorService;
import org.apache.druid.server.coordinator.simulate.WrappingScheduledExecutorService;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TaskActionToolboxTest
{
  private TaskActionToolbox toolbox;
  private SupervisorManager supervisorManager;
  private BlockingExecutorService taskActionExec;

  @BeforeEach
  public void setup()
  {
    taskActionExec = new BlockingExecutorService("test");
    supervisorManager = mock(SupervisorManager.class);
    toolbox = new TaskActionToolbox(
        null,
        null,
        null,
        null,
        null,
        supervisorManager,
        null,
        (_, nameFormat) -> new WrappingScheduledExecutorService(nameFormat, taskActionExec, true)
    );
  }

  @Test
  public void test_setTaskRunnerFactory()
  {
    TaskActionToolbox toolbox = new TaskActionToolbox(null, null, null, null, null, null);
    assertFalse(toolbox.getTaskRunner().isPresent());
    ForkingTaskRunnerFactory factory = mock(ForkingTaskRunnerFactory.class);
    when(factory.get()).thenReturn(mock(ForkingTaskRunner.class));
    toolbox.setTaskRunnerFactory(factory);
    assertTrue(toolbox.getTaskRunner().isPresent());
  }

  @Test
  public void test_publishSegmentsWhenReady_waits_untilTaskIsReady()
  {
    final Task task = NoopTask.create();
    final String supervisorId = "supervisor1";
    final DataSourceMetadata startMetadata = new ObjectMetadata("abc");

    final SettableFuture<Boolean> taskReadyFuture = SettableFuture.create();

    when(supervisorManager.isTaskReadyToPublishSegments(supervisorId, task.getId(), startMetadata))
        .thenReturn(taskReadyFuture);

    final Set<DataSegment> segmentsToPublish = Set.of(DataSegment.builder(SegmentId.dummy("wiki")).build());
    final ListenableFuture<SegmentPublishResult> publishFuture = toolbox.publishSegmentsWhenReady(
        task,
        supervisorId,
        startMetadata,
        (_, _) -> SegmentPublishResult.ok(segmentsToPublish)
    );

    assertFalse(taskReadyFuture.isDone());
    assertFalse(publishFuture.isDone());

    // Mark the task as ready to publish and verify that publish proceeds
    taskReadyFuture.set(true);
    taskActionExec.finishAllPendingTasks();
    assertTrue(publishFuture.isDone());
    assertTrue(taskReadyFuture.isDone());
    assertEquals(segmentsToPublish, getResult(publishFuture).getSegments());
  }

  @Test
  public void test_publishSegmentsWhenReady_proceedsImmediately_ifSupervisorIdIsNull()
  {
    final Task task = NoopTask.create();

    final Set<DataSegment> segmentsToPublish = Set.of(DataSegment.builder(SegmentId.dummy("wiki")).build());
    final ListenableFuture<SegmentPublishResult> publishFuture = toolbox.publishSegmentsWhenReady(
        task,
        null,
        null,
        (_, _) -> SegmentPublishResult.ok(segmentsToPublish)
    );

    assertFalse(publishFuture.isDone());

    // Verify that the publish proceeds immediately
    taskActionExec.finishAllPendingTasks();
    assertTrue(publishFuture.isDone());
    assertEquals(segmentsToPublish, getResult(publishFuture).getSegments());

    verify(supervisorManager, never()).isTaskReadyToPublishSegments(any(), eq(task.getId()), any());
  }

  @Test
  public void test_publishSegmentsWhenReady_returnsNonRetryableFailure_ifOffsetMismatches()
  {
    final Task task = NoopTask.create();
    final String supervisorId = "supervisor1";
    final DataSourceMetadata startMetadata = new ObjectMetadata("abc");

    final SettableFuture<Boolean> taskReadyFuture = SettableFuture.create();

    when(supervisorManager.isTaskReadyToPublishSegments(supervisorId, task.getId(), startMetadata))
        .thenReturn(taskReadyFuture);

    final ListenableFuture<SegmentPublishResult> publishFuture = toolbox.publishSegmentsWhenReady(
        task,
        supervisorId,
        startMetadata,
        (_, _) -> SegmentPublishResult.retryableOffsetMismatchFailure("offset mismatch")
    );

    assertFalse(taskReadyFuture.isDone());
    assertFalse(publishFuture.isDone());

    // Mark the task as ready to publish and verify that publish proceeds
    taskReadyFuture.set(true);
    taskActionExec.finishAllPendingTasks();
    assertTrue(publishFuture.isDone());
    assertTrue(taskReadyFuture.isDone());

    // Verify that the publish failure is non retryable
    final SegmentPublishResult result = getResult(publishFuture);
    assertFalse(result.isRetryable());
    assertEquals("Non-retryable metadata mismatch error: offset mismatch", result.getErrorMsg());
  }

  @Test
  public void test_publishSegmentsWhenReady_returnsRetryableFailure_ifTaskCouldNotBecomeReady()
  {
    final Task task = NoopTask.create();
    final String supervisorId = "supervisor1";
    final DataSourceMetadata startMetadata = new ObjectMetadata("abc");

    final SettableFuture<Boolean> taskReadyFuture = SettableFuture.create();

    when(supervisorManager.isTaskReadyToPublishSegments(supervisorId, task.getId(), startMetadata))
        .thenReturn(taskReadyFuture);

    final Set<DataSegment> segmentsToPublish = Set.of(DataSegment.builder(SegmentId.dummy("wiki")).build());
    final ListenableFuture<SegmentPublishResult> publishFuture = toolbox.publishSegmentsWhenReady(
        task,
        supervisorId,
        startMetadata,
        (_, _) -> SegmentPublishResult.ok(segmentsToPublish)
    );

    assertFalse(taskReadyFuture.isDone());
    assertFalse(publishFuture.isDone());

    // Mark the task as not ready to publish and verify that publish fails
    taskReadyFuture.set(false);
    taskActionExec.finishAllPendingTasks();
    assertTrue(publishFuture.isDone());
    assertTrue(taskReadyFuture.isDone());

    assertEquals(
        SegmentPublishResult.retryableFailure("Task is not ready to publish yet"),
        getResult(publishFuture)
    );
  }

  private SegmentPublishResult getResult(ListenableFuture<SegmentPublishResult> publishFuture)
  {
    return FutureUtils.getUnchecked(publishFuture, true);
  }
}
