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

import org.apache.druid.error.DruidException;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.GlobalTaskLockbox;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.LockResult;
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.indexing.overlord.ShareInboxBatch;
import org.apache.druid.indexing.overlord.ShareInboxClaimRequest;
import org.apache.druid.indexing.overlord.ShareInboxCompletionRequest;
import org.apache.druid.indexing.overlord.ShareInboxManifest;
import org.apache.druid.indexing.overlord.TimeChunkLockRequest;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.joda.time.Interval;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SegmentTransactionalAppendFromShareInboxActionTest
{
  private static final String DATA_SOURCE = "datasource";
  private static final String TASK_ID = "task-1";
  private static final Interval INTERVAL = Intervals.of("2026-01-01/2026-01-02");

  @RegisterExtension
  public final TaskActionTestKit actionTestKit = new TaskActionTestKit();

  @Test
  public void testSerde() throws Exception
  {
    final SegmentTransactionalAppendFromShareInboxAction original = new SegmentTransactionalAppendFromShareInboxAction(
        Set.of(),
        completionRequest("manifest-1", 2L),
        null
    );

    final TaskAction<?> deserialized = TestHelper.JSON_MAPPER.readValue(
        TestHelper.JSON_MAPPER.writeValueAsBytes(original),
        TaskAction.class
    );

    Assertions.assertInstanceOf(SegmentTransactionalAppendFromShareInboxAction.class, deserialized);
    final SegmentTransactionalAppendFromShareInboxAction action =
        (SegmentTransactionalAppendFromShareInboxAction) deserialized;
    Assertions.assertEquals(original.getSegments(), action.getSegments());
    Assertions.assertEquals(original.getCompletionRequest().getClaims(), action.getCompletionRequest().getClaims());
  }

  @Test
  public void testPerformCompletesZeroRowManifest()
  {
    final NoopTask task = task();
    final ShareInboxCompletionRequest request = completionRequest("manifest-1", 2L);
    final SegmentTransactionalAppendFromShareInboxAction action =
        new SegmentTransactionalAppendFromShareInboxAction(Set.of(), request, null);
    final TaskActionToolbox toolbox = mock(TaskActionToolbox.class);
    final GlobalTaskLockbox lockbox = mock(GlobalTaskLockbox.class);
    final IndexerMetadataStorageCoordinator coordinator = mock(IndexerMetadataStorageCoordinator.class);
    final SegmentPublishResult expected = SegmentPublishResult.ok(Set.of());
    when(toolbox.getTaskLockbox()).thenReturn(lockbox);
    when(toolbox.getIndexerMetadataStorageCoordinator()).thenReturn(coordinator);
    when(toolbox.getEmitter()).thenReturn(new StubServiceEmitter());
    when(lockbox.findLocksForTask(task)).thenReturn(List.of());
    when(coordinator.commitAppendSegmentsAndShareInbox(Set.of(), Map.of(), TASK_ID, null, request))
        .thenReturn(expected);

    Assertions.assertSame(expected, action.perform(task, toolbox));
    verify(coordinator).commitAppendSegmentsAndShareInbox(Set.of(), Map.of(), TASK_ID, null, request);
  }

  @Test
  public void testPerformRejectsDifferentClaimOwner()
  {
    final NoopTask task = task();
    final ShareInboxCompletionRequest request = new ShareInboxCompletionRequest(
        DATA_SOURCE,
        "inbox",
        "fingerprint",
        "other-task",
        Map.of("manifest-1", 2L),
        "completion-1"
    );
    final SegmentTransactionalAppendFromShareInboxAction action =
        new SegmentTransactionalAppendFromShareInboxAction(Set.of(), request, null);
    final TaskActionToolbox toolbox = mock(TaskActionToolbox.class);
    final IndexerMetadataStorageCoordinator coordinator = mock(IndexerMetadataStorageCoordinator.class);
    when(toolbox.getIndexerMetadataStorageCoordinator()).thenReturn(coordinator);

    Assertions.assertThrows(DruidException.class, () -> action.perform(task, toolbox));
    verify(coordinator, never()).commitAppendSegmentsAndShareInbox(any(), any(), any(), any(), any());
  }

  @Test
  public void testPerformRejectsTaskWithoutPendingSegmentAllocation()
  {
    final Task task = mock(Task.class);
    final SegmentTransactionalAppendFromShareInboxAction action =
        new SegmentTransactionalAppendFromShareInboxAction(Set.of(), completionRequest("manifest-1", 2L), null);
    when(task.getId()).thenReturn(TASK_ID);
    when(task.getType()).thenReturn("test");

    Assertions.assertThrows(DruidException.class, () -> action.perform(task, mock(TaskActionToolbox.class)));
  }

  @Test
  public void testPerformPublishesSegmentAndCompletesInbox() throws Exception
  {
    actionTestKit.getTestDerbyConnector().createShareReceiptsTable();
    actionTestKit.getTestDerbyConnector().createShareInboxTable();
    final NoopTask task = task();
    actionTestKit.getTaskLockbox().add(task);
    final LockResult lockResult = actionTestKit.getTaskLockbox().lock(
        task,
        new TimeChunkLockRequest(TaskLockType.APPEND, task, INTERVAL, null),
        5_000
    );
    Assertions.assertTrue(lockResult.isOk());
    final DataSegment segment = segment(lockResult.getTaskLock().getVersion());
    final IndexerMetadataStorageCoordinator coordinator = actionTestKit.getMetadataStorageCoordinator();
    coordinator.stageShareInboxBatch(batch());
    final ShareInboxManifest manifest = coordinator.claimShareInboxManifests(
        new ShareInboxClaimRequest(DATA_SOURCE, "inbox", "fingerprint", TASK_ID, 1, 10, 10_000, 60_000)
    ).getManifests().get(0);
    final ShareInboxCompletionRequest request = completionRequest(manifest.getManifestId(), manifest.getClaimEpoch());
    final SegmentTransactionalAppendFromShareInboxAction action =
        new SegmentTransactionalAppendFromShareInboxAction(Set.of(segment), request, null);

    final SegmentPublishResult first = action.perform(task, actionTestKit.getTaskActionToolbox());
    final SegmentPublishResult retry = action.perform(task, actionTestKit.getTaskActionToolbox());

    Assertions.assertTrue(first.isSuccess());
    Assertions.assertEquals(Set.of(segment), first.getSegments());
    Assertions.assertTrue(retry.isSuccess());
    Assertions.assertTrue(retry.getSegments().isEmpty());
    Assertions.assertEquals(segment, coordinator.retrieveSegmentForId(segment.getId()));
  }

  private static NoopTask task()
  {
    return new NoopTask(TASK_ID, null, DATA_SOURCE, 0, 0, null);
  }

  private static ShareInboxCompletionRequest completionRequest(String manifestId, long claimEpoch)
  {
    return new ShareInboxCompletionRequest(
        DATA_SOURCE,
        "inbox",
        "fingerprint",
        TASK_ID,
        Map.of(manifestId, claimEpoch),
        "completion-1"
    );
  }

  private static ShareInboxBatch batch()
  {
    return new ShareInboxBatch(
        DATA_SOURCE,
        "inbox",
        "group",
        "cluster",
        "topic-id",
        "topic",
        0,
        "fingerprint",
        "manifest-1",
        "path/manifest-1",
        "hash",
        1,
        List.of(1L),
        100
    );
  }

  private static DataSegment segment(String version)
  {
    return new DataSegment(
        DATA_SOURCE,
        INTERVAL,
        version,
        Map.of(),
        List.of("dim"),
        List.of("metric"),
        new LinearShardSpec(0),
        9,
        100
    );
  }
}
