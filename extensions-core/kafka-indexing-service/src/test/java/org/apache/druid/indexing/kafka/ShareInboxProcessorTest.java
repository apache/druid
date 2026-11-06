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

package org.apache.druid.indexing.kafka;

import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.data.input.kafka.KafkaTopicPartition;
import org.apache.druid.indexing.common.actions.ClaimShareInboxManifestsAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.overlord.ShareInboxClaimResult;
import org.apache.druid.indexing.overlord.ShareInboxManifest;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ShareInboxProcessorTest
{
  @Test
  public void testClaimsReadsAndProcessesOneFencedBatch() throws Exception
  {
    final TaskActionClient actionClient = mock(TaskActionClient.class);
    final ShareInboxRecordSource recordSource = mock(ShareInboxRecordSource.class);
    final ShareInboxBatchHandler batchHandler = mock(ShareInboxBatchHandler.class);
    final ScheduledExecutorService scheduler = scheduler();
    final ShareInboxManifest first = manifest("manifest-1", 1, "task-1");
    final ShareInboxManifest second = manifest("manifest-2", 2, "task-1");
    final OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> firstRecord = record(1);
    final OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> secondRecord = record(2);
    when(actionClient.submit(any(ClaimShareInboxManifestsAction.class)))
        .thenReturn(new ShareInboxClaimResult(List.of(first, second)));
    when(recordSource.read(first)).thenReturn(List.of(firstRecord));
    when(recordSource.read(second)).thenReturn(List.of(secondRecord));
    final ShareInboxProcessor processor = processor(actionClient, recordSource, batchHandler, scheduler);

    Assertions.assertTrue(processor.processNext());

    final ArgumentCaptor<ClaimShareInboxManifestsAction> claimCaptor =
        ArgumentCaptor.forClass(ClaimShareInboxManifestsAction.class);
    verify(actionClient).submit(claimCaptor.capture());
    Assertions.assertEquals("task-1", claimCaptor.getValue().getRequest().getClaimOwner());
    verify(batchHandler).process(eq(List.of(first, second)), eq(List.of(firstRecord, secondRecord)), any());
  }

  @Test
  public void testNoClaimedWorkDoesNotInvokeHandler() throws Exception
  {
    final TaskActionClient actionClient = mock(TaskActionClient.class);
    final ShareInboxRecordSource recordSource = mock(ShareInboxRecordSource.class);
    final ShareInboxBatchHandler batchHandler = mock(ShareInboxBatchHandler.class);
    when(actionClient.submit(any(ClaimShareInboxManifestsAction.class)))
        .thenReturn(new ShareInboxClaimResult(List.of()));
    final ShareInboxProcessor processor = processor(actionClient, recordSource, batchHandler, scheduler());

    Assertions.assertFalse(processor.processNext());

    verify(recordSource, never()).read(any());
    verify(batchHandler, never()).process(any(), any(), any());
  }

  @Test
  public void testRejectsManifestClaimedForAnotherTask() throws Exception
  {
    final TaskActionClient actionClient = mock(TaskActionClient.class);
    final ShareInboxRecordSource recordSource = mock(ShareInboxRecordSource.class);
    final ShareInboxBatchHandler batchHandler = mock(ShareInboxBatchHandler.class);
    when(actionClient.submit(any(ClaimShareInboxManifestsAction.class)))
        .thenReturn(new ShareInboxClaimResult(List.of(manifest("manifest-1", 1, "other-task"))));
    final ShareInboxProcessor processor = processor(actionClient, recordSource, batchHandler, scheduler());

    Assertions.assertThrows(ISE.class, processor::processNext);

    verify(recordSource, never()).read(any());
    verify(batchHandler, never()).process(any(), any(), any());
  }

  @Test
  public void testStopDuringBatchSuppressesLeaseCancellationFailure() throws Exception
  {
    final TaskActionClient actionClient = mock(TaskActionClient.class);
    final ShareInboxRecordSource recordSource = mock(ShareInboxRecordSource.class);
    final ShareInboxBatchHandler batchHandler = mock(ShareInboxBatchHandler.class);
    final ShareInboxManifest manifest = manifest("manifest-1", 1, "task-1");
    when(actionClient.submit(any(ClaimShareInboxManifestsAction.class)))
        .thenReturn(new ShareInboxClaimResult(List.of(manifest)));
    when(recordSource.read(manifest)).thenReturn(List.of(record(1)));
    final AtomicReference<ShareInboxProcessor> processorReference = new AtomicReference<>();
    org.mockito.Mockito.doAnswer(invocation -> {
      processorReference.get().requestStop();
      throw new ISE("claim lease invalidated by stop");
    }).when(batchHandler).process(any(), any(), any());
    final ShareInboxProcessor processor = processor(actionClient, recordSource, batchHandler, scheduler());
    processorReference.set(processor);

    processor.run();

    verify(batchHandler).process(any(), any(), any());
  }

  private static ShareInboxProcessor processor(
      TaskActionClient actionClient,
      ShareInboxRecordSource recordSource,
      ShareInboxBatchHandler batchHandler,
      ScheduledExecutorService scheduler
  )
  {
    return new ShareInboxProcessor(
        actionClient,
        recordSource,
        batchHandler,
        scheduler,
        "datasource",
        "inbox",
        "fingerprint",
        "task-1",
        10,
        1_000,
        1_000_000,
        60_000,
        10_000,
        100
    );
  }

  @SuppressWarnings("unchecked")
  private static ScheduledExecutorService scheduler()
  {
    final ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    when(scheduler.scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any()))
        .thenReturn(mock(ScheduledFuture.class));
    return scheduler;
  }

  private static ShareInboxManifest manifest(String manifestId, long claimEpoch, String claimOwner)
  {
    return new ShareInboxManifest(
        manifestId,
        "datasource",
        "inbox",
        "group",
        "cluster-id",
        "topic-id",
        "topic",
        2,
        "fingerprint",
        "path/" + manifestId,
        "hash",
        100,
        List.of(claimEpoch),
        1,
        claimOwner,
        claimEpoch,
        DateTimes.nowUtc().plusMinutes(1),
        1
    );
  }

  private static OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> record(long offset)
  {
    return KafkaShareGroupRecord.from(new ConsumerRecord<>("topic", 2, offset, null, new byte[]{1}));
  }
}
