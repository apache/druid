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

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.data.input.kafka.KafkaTopicPartition;
import org.apache.druid.indexing.common.actions.SegmentTransactionalAppendFromShareInboxAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.indexing.overlord.ShareInboxManifest;
import org.apache.druid.indexing.seekablestream.StreamChunkReader;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.segment.realtime.appenderator.AppenderatorDriverAddResult;
import org.apache.druid.segment.realtime.appenderator.SegmentsAndCommitMetadata;
import org.apache.druid.segment.realtime.appenderator.StreamAppenderatorDriver;
import org.apache.druid.segment.realtime.appenderator.TransactionalSegmentPublisher;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AppenderatorShareInboxBatchHandlerTest
{
  @Test
  public void testParsesPersistsAndPublishesWithCompletionAction() throws Exception
  {
    final StreamAppenderatorDriver driver = mock(StreamAppenderatorDriver.class);
    final StreamChunkReader<KafkaRecordEntity> chunkReader = chunkReader();
    final TaskActionClient actionClient = mock(TaskActionClient.class);
    final ShareInboxClaimLease claimLease = mock(ShareInboxClaimLease.class);
    final InputRow row = mock(InputRow.class);
    final AppenderatorDriverAddResult addResult = mock(AppenderatorDriverAddResult.class);
    final OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> record = record(1);
    when(chunkReader.parse(any(), anyBoolean())).thenReturn(List.of(row));
    when(addResult.isOk()).thenReturn(true);
    when(driver.add(any(), anyString(), any(), anyBoolean(), anyBoolean())).thenReturn(addResult);
    mockPublish(driver);
    final AppenderatorShareInboxBatchHandler handler = handler(driver, chunkReader, actionClient);

    handler.process(List.of(manifest()), List.of(record), claimLease);

    verify(driver).persist(any());
    final ArgumentCaptor<TransactionalSegmentPublisher> publisherCaptor =
        ArgumentCaptor.forClass(TransactionalSegmentPublisher.class);
    verify(driver).publish(publisherCaptor.capture(), any(), any());
    final TransactionalSegmentPublisher publisher = publisherCaptor.getValue();
    Assertions.assertTrue(publisher.supportsEmptyPublish());
    when(actionClient.submit(any(SegmentTransactionalAppendFromShareInboxAction.class)))
        .thenReturn(SegmentPublishResult.ok(Set.of()));
    publisher.publishAnnotatedSegments(null, Set.of(), null, null);
    final ArgumentCaptor<SegmentTransactionalAppendFromShareInboxAction> actionCaptor =
        ArgumentCaptor.forClass(SegmentTransactionalAppendFromShareInboxAction.class);
    verify(actionClient).submit(actionCaptor.capture());
    Assertions.assertEquals(
        Set.of("manifest-1"),
        actionCaptor.getValue().getCompletionRequest().getClaims().keySet()
    );
  }

  @Test
  public void testZeroParsedRowsStillPublishesCompletion() throws Exception
  {
    final StreamAppenderatorDriver driver = mock(StreamAppenderatorDriver.class);
    final StreamChunkReader<KafkaRecordEntity> chunkReader = chunkReader();
    final TaskActionClient actionClient = mock(TaskActionClient.class);
    final OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> record = record(1);
    when(chunkReader.parse(any(), anyBoolean())).thenReturn(List.of());
    mockPublish(driver);

    handler(driver, chunkReader, actionClient).process(
        List.of(manifest()),
        List.of(record),
        mock(ShareInboxClaimLease.class)
    );

    verify(driver, never()).add(any(), anyString(), any(), anyBoolean(), anyBoolean());
    verify(driver, never()).persist(any());
    verify(driver).publish(any(), any(), any());
  }

  private static AppenderatorShareInboxBatchHandler handler(
      StreamAppenderatorDriver driver,
      StreamChunkReader<KafkaRecordEntity> chunkReader,
      TaskActionClient actionClient
  )
  {
    return new AppenderatorShareInboxBatchHandler(driver, chunkReader, actionClient, "task-1");
  }

  @SuppressWarnings("unchecked")
  private static StreamChunkReader<KafkaRecordEntity> chunkReader()
  {
    return mock(StreamChunkReader.class);
  }

  @SuppressWarnings("unchecked")
  private static void mockPublish(StreamAppenderatorDriver driver) throws Exception
  {
    final ListenableFuture<SegmentsAndCommitMetadata> future = mock(ListenableFuture.class);
    when(future.get()).thenReturn(mock(SegmentsAndCommitMetadata.class));
    when(driver.publish(any(), any(), any())).thenReturn(future);
  }

  private static ShareInboxManifest manifest()
  {
    return new ShareInboxManifest(
        "manifest-1",
        "datasource",
        "inbox",
        "group",
        "cluster-id",
        "topic-id",
        "topic",
        2,
        "fingerprint",
        "path/manifest-1",
        "hash",
        100,
        List.of(1L),
        1,
        "task-1",
        1,
        DateTimes.nowUtc().plusMinutes(1),
        1
    );
  }

  private static OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> record(long offset)
  {
    return KafkaShareGroupRecord.from(new ConsumerRecord<>("topic", 2, offset, null, new byte[]{1}));
  }
}
