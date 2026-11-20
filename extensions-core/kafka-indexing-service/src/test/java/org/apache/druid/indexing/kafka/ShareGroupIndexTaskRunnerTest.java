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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.SegmentTransactionalAppendAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.indexing.seekablestream.StreamChunkReader;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.realtime.appenderator.AppenderatorDriverAddResult;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.segment.realtime.appenderator.SegmentsAndCommitMetadata;
import org.apache.druid.segment.realtime.appenderator.StreamAppenderatorDriver;
import org.apache.druid.segment.realtime.appenderator.TransactionalSegmentPublisher;
import org.apache.druid.testing.junit.LoggerCaptureExtension;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ShareGroupIndexTaskRunnerTest
{
  private static final TopicPartition PARTITION = new TopicPartition("topic", 0);
  private static final TopicIdPartition TOPIC_ID_PARTITION = new TopicIdPartition(Uuid.randomUuid(), PARTITION);
  private static final ConsumerRecord<byte[], byte[]> RECORD = new ConsumerRecord<>(
      PARTITION.topic(),
      PARTITION.partition(),
      0L,
      null,
      new byte[]{1}
  );
  private static final DataSegment SEGMENT = DataSegment.builder(SegmentId.of("data", Intervals.of("2026-01-01/2026-01-02"), "version", 0))
      .loadSpec(Map.of("path", "segment.zip"))
      .size(1)
      .build();

  @RegisterExtension
  public final LoggerCaptureExtension logCapture = new LoggerCaptureExtension(ShareGroupIndexTaskRunner.class);

  private final TaskToolbox toolbox = Mockito.mock(TaskToolbox.class);
  private final ShareGroupIndexTask task = Mockito.mock(ShareGroupIndexTask.class);
  private final TaskActionClient actionClient = Mockito.mock(TaskActionClient.class);
  private final StreamAppenderatorDriver driver = Mockito.mock(StreamAppenderatorDriver.class);
  private final AtomicBoolean stopped = new AtomicBoolean();
  private final ShareGroupIndexTaskRunner runner = new ShareGroupIndexTaskRunner(task, toolbox);

  @SuppressWarnings("unchecked")
  private final ShareConsumer<byte[], byte[]> consumer = Mockito.mock(ShareConsumer.class);
  @SuppressWarnings("unchecked")
  private final StreamChunkReader<KafkaRecordEntity> reader = Mockito.mock(StreamChunkReader.class);

  @BeforeEach
  public void setUp() throws IOException
  {
    Mockito.when(task.getIOConfig()).thenReturn(new ShareGroupIndexTaskIOConfig(
        PARTITION.topic(),
        "group",
        Map.of(),
        new JsonInputFormat(null, null, null, null, null),
        null
    ));
    Mockito.when(task.getTuningConfig()).thenReturn(Mockito.mock(KafkaIndexTaskTuningConfig.class));
    Mockito.when(task.isStopRequested()).thenAnswer(invocation -> stopped.get());
    Mockito.when(toolbox.getTaskActionClient()).thenReturn(actionClient);
    Mockito.when(consumer.acquisitionLockTimeoutMs()).thenReturn(Optional.of(30_000));
    Mockito.when(consumer.poll(Mockito.any())).thenReturn(
        new ConsumerRecords<>(Map.of(PARTITION, List.of(RECORD)), Map.of())
    );
    Mockito.when(reader.parse(Mockito.anyList(), Mockito.eq(false))).thenReturn(List.of(Mockito.mock(InputRow.class)));
    Mockito.when(driver.add(Mockito.any(), Mockito.anyString(), Mockito.any(), Mockito.eq(true), Mockito.eq(true)))
           .thenReturn(AppenderatorDriverAddResult.ok(SegmentIdWithShardSpec.fromDataSegment(SEGMENT), 1, 1, true));
    Mockito.when(driver.registerHandoff(Mockito.any())).thenReturn(Futures.immediateFuture(null));
    stopAfterAcknowledgedBatches(1);
  }

  @Test
  public void testAcknowledgesAfterPublishing() throws Exception
  {
    preparePublishResult(SegmentPublishResult.ok(Set.of(SEGMENT)));

    runner.runLoop(consumer, reader, driver, 300_000);

    final InOrder order = Mockito.inOrder(actionClient, consumer);
    order.verify(actionClient).submit(Mockito.any(SegmentTransactionalAppendAction.class));
    order.verify(consumer).acknowledge(RECORD, AcknowledgeType.ACCEPT);
    order.verify(consumer).commitSync(Mockito.any());
    Mockito.verify(driver).registerHandoff(Mockito.any());
  }

  @Test
  public void testDoesNotAcknowledgeFailedPublication()
  {
    Mockito.when(driver.publish(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(
        Futures.immediateFailedFuture(new IOException("Publish failed"))
    );

    Assertions.assertThrows(ExecutionException.class, () -> runner.runLoop(consumer, reader, driver, 300_000));
    Mockito.verify(consumer, Mockito.never()).acknowledge(Mockito.any(), Mockito.any());
    Mockito.verify(consumer, Mockito.never()).commitSync(Mockito.any());
  }

  @Test
  public void testDoesNotAcknowledgePublicationOfDifferentLoadSpec() throws IOException
  {
    final DataSegment otherSegment = DataSegment.builder(SEGMENT).loadSpec(Map.of("path", "other.zip")).build();
    preparePublishResult(SegmentPublishResult.ok(Set.of(otherSegment)));

    Assertions.assertThrows(ISE.class, () -> runner.runLoop(consumer, reader, driver, 300_000));
    Mockito.verify(consumer, Mockito.never()).acknowledge(Mockito.any(), Mockito.any());
  }

  @Test
  public void testAcknowledgementFailureLeavesPublicationInPlace() throws IOException
  {
    preparePublishResult(SegmentPublishResult.ok(Set.of(SEGMENT)));
    Mockito.doReturn(Map.of(TOPIC_ID_PARTITION, Optional.of(new KafkaException("Acquisition expired"))))
           .when(consumer).commitSync(Mockito.any());

    Assertions.assertThrows(ISE.class, () -> runner.runLoop(consumer, reader, driver, 300_000));
    Mockito.verify(driver).publish(Mockito.any(), Mockito.any(), Mockito.any());
    Mockito.verify(driver).registerHandoff(Mockito.any());
    Mockito.verify(consumer).poll(Mockito.any());
  }

  @Test
  public void testAcknowledgesBatchWithNoInputRowsWithoutPublishing() throws Exception
  {
    Mockito.when(reader.parse(Mockito.anyList(), Mockito.eq(false))).thenReturn(List.of());

    runner.runLoop(consumer, reader, driver, 300_000);

    Mockito.verify(driver, Mockito.never()).publish(Mockito.any(), Mockito.any(), Mockito.any());
    Mockito.verify(consumer).acknowledge(RECORD, AcknowledgeType.ACCEPT);
    Mockito.verify(consumer).commitSync(Mockito.any());
  }

  @Test
  public void testWaitsForHandoffCapacityAndResumesWhenLaterHandoffCompletes() throws Exception
  {
    preparePublishResult(SegmentPublishResult.ok(Set.of(SEGMENT)));
    stopAfterAcknowledgedBatches(9);
    Mockito.when(task.getTuningConfig().getHandoffConditionTimeout()).thenReturn(1L);
    final ListenableFuture<SegmentsAndCommitMetadata> firstHandoff = Mockito.mock();
    final SettableFuture<SegmentsAndCommitMetadata> laterHandoff = SettableFuture.create();
    final SettableFuture<SegmentsAndCommitMetadata> pending = SettableFuture.create();
    Mockito.when(driver.registerHandoff(Mockito.any())).thenReturn(firstHandoff)
           .thenReturn(laterHandoff).thenReturn(pending);
    Mockito.when(firstHandoff.get(Mockito.anyLong(), Mockito.eq(TimeUnit.MILLISECONDS)))
           .thenAnswer(invocation -> pending.get(invocation.getArgument(0), TimeUnit.MILLISECONDS))
           .thenAnswer(invocation -> {
             Mockito.verify(consumer, Mockito.times(8)).poll(Mockito.any());
             Mockito.verify(consumer, Mockito.times(8)).commitSync(Mockito.any());
             laterHandoff.set(null);
             throw new TimeoutException();
           });

    runner.runLoop(consumer, reader, driver, 300_000);

    Assertions.assertTrue(laterHandoff.isDone());
    Mockito.verify(consumer, Mockito.times(9)).poll(Mockito.any());
    Mockito.verify(consumer, Mockito.times(9)).commitSync(Mockito.any());
    Assertions.assertEquals(1, logCapture.getLogEvents().stream()
                                        .filter(e -> e.getMessage().getFormattedMessage().contains("pending batch limit"))
                                        .count());
  }

  @Test
  public void testStopsWhileWaitingForHandoffCapacity() throws Exception
  {
    preparePublishResult(SegmentPublishResult.ok(Set.of(SEGMENT)));
    stopAfterAcknowledgedBatches(9);
    final ListenableFuture<SegmentsAndCommitMetadata> firstHandoff = Mockito.mock();
    Mockito.when(driver.registerHandoff(Mockito.any())).thenReturn(firstHandoff).thenReturn(SettableFuture.create());
    Mockito.when(firstHandoff.get(Mockito.anyLong(), Mockito.eq(TimeUnit.MILLISECONDS))).thenAnswer(invocation -> {
      stopped.set(true);
      throw new TimeoutException();
    });

    runner.runLoop(consumer, reader, driver, 300_000);

    Mockito.verify(consumer, Mockito.times(8)).poll(Mockito.any());
    Mockito.verify(consumer, Mockito.times(8)).commitSync(Mockito.any());
  }

  private void stopAfterAcknowledgedBatches(int batches)
  {
    final AtomicInteger count = new AtomicInteger();
    Mockito.doAnswer(invocation -> {
      stopped.set(count.incrementAndGet() >= batches);
      return Map.of(TOPIC_ID_PARTITION, Optional.empty());
    }).when(consumer).commitSync(Mockito.any());
  }

  private void preparePublishResult(SegmentPublishResult result) throws IOException
  {
    Mockito.when(actionClient.submit(Mockito.any(SegmentTransactionalAppendAction.class))).thenReturn(result);
    Mockito.when(driver.publish(Mockito.any(), Mockito.any(), Mockito.any())).thenAnswer(invocation -> {
      final TransactionalSegmentPublisher publisher = invocation.getArgument(0);
      publisher.publishAnnotatedSegments(null, Set.of(SEGMENT), null, null);
      return Futures.immediateFuture(new SegmentsAndCommitMetadata(List.of(SEGMENT), null).withWasPublished(true));
    });
  }
}
