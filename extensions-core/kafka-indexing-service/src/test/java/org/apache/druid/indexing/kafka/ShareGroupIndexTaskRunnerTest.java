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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.data.input.kafka.KafkaTopicPartition;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.seekablestream.StreamChunkReader;
import org.apache.druid.indexing.seekablestream.common.AcknowledgeType;
import org.apache.druid.indexing.seekablestream.common.AcknowledgingRecordSupplier;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.realtime.appenderator.Appenderator;
import org.apache.druid.segment.realtime.appenderator.AppenderatorDriverAddResult;
import org.apache.druid.segment.realtime.appenderator.SegmentsAndCommitMetadata;
import org.apache.druid.segment.realtime.appenderator.StreamAppenderatorDriver;
import org.apache.druid.segment.realtime.appenderator.TransactionalSegmentPublisher;
import org.apache.kafka.common.errors.WakeupException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Lightweight unit tests for {@link ShareGroupIndexTaskRunner}; the run loop
 * itself is covered end-to-end by {@code EmbeddedShareGroupIngestionTest}.
 */
public class ShareGroupIndexTaskRunnerTest
{
  private ObjectMapper mapper;
  private ShareGroupIndexTask task;
  private TaskToolbox toolbox;

  @Before
  public void setUp()
  {
    mapper = new DefaultObjectMapper();
    toolbox = Mockito.mock(TaskToolbox.class);

    final DataSchema dataSchema = DataSchema.builder()
        .withDataSource("test_datasource")
        .withTimestamp(new TimestampSpec("__time", null, null))
        .withDimensions(DimensionsSpec.EMPTY)
        .build();

    final Map<String, Object> consumerProps = ImmutableMap.of("bootstrap.servers", "localhost:9092");
    final ShareGroupIndexTaskIOConfig ioConfig = new ShareGroupIndexTaskIOConfig(
        "test-topic",
        "test-share-group",
        consumerProps,
        null,
        null
    );
    final KafkaIndexTaskTuningConfig tuningConfig = new KafkaIndexTaskTuningConfig(
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );
    task = new ShareGroupIndexTask(
        "task_runner_test",
        null,
        dataSchema,
        tuningConfig,
        ioConfig,
        null,
        mapper
    );
  }

  @Test
  public void testRequestWakeupIsNullSafeWhenNoActiveSupplier()
  {
    final ShareGroupIndexTaskRunner runner = new ShareGroupIndexTaskRunner(task, toolbox, mapper);
    runner.requestWakeup();
  }

  @Test
  public void testStopGracefullyBeforeRunTaskIsSafe()
  {
    Assert.assertFalse(task.isStopRequested());
    task.stopGracefully(null);
    Assert.assertTrue(task.isStopRequested());
  }

  @Test
  public void testRunnerAcceptsCustomSupplierFactory()
  {
    final ShareGroupIndexTaskRunner runner = new ShareGroupIndexTaskRunner(
        task,
        toolbox,
        mapper,
        ioConfig -> Mockito.mock(
            org.apache.druid.indexing.seekablestream.common.AcknowledgingRecordSupplier.class
        )
    );
    Assert.assertNotNull(runner);
    runner.requestWakeup();
  }

  @Test
  public void testCommitFailureMetricName()
  {
    Assert.assertEquals(
        "ingest/shareGroup/commitFailures",
        ShareGroupIndexTaskRunner.METRIC_COMMIT_FAILURES
    );
  }

  @Test
  public void testRunLoop_stopRequestedBeforeFirstPoll_exitsImmediately() throws Exception
  {
    task.stopGracefully(null);

    final StreamAppenderatorDriver driver = mockDriver();
    final Appenderator appenderator = Mockito.mock(Appenderator.class);
    final FakeRecordSupplier supplier = new FakeRecordSupplier(0, task);
    final StreamChunkReader chunkReader = Mockito.mock(StreamChunkReader.class);
    final ShareGroupIndexTaskRunner runner = new ShareGroupIndexTaskRunner(task, toolbox, mapper);

    final TaskStatus status = runner.runLoop(
        driver,
        appenderator,
        supplier,
        chunkReader,
        task.getIOConfig(),
        task.getTuningConfig(),
        toolbox
    );

    Assert.assertTrue(status.isSuccess());
    Assert.assertEquals(0, supplier.pollCallCount());
  }

  @Test
  public void testRunLoop_emptyPollThenStop_exitsCleanly() throws Exception
  {
    final StreamAppenderatorDriver driver = mockDriver();
    final Appenderator appenderator = Mockito.mock(Appenderator.class);
    final FakeRecordSupplier supplier = new FakeRecordSupplier(0, task);
    final StreamChunkReader chunkReader = Mockito.mock(StreamChunkReader.class);
    final ShareGroupIndexTaskRunner runner = new ShareGroupIndexTaskRunner(task, toolbox, mapper);

    final TaskStatus status = runner.runLoop(
        driver,
        appenderator,
        supplier,
        chunkReader,
        task.getIOConfig(),
        task.getTuningConfig(),
        toolbox
    );

    Assert.assertTrue(status.isSuccess());
    Assert.assertEquals(task.getId(), status.getId());
  }

  @Test
  public void testRunLoop_recordsProcessed_acknowledgesAndPublishes() throws Exception
  {
    final StreamAppenderatorDriver driver = mockDriver();
    final Appenderator appenderator = Mockito.mock(Appenderator.class);
    final RecordBearingFakeSupplier supplier = new RecordBearingFakeSupplier(task);
    final StreamChunkReader chunkReader = Mockito.mock(StreamChunkReader.class);
    final InputRow row = Mockito.mock(InputRow.class);
    Mockito.when(row.getTimestamp()).thenReturn(org.apache.druid.java.util.common.DateTimes.nowUtc());
    Mockito.when(chunkReader.parse(Mockito.any(), Mockito.anyBoolean()))
           .thenReturn(Collections.singletonList(row));
    final AppenderatorDriverAddResult addOk = AppenderatorDriverAddResult.ok(
        Mockito.mock(org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec.class),
        1,
        1L,
        false
    );
    Mockito.when(driver.add(Mockito.any(), Mockito.anyString(), Mockito.any(), Mockito.anyBoolean(), Mockito.anyBoolean()))
           .thenReturn(addOk);
    final ShareGroupIndexTaskRunner runner = new ShareGroupIndexTaskRunner(task, toolbox, mapper);

    final TaskStatus status = runner.runLoop(
        driver,
        appenderator,
        supplier,
        chunkReader,
        task.getIOConfig(),
        task.getTuningConfig(),
        toolbox
    );

    Assert.assertTrue(status.isSuccess());
    Assert.assertTrue(supplier.acknowledgedAtLeastOnce());
  }

  @Test
  public void testRunLoop_segmentAllocationFails_throwsAndExits() throws Exception
  {
    final StreamAppenderatorDriver driver = mockDriver();
    final Appenderator appenderator = Mockito.mock(Appenderator.class);
    final RecordBearingFakeSupplier supplier = new RecordBearingFakeSupplier(task);
    final StreamChunkReader chunkReader = Mockito.mock(StreamChunkReader.class);
    final InputRow row = Mockito.mock(InputRow.class);
    Mockito.when(row.getTimestamp()).thenReturn(org.apache.druid.java.util.common.DateTimes.nowUtc());
    Mockito.when(chunkReader.parse(Mockito.any(), Mockito.anyBoolean()))
           .thenReturn(Collections.singletonList(row));
    final AppenderatorDriverAddResult notOk = AppenderatorDriverAddResult.fail();
    Mockito.when(driver.add(Mockito.any(), Mockito.anyString(), Mockito.any(), Mockito.anyBoolean(), Mockito.anyBoolean()))
           .thenReturn(notOk);
    final ShareGroupIndexTaskRunner runner = new ShareGroupIndexTaskRunner(task, toolbox, mapper);

    try {
      runner.runLoop(
          driver,
          appenderator,
          supplier,
          chunkReader,
          task.getIOConfig(),
          task.getTuningConfig(),
          toolbox
      );
      Assert.fail("expected ISE for failed segment allocation");
    }
    catch (org.apache.druid.java.util.common.ISE expected) {
      Assert.assertTrue(expected.getMessage().contains("Could not allocate segment"));
    }
  }

  @Test
  public void testRunLoop_wakeupExceptionWhenStopping_breaksLoop() throws Exception
  {
    final StreamAppenderatorDriver driver = mockDriver();
    final Appenderator appenderator = Mockito.mock(Appenderator.class);
    final WakeupFakeRecordSupplier supplier = new WakeupFakeRecordSupplier(task);
    final StreamChunkReader chunkReader = Mockito.mock(StreamChunkReader.class);
    final ShareGroupIndexTaskRunner runner = new ShareGroupIndexTaskRunner(task, toolbox, mapper);

    final TaskStatus status = runner.runLoop(
        driver,
        appenderator,
        supplier,
        chunkReader,
        task.getIOConfig(),
        task.getTuningConfig(),
        toolbox
    );

    Assert.assertTrue(status.isSuccess());
  }

  private StreamAppenderatorDriver mockDriver()
  {
    final StreamAppenderatorDriver driver = Mockito.mock(StreamAppenderatorDriver.class);
    final SegmentsAndCommitMetadata published = Mockito.mock(SegmentsAndCommitMetadata.class);
    Mockito.when(published.getSegments()).thenReturn(Collections.emptyList());
    Mockito.when(driver.publish(
        Mockito.any(TransactionalSegmentPublisher.class),
        Mockito.any(),
        Mockito.anyList()
    )).thenReturn(com.google.common.util.concurrent.Futures.immediateFuture(published));
    return driver;
  }

  /**
   * Fake supplier that emits zero records then signals stop after the first poll attempt.
   */
  private static class FakeRecordSupplier
      implements AcknowledgingRecordSupplier<KafkaTopicPartition, Long, KafkaRecordEntity>
  {
    private final int batchesBeforeStop;
    private final ShareGroupIndexTask task;
    private final AtomicInteger pollCount = new AtomicInteger();

    FakeRecordSupplier(int batchesBeforeStop, ShareGroupIndexTask task)
    {
      this.batchesBeforeStop = batchesBeforeStop;
      this.task = task;
    }

    int pollCallCount()
    {
      return pollCount.get();
    }

    @Override
    public void subscribe(java.util.Set<String> topics)
    {
    }

    @Override
    public void unsubscribe()
    {
    }

    @Override
    public java.util.Set<String> subscription()
    {
      return Collections.emptySet();
    }

    @Override
    public List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> poll(
        long timeout
    )
    {
      final int n = pollCount.incrementAndGet();
      if (n > batchesBeforeStop) {
        task.stopGracefully(null);
      }
      return Collections.emptyList();
    }

    @Override
    public void acknowledge(KafkaTopicPartition partitionId, Long offset)
    {
    }

    @Override
    public void acknowledge(KafkaTopicPartition partitionId, Long offset, AcknowledgeType type)
    {
    }

    @Override
    public void acknowledge(
        Map<KafkaTopicPartition, java.util.Collection<Long>> offsets,
        AcknowledgeType type
    )
    {
    }

    @Override
    public Map<KafkaTopicPartition, Optional<Exception>> commitSync()
    {
      return Collections.emptyMap();
    }

    @Override
    public java.util.Set<KafkaTopicPartition> getPartitionIds(String stream)
    {
      return Collections.emptySet();
    }

    @Override
    public void close()
    {
    }
  }

  private static class RecordBearingFakeSupplier extends FakeRecordSupplier
  {
    private final java.util.concurrent.atomic.AtomicBoolean acked = new java.util.concurrent.atomic.AtomicBoolean();

    RecordBearingFakeSupplier(ShareGroupIndexTask task)
    {
      super(0, task);
    }

    boolean acknowledgedAtLeastOnce()
    {
      return acked.get();
    }

    @Override
    public List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> poll(
        long timeout
    )
    {
      // First poll returns one record; subsequent polls trigger stop.
      if (super.pollCallCount() == 0) {
        super.poll(timeout);
        final KafkaTopicPartition tp = new KafkaTopicPartition(false, "topic", 0);
        final KafkaRecordEntity entity = Mockito.mock(KafkaRecordEntity.class);
        return Collections.singletonList(
            new OrderedPartitionableRecord<>("topic", tp, 100L, Collections.singletonList(entity))
        );
      }
      return super.poll(timeout);
    }

    @Override
    public void acknowledge(
        KafkaTopicPartition partitionId,
        Long offset,
        AcknowledgeType type
    )
    {
      acked.set(true);
    }
  }

  /**
   * Fake supplier that throws WakeupException once stopGracefully has been called.
   */
  private static class WakeupFakeRecordSupplier extends FakeRecordSupplier
  {
    WakeupFakeRecordSupplier(ShareGroupIndexTask task)
    {
      super(0, task);
    }

    @Override
    public List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> poll(
        long timeout
    )
    {
      super.poll(timeout);
      throw new WakeupException();
    }
  }
}
