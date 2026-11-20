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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.SegmentTransactionalAppendAction;
import org.apache.druid.indexing.common.stats.TaskRealtimeMetricsMonitor;
import org.apache.druid.indexing.common.task.InputRowFilter;
import org.apache.druid.indexing.input.InputRowSchemas;
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTask;
import org.apache.druid.indexing.seekablestream.StreamChunkReader;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Stopwatch;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.SegmentSchemaMapping;
import org.apache.druid.segment.incremental.ParseExceptionHandler;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.realtime.SegmentGenerationMetrics;
import org.apache.druid.segment.realtime.appenderator.Appenderator;
import org.apache.druid.segment.realtime.appenderator.AppenderatorDriverAddResult;
import org.apache.druid.segment.realtime.appenderator.SegmentsAndCommitMetadata;
import org.apache.druid.segment.realtime.appenderator.StreamAppenderatorDriver;
import org.apache.druid.segment.realtime.appenderator.TransactionalSegmentPublisher;
import org.apache.druid.segment.realtime.sink.Committers;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import javax.annotation.Nullable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

final class ShareGroupIndexTaskRunner
{
  private static final Logger log = new Logger(ShareGroupIndexTaskRunner.class);
  private static final int DEFAULT_MAX_POLL_INTERVAL_MS = 300_000;
  private static final int DEFAULT_ACQUISITION_LOCK_MS = 30_000;
  private static final int MAX_PENDING_HANDOFFS = 8;
  private static final long HANDOFF_CHECK_INTERVAL_MS = 100L;

  private final ShareGroupIndexTask task;
  private final TaskToolbox toolbox;
  private final AtomicReference<ShareConsumer<byte[], byte[]>> activeConsumer = new AtomicReference<>();

  ShareGroupIndexTaskRunner(ShareGroupIndexTask task, TaskToolbox toolbox)
  {
    this.task = task;
    this.toolbox = toolbox;
  }

  TaskStatus run() throws Exception
  {
    final ShareGroupIndexTaskIOConfig ioConfig = task.getIOConfig();
    final Properties properties = ShareGroupConsumerProperties.resolve(ioConfig, task.getConfigMapper());
    final int maxPollIntervalMs = Integer.parseInt(properties.getProperty(
        ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,
        Integer.toString(DEFAULT_MAX_POLL_INTERVAL_MS)
    ).trim());
    if (ioConfig.getPollTimeout() >= maxPollIntervalMs) {
      throw InvalidInput.exception("pollTimeout must be less than max.poll.interval.ms [%d]", maxPollIntervalMs);
    }

    final DataSchema dataSchema = task.getDataSchema();
    final KafkaIndexTaskTuningConfig tuningConfig = task.getTuningConfig();
    final RowIngestionMeters meters = toolbox.getRowIngestionMetersFactory().createRowIngestionMeters();
    final ParseExceptionHandler parseExceptionHandler = new ParseExceptionHandler(
        meters,
        tuningConfig.isLogParseExceptions(),
        tuningConfig.getMaxParseExceptions(),
        tuningConfig.getMaxSavedParseExceptions()
    );
    final SegmentGenerationMetrics metrics = new SegmentGenerationMetrics();
    final StreamChunkReader<KafkaRecordEntity> chunkReader = new StreamChunkReader<>(
        ioConfig.getInputFormat(),
        InputRowSchemas.fromDataSchema(dataSchema),
        dataSchema.getTransformSpec(),
        toolbox.getIndexingTmpDir(),
        InputRowFilter.allowAll(),
        meters,
        parseExceptionHandler
    );
    final Appenderator appenderator = SeekableStreamIndexTask.newAppenderator(
        toolbox,
        task.getId(),
        dataSchema,
        tuningConfig,
        metrics,
        meters,
        parseExceptionHandler
    );
    final TaskRealtimeMetricsMonitor monitor = new TaskRealtimeMetricsMonitor(metrics, meters, task.getMetricBuilder());
    boolean completed = false;
    try (final StreamAppenderatorDriver driver = SeekableStreamIndexTask.newDriver(
        appenderator,
        toolbox,
        metrics,
        dataSchema,
        LockGranularity.TIME_CHUNK,
        TaskLockType.APPEND
    )) {
      toolbox.addMonitor(monitor);
      driver.startJob(null);
      if (!appenderator.getSegments().isEmpty()) {
        driver.clear();
      }
      try (final ShareConsumer<byte[], byte[]> consumer = createConsumer(properties)) {
        activeConsumer.set(consumer);
        try {
          runLoop(consumer, chunkReader, driver, maxPollIntervalMs);
        }
        catch (WakeupException e) {
          if (!task.isStopRequested()) {
            throw e;
          }
        }
        finally {
          activeConsumer.set(null);
        }
      }
      appenderator.close();
      completed = true;
      return TaskStatus.success(task.getId());
    }
    finally {
      if (!completed) {
        try {
          appenderator.closeNow();
        }
        catch (RuntimeException e) {
          log.warn(e, "Failed to close appenderator for task [%s].", task.getId());
        }
      }
      toolbox.removeMonitor(monitor);
    }
  }

  void requestWakeup()
  {
    final ShareConsumer<byte[], byte[]> consumer = activeConsumer.get();
    if (consumer != null) {
      consumer.wakeup();
    }
  }

  @VisibleForTesting
  void runLoop(
      ShareConsumer<byte[], byte[]> consumer,
      StreamChunkReader<KafkaRecordEntity> chunkReader,
      StreamAppenderatorDriver driver,
      int maxPollIntervalMs
  ) throws Exception
  {
    final ShareGroupIndexTaskIOConfig ioConfig = task.getIOConfig();
    final Duration pollTimeout = Duration.ofMillis(ioConfig.getPollTimeout());
    final Deque<ListenableFuture<SegmentsAndCommitMetadata>> handoffs = new ArrayDeque<>();
    final long handoffWarningMs = task.getTuningConfig().getHandoffConditionTimeout();
    final TransactionalSegmentPublisher publisher = new TransactionalSegmentPublisher()
    {
      @Override
      public SegmentPublishResult publishAnnotatedSegments(
          @Nullable Set<DataSegment> overwriteSegments,
          Set<DataSegment> segments,
          @Nullable Object commitMetadata,
          SegmentSchemaMapping segmentSchemaMapping
      ) throws IOException
      {
        return appendSegments(segments, segmentSchemaMapping);
      }
    };

    consumer.subscribe(Set.of(ioConfig.getTopic()));
    final Stopwatch batchTime = Stopwatch.createUnstarted();
    while (awaitHandoffCapacity(handoffs, handoffWarningMs)) {
      batchTime.restart();
      final ConsumerRecords<byte[], byte[]> records = consumer.poll(pollTimeout);
      if (records.isEmpty()) {
        continue;
      }
      final long batchBudgetMs = Math.min(
          maxPollIntervalMs,
          consumer.acquisitionLockTimeoutMs().orElse(DEFAULT_ACQUISITION_LOCK_MS)
      );
      final long publishBudgetMs = batchBudgetMs - Math.min(5_000L, batchBudgetMs / 5L);
      final String sequenceName = task.getId() + "-share-" + UUID.randomUUID();
      boolean addedRows = false;
      for (final ConsumerRecord<byte[], byte[]> record : records) {
        remainingMillis(batchTime, publishBudgetMs);
        final List<KafkaRecordEntity> input = record.value() == null
                                            ? List.of()
                                            : List.of(new KafkaRecordEntity(record));
        for (final InputRow row : chunkReader.parse(input, false)) {
          final AppenderatorDriverAddResult addResult = driver.add(
              row,
              sequenceName,
              Committers.nilSupplier(),
              true,
              true
          );
          if (!addResult.isOk()) {
            throw new ISE("Could not allocate a segment for Kafka share group [%s].", ioConfig.getGroupId());
          }
          addedRows = true;
        }
      }
      if (addedRows) {
        remainingMillis(batchTime, publishBudgetMs);
        final SegmentsAndCommitMetadata published = driver.publish(publisher, Committers.nil(), List.of(sequenceName))
                                                          .get(
                                                              remainingMillis(batchTime, publishBudgetMs),
                                                              TimeUnit.MILLISECONDS
                                                          );
        if (published == null || !published.wasPublished() || published.getSegments().isEmpty()) {
          throw new ISE("Publication was not confirmed for Kafka share group [%s].", ioConfig.getGroupId());
        }
        handoffs.addLast(driver.registerHandoff(published));
      }

      // ACCEPT can also be flushed by close(), so queue it only after publication.
      records.forEach(record -> consumer.acknowledge(record, AcknowledgeType.ACCEPT));
      final Map<TopicIdPartition, Optional<KafkaException>> results = consumer.commitSync(
          Duration.ofMillis(remainingMillis(batchTime, batchBudgetMs))
      );
      final Set<TopicPartition> unconfirmed = new HashSet<>(records.partitions());
      for (final Map.Entry<TopicIdPartition, Optional<KafkaException>> entry : results.entrySet()) {
        if (entry.getValue().isPresent()) {
          throw new ISE(entry.getValue().get(), "Kafka acknowledgement failed for partition [%s].", entry.getKey());
        }
        unconfirmed.remove(entry.getKey().topicPartition());
      }
      if (!unconfirmed.isEmpty()) {
        throw new ISE("Kafka acknowledgement was not confirmed for partitions [%s].", unconfirmed);
      }
      task.emitMetric(toolbox.getEmitter(), "ingest/kafka/shareGroup/acknowledged", records.count());
      task.emitMetric(toolbox.getEmitter(), "ingest/kafka/shareGroup/pollToAck/time", batchTime.millisElapsed());
    }
  }

  private boolean awaitHandoffCapacity(
      Deque<ListenableFuture<SegmentsAndCommitMetadata>> handoffs,
      long handoffWarningMs
  ) throws ExecutionException, InterruptedException
  {
    final Stopwatch waitTime = Stopwatch.createStarted();
    while (!task.isStopRequested()) {
      for (final Iterator<ListenableFuture<SegmentsAndCommitMetadata>> it = handoffs.iterator(); it.hasNext(); ) {
        final ListenableFuture<SegmentsAndCommitMetadata> handoff = it.next();
        if (handoff.isDone()) {
          handoff.get();
          it.remove();
        }
      }
      if (handoffs.size() < MAX_PENDING_HANDOFFS) {
        return !task.isStopRequested();
      }

      try {
        handoffs.getFirst().get(HANDOFF_CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS);
      }
      catch (TimeoutException e) {
        final long waitedMs = waitTime.millisElapsed();
        if (handoffWarningMs > 0 && waitTime.isRunning() && waitedMs >= handoffWarningMs) {
          log.warn(
              "Task [%s] has waited [%d] ms for segment handoff at the pending batch limit [%d].",
              task.getId(),
              waitedMs,
              MAX_PENDING_HANDOFFS
          );
          waitTime.stop();
        }
      }
    }
    return false;
  }

  private SegmentPublishResult appendSegments(Set<DataSegment> segments, SegmentSchemaMapping segmentSchemaMapping)
      throws IOException
  {
    final SegmentPublishResult result = toolbox.getTaskActionClient().submit(
        SegmentTransactionalAppendAction.forSegments(segments, segmentSchemaMapping)
    );
    if (result.isSuccess()) {
      final Map<SegmentId, DataSegment> confirmed = new HashMap<>();
      for (final DataSegment segment : result.getSegments()) {
        confirmed.put(segment.getId(), segment);
      }
      for (final DataSegment segment : segments) {
        final DataSegment actual = confirmed.get(segment.getId());
        if (actual == null || !Objects.equals(actual.getLoadSpec(), segment.getLoadSpec())) {
          throw new ISE("Publication did not confirm segment [%s] for task [%s].", segment.getId(), task.getId());
        }
      }
    }
    return result;
  }

  private static long remainingMillis(Stopwatch elapsed, long budgetMs) throws TimeoutException
  {
    final long remaining = budgetMs - elapsed.millisElapsed();
    if (remaining <= 0) {
      throw new TimeoutException("Kafka share group batch exceeded its time budget of " + budgetMs + " ms");
    }
    return remaining;
  }

  private static ShareConsumer<byte[], byte[]> createConsumer(Properties properties)
  {
    final Thread thread = Thread.currentThread();
    final ClassLoader originalClassLoader = thread.getContextClassLoader();
    try {
      thread.setContextClassLoader(ShareGroupIndexTaskRunner.class.getClassLoader());
      return new KafkaShareConsumer<>(properties, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }
    finally {
      thread.setContextClassLoader(originalClassLoader);
    }
  }
}
