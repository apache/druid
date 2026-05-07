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
import org.apache.druid.data.input.Committer;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.data.input.kafka.KafkaTopicPartition;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexing.appenderator.ActionBasedPublishedSegmentRetriever;
import org.apache.druid.indexing.appenderator.ActionBasedSegmentAllocator;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskLocks;
import org.apache.druid.indexing.common.actions.LockReleaseAction;
import org.apache.druid.indexing.common.actions.SegmentAllocateAction;
import org.apache.druid.indexing.common.actions.SegmentTransactionalAppendAction;
import org.apache.druid.indexing.common.task.InputRowFilter;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.input.InputRowSchemas;
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.indexing.seekablestream.SeekableStreamAppenderatorConfig;
import org.apache.druid.indexing.seekablestream.StreamChunkReader;
import org.apache.druid.indexing.seekablestream.common.AcknowledgeType;
import org.apache.druid.indexing.seekablestream.common.AcknowledgingRecordSupplier;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.common.config.Configs;
import org.apache.druid.java.util.common.ISE;
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
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NumberedPartialShardSpec;

import org.apache.kafka.common.errors.WakeupException;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;

/**
 * Ingestion loop for {@link ShareGroupIndexTask}.
 *
 * <p><b>Phase 1 invariants:</b>
 * <ol>
 *   <li><b>ACK after publish (data safety):</b> records are acknowledged with
 *       {@code ACCEPT} only after the segment containing them is atomically
 *       registered in the metadata store via
 *       {@link SegmentTransactionalAppendAction}. Any failure between poll and
 *       publish leaves the records unacknowledged and the broker will
 *       redeliver them after the acquisition lock expires.</li>
 *   <li><b>Multi-row safe:</b> records are parsed via
 *       {@link StreamChunkReader} which returns 0..N rows per record (handles
 *       JSON arrays, null/empty records, and ParseException routing). Every
 *       row produced from a record is added to the appenderator before the
 *       record is acknowledged.</li>
 *   <li><b>Resource safe:</b> {@code Appenderator} and {@code RecordSupplier}
 *       are released on every exit path, including
 *       {@link org.apache.kafka.common.errors.WakeupException} during graceful
 *       stop and any in-loop ISE.</li>
 *   <li><b>Graceful stop:</b> {@link ShareGroupIndexTask#stopGracefully} calls
 *       {@link #requestWakeup()} which forwards to
 *       {@link AcknowledgingRecordSupplier#wakeup()}, interrupting an
 *       in-flight {@code poll()} so the loop exits at the next iteration.</li>
 *   <li><b>Observability:</b> per-partition commit failures are emitted as
 *       {@code ingest/shareGroup/commitFailures}; broker-effective acquisition
 *       lock duration is logged once after the first poll.</li>
 * </ol>
 *
 * <p><b>Single-threaded:</b> all {@code Appenderator} interactions happen on
 * the run thread. Background lock-renewal is a Phase 2 enhancement (see
 * {@link AcknowledgeType#RENEW}).</p>
 *
 * <p><b>Testability (DIP):</b> the supplier factory is constructor-injected
 * so unit tests can plug in a mock {@link AcknowledgingRecordSupplier} without
 * spinning up a Kafka broker.</p>
 */
public class ShareGroupIndexTaskRunner
{
  private static final Logger log = new Logger(ShareGroupIndexTaskRunner.class);
  private static final String SEQUENCE_NAME = "share_group_seq_0";

  /**
   * Metric for per-partition acknowledgement commit failures during share-group
   * ingestion. Emitted with the standard task dimensions (datasource, taskId,
   * etc.) so operators can alert on sustained broker commit issues. A non-zero
   * value implies the corresponding records will be redelivered by the broker.
   */
  static final String METRIC_COMMIT_FAILURES = "ingest/shareGroup/commitFailures";

  private final ShareGroupIndexTask task;
  private final TaskToolbox toolbox;
  private final ObjectMapper configMapper;
  private final Function<ShareGroupIndexTaskIOConfig,
      AcknowledgingRecordSupplier<KafkaTopicPartition, Long, KafkaRecordEntity>> supplierFactory;

  /**
   * Active supplier reference used by {@link #requestWakeup()} so the task's
   * {@code stopGracefully} hook can interrupt a blocking poll. Set on entry
   * to the run loop and cleared on exit.
   */
  private final AtomicReference<AcknowledgingRecordSupplier<KafkaTopicPartition, Long, KafkaRecordEntity>>
      activeSupplier = new AtomicReference<>();

  ShareGroupIndexTaskRunner(
      ShareGroupIndexTask task,
      TaskToolbox toolbox,
      ObjectMapper configMapper
  )
  {
    this(task, toolbox, configMapper, null);
  }

  /**
   * DIP-friendly constructor: accepts a factory that builds the
   * {@link AcknowledgingRecordSupplier} from the IO config. Tests can pass a
   * factory returning a mock supplier; production passes {@code null} and the
   * runner falls back to building a {@link KafkaShareGroupRecordSupplier}.
   */
  @VisibleForTesting
  ShareGroupIndexTaskRunner(
      ShareGroupIndexTask task,
      TaskToolbox toolbox,
      ObjectMapper configMapper,
      @Nullable Function<ShareGroupIndexTaskIOConfig,
          AcknowledgingRecordSupplier<KafkaTopicPartition, Long, KafkaRecordEntity>> supplierFactory
  )
  {
    this.task = task;
    this.toolbox = toolbox;
    this.configMapper = configMapper;
    this.supplierFactory = supplierFactory != null ? supplierFactory : this::createDefaultRecordSupplier;
  }

  public TaskStatus run() throws Exception
  {
    final DataSchema dataSchema = task.getDataSchema();
    final KafkaIndexTaskTuningConfig tuningConfig = task.getTuningConfig();
    final ShareGroupIndexTaskIOConfig ioConfig = task.getIOConfig();

    final RowIngestionMeters rowIngestionMeters =
        toolbox.getRowIngestionMetersFactory().createRowIngestionMeters();
    final ParseExceptionHandler parseExceptionHandler = new ParseExceptionHandler(
        rowIngestionMeters,
        tuningConfig.isLogParseExceptions(),
        tuningConfig.getMaxParseExceptions(),
        tuningConfig.getMaxSavedParseExceptions()
    );
    final SegmentGenerationMetrics segmentGenerationMetrics = new SegmentGenerationMetrics();

    final InputRowSchema inputRowSchema = InputRowSchemas.fromDataSchema(dataSchema);
    final InputFormat inputFormat = ioConfig.getInputFormat();
    if (inputFormat == null) {
      throw new ISE("inputFormat must be specified in ioConfig");
    }

    // Single canonical multi-row parser, shared with the seekable-stream runner.
    // Handles null/empty records, multi-row records (e.g. JSON arrays), parse
    // exceptions (via ParseExceptionHandler), and processedBytes metrics.
    //
    // Raw type matches the SeekableStreamIndexTaskRunner idiom: works around
    // OrderedPartitionableRecord.getData() returning List<? extends ByteEntity>
    // (rather than List<? extends RecordType>); changing that signature would
    // be a much larger refactor for marginal benefit.
    @SuppressWarnings({"rawtypes", "unchecked"})
    final StreamChunkReader chunkReader = new StreamChunkReader<KafkaRecordEntity>(
        inputFormat,
        inputRowSchema,
        dataSchema.getTransformSpec(),
        toolbox.getIndexingTmpDir(),
        InputRowFilter.allowAll(),
        rowIngestionMeters,
        parseExceptionHandler
    );

    final LockGranularity lockGranularity = Configs.valueOrDefault(
        task.getContextValue(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, Tasks.DEFAULT_FORCE_TIME_CHUNK_LOCK),
        Tasks.DEFAULT_FORCE_TIME_CHUNK_LOCK
    )
        ? LockGranularity.TIME_CHUNK
        : LockGranularity.SEGMENT;
    final TaskLockType lockType = TaskLocks.determineLockTypeForAppend(task.getContext());

    // Create appenderator and driver
    final Appenderator appenderator = toolbox.getAppenderatorsManager().createRealtimeAppenderatorForTask(
        toolbox.getSegmentLoaderConfig(),
        task.getId(),
        dataSchema,
        SeekableStreamAppenderatorConfig.fromTuningConfig(
            tuningConfig.withBasePersistDirectory(toolbox.getPersistDir()),
            toolbox.getProcessingConfig()
        ),
        toolbox.getConfig(),
        segmentGenerationMetrics,
        toolbox.getSegmentPusher(),
        toolbox.getJsonMapper(),
        toolbox.getIndexIO(),
        toolbox.getIndexMerger(),
        toolbox.getQueryRunnerFactoryConglomerate(),
        toolbox.getSegmentAnnouncer(),
        toolbox.getEmitter(),
        toolbox.getQueryProcessingPool(),
        toolbox.getJoinableFactory(),
        toolbox.getCache(),
        toolbox.getCacheConfig(),
        toolbox.getCachePopulatorStats(),
        toolbox.getPolicyEnforcer(),
        rowIngestionMeters,
        parseExceptionHandler,
        toolbox.getCentralizedTableSchemaConfig(),
        interval -> {
          toolbox.getTaskActionClient().submit(new LockReleaseAction(interval));
        }
    );

    final StreamAppenderatorDriver driver = new StreamAppenderatorDriver(
        appenderator,
        new ActionBasedSegmentAllocator(
            toolbox.getTaskActionClient(),
            dataSchema,
            (schema, row, sequenceName, previousSegmentId, skipSegmentLineageCheck) -> new SegmentAllocateAction(
                schema.getDataSource(),
                row.getTimestamp(),
                schema.getGranularitySpec().getQueryGranularity(),
                schema.getGranularitySpec().getSegmentGranularity(),
                sequenceName,
                previousSegmentId,
                skipSegmentLineageCheck,
                NumberedPartialShardSpec.instance(),
                lockGranularity,
                lockType
            )
        ),
        toolbox.getSegmentHandoffNotifierFactory(),
        new ActionBasedPublishedSegmentRetriever(toolbox.getTaskActionClient()),
        toolbox.getDataSegmentKiller(),
        toolbox.getJsonMapper(),
        segmentGenerationMetrics
    );

    boolean appenderatorClosedNormally = false;
    try (final AcknowledgingRecordSupplier<KafkaTopicPartition, Long, KafkaRecordEntity> recordSupplier =
             supplierFactory.apply(ioConfig)) {
      activeSupplier.set(recordSupplier);
      recordSupplier.subscribe(Collections.singleton(ioConfig.getTopic()));

      driver.startJob(segmentId -> true);

      final Supplier<Committer> committerSupplier = () -> new Committer()
      {
        @Override
        public Object getMetadata()
        {
          return ImmutableMap.of("type", "share_group");
        }

        @Override
        public void run()
        {
          // no-op: share group does not need client-side offset persistence
        }
      };

      log.info("Starting share group ingestion loop for topic[%s], group[%s].",
               ioConfig.getTopic(), ioConfig.getGroupId());

      long totalRowsIngested = 0;
      boolean lockTimeoutLogged = false;

      while (!task.isStopRequested()) {

        final List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> records;
        try {
          records = recordSupplier.poll(ioConfig.getPollTimeout());
        }
        catch (WakeupException e) {
          if (task.isStopRequested()) {
            log.info("Wake-up received while stopGracefully is in progress; exiting ingestion loop.");
            break;
          }
          throw e;
        }

        if (!lockTimeoutLogged) {
          final Optional<Integer> effectiveLockMs = recordSupplier.acquisitionLockTimeoutMs();
          log.info(
              "Effective broker acquisition lock timeout for share-group[%s]: %s ms",
              ioConfig.getGroupId(),
              effectiveLockMs.map(String::valueOf).orElse("<unknown>")
          );
          lockTimeoutLogged = true;
        }

        if (records.isEmpty()) {
          continue;
        }

        // Track offsets of records in this batch for acknowledgement after publish
        final Map<KafkaTopicPartition, List<Long>> batchOffsets = new HashMap<>();
        boolean midBatchPersistNeeded = false;
        boolean pushThresholdLogged = false;

        for (OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> record : records) {
          batchOffsets.computeIfAbsent(record.getPartitionId(), k -> new ArrayList<>())
                      .add(record.getSequenceNumber());

          // Multi-row safe: chunkReader returns 0..N rows per record, increments
          // processedBytes/thrownAway/unparseable internally, and routes parse
          // exceptions through parseExceptionHandler (so maxParseExceptions/etc.
          // are honored). Null record.data() is handled inside parse().
          final List<InputRow> rows = chunkReader.parse(record.getData(), false);

          for (InputRow row : rows) {
            final AppenderatorDriverAddResult addResult = driver.add(
                row,
                SEQUENCE_NAME,
                committerSupplier,
                true,
                false
            );

            if (!addResult.isOk()) {
              // Failure to allocate segment puts data integrity at risk: bail out
              // so the records remain unacknowledged and the broker redelivers.
              throw new ISE(
                  "Could not allocate segment for row with timestamp[%s]",
                  row.getTimestamp()
              );
            }

            totalRowsIngested++;
            midBatchPersistNeeded |= addResult.isPersistRequired();

            if (!pushThresholdLogged && addResult.isPushRequired(
                tuningConfig.getPartitionsSpec().getMaxRowsPerSegment(),
                tuningConfig.getPartitionsSpec()
                            .getMaxTotalRowsOr(DynamicPartitionsSpec.DEFAULT_MAX_TOTAL_ROWS)
            )) {
              pushThresholdLogged = true;
              log.info(
                  "Segment row threshold reached during batch (current segment[%s], rows[%d]); "
                  + "will publish at end-of-batch boundary.",
                  addResult.getSegmentIdentifier(),
                  addResult.getNumRowsInSegment()
              );
            }
          }
        }

        // Mid-batch persist if memory pressure dictates (max{Bytes,Rows}InMemory).
        if (midBatchPersistNeeded) {
          driver.persist(committerSupplier.get());
        }

        // End-of-batch persist before publish
        driver.persist(committerSupplier.get());

        // Publish all segments for this sequence
        final TransactionalSegmentPublisher publisher = new TransactionalSegmentPublisher()
        {
          @Override
          public SegmentPublishResult publishAnnotatedSegments(
              @Nullable Set<DataSegment> mustBeNullOrEmptyOverwriteSegments,
              Set<DataSegment> segmentsToPush,
              @Nullable Object commitMetadata,
              SegmentSchemaMapping segmentSchemaMapping
          ) throws IOException
          {
            if (segmentsToPush.isEmpty()) {
              return SegmentPublishResult.ok(segmentsToPush);
            }
            return toolbox.getTaskActionClient().submit(
                SegmentTransactionalAppendAction.forSegments(segmentsToPush, segmentSchemaMapping)
            );
          }

          @Override
          public boolean supportsEmptyPublish()
          {
            return false;
          }
        };

        final SegmentsAndCommitMetadata published = driver.publish(
            publisher,
            committerSupplier.get(),
            Collections.singletonList(SEQUENCE_NAME)
        ).get();

        final int segmentCount = published.getSegments().size();
        if (segmentCount > 0) {
          log.info("Published %d segments with %d total rows.", segmentCount, totalRowsIngested);
        }

        // INVARIANT 1: ACK only after successful publish
        for (Map.Entry<KafkaTopicPartition, List<Long>> entry : batchOffsets.entrySet()) {
          for (Long offset : entry.getValue()) {
            recordSupplier.acknowledge(entry.getKey(), offset, AcknowledgeType.ACCEPT);
          }
        }

        // Commit acknowledgements to the broker. Failures here mean the broker
        // will redeliver the affected records (at-least-once); we count and emit
        // a metric so operators can alert on sustained commit issues.
        final Map<KafkaTopicPartition, Optional<Exception>> commitResult = recordSupplier.commitSync();
        long commitFailures = 0L;
        for (Map.Entry<KafkaTopicPartition, Optional<Exception>> entry : commitResult.entrySet()) {
          if (entry.getValue().isPresent()) {
            commitFailures++;
            log.warn(
                entry.getValue().get(),
                "Commit failed for partition[%s]. Records may be redelivered.",
                entry.getKey()
            );
          }
        }
        if (commitFailures > 0) {
          task.emitMetric(toolbox.getEmitter(), METRIC_COMMIT_FAILURES, commitFailures);
        }
      }

      // Final persist and graceful close
      driver.persist(committerSupplier.get());

      log.info("Share group ingestion complete. Total rows ingested: %d", totalRowsIngested);
      appenderator.close();
      appenderatorClosedNormally = true;
    }
    finally {
      activeSupplier.set(null);
      try {
        driver.close();
      }
      catch (Exception e) {
        log.warn(e, "Exception closing StreamAppenderatorDriver; continuing teardown.");
      }
      // Guarantees the appenderator is released on every exception path
      // (incl. WakeupException, ISE during add, persist/publish failures).
      // closeNow() is the immediate variant; safe to call after a successful
      // close() since Appenderator implementations make close idempotent.
      if (!appenderatorClosedNormally) {
        try {
          appenderator.closeNow();
        }
        catch (Exception e) {
          log.warn(e, "Exception during emergency closeNow() of Appenderator; continuing teardown.");
        }
      }
    }

    return TaskStatus.success(task.getId());
  }

  /**
   * Wake up an in-flight {@link AcknowledgingRecordSupplier#poll(long)} call.
   * Called by {@link ShareGroupIndexTask#stopGracefully(org.apache.druid.indexing.common.config.TaskConfig)}.
   * Safe to call from any thread; no-op if the loop is not currently running.
   */
  void requestWakeup()
  {
    final AcknowledgingRecordSupplier<KafkaTopicPartition, Long, KafkaRecordEntity> supplier = activeSupplier.get();
    if (supplier == null) {
      return;
    }
    try {
      supplier.wakeup();
    }
    catch (Exception e) {
      log.warn(e, "Exception calling wakeup() on the active record supplier; ignoring.");
    }
  }

  private AcknowledgingRecordSupplier<KafkaTopicPartition, Long, KafkaRecordEntity> createDefaultRecordSupplier(
      ShareGroupIndexTaskIOConfig ioConfig
  )
  {
    final ClassLoader currCtxCl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
      return new KafkaShareGroupRecordSupplier(
          ioConfig.getConsumerProperties(),
          configMapper,
          ioConfig.getGroupId()
      );
    }
    finally {
      Thread.currentThread().setContextClassLoader(currCtxCl);
    }
  }

}
