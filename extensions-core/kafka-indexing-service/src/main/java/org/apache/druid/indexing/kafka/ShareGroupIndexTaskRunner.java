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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.Configs;
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
import org.apache.druid.indexing.common.actions.LockReleaseAction;
import org.apache.druid.indexing.common.actions.SegmentAllocateAction;
import org.apache.druid.indexing.common.actions.SegmentTransactionalAppendAction;
import org.apache.druid.indexing.common.actions.TaskLocks;
import org.apache.druid.indexing.common.task.InputRowFilter;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.input.InputRowSchemas;
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.indexing.seekablestream.SeekableStreamAppenderatorConfig;
import org.apache.druid.indexing.seekablestream.StreamChunkReader;
import org.apache.druid.indexing.seekablestream.common.AcknowledgeType;
import org.apache.druid.indexing.seekablestream.common.AcknowledgingRecordSupplier;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
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

/**
 * Single-threaded ingestion loop for {@link ShareGroupIndexTask}. Records are
 * {@code ACCEPT}-acknowledged only after the containing segment is published;
 * any earlier failure leaves them unacknowledged so the broker redelivers
 * them once the acquisition lock expires.
 */
public class ShareGroupIndexTaskRunner
{
  private static final Logger log = new Logger(ShareGroupIndexTaskRunner.class);
  private static final String SEQUENCE_NAME = "share_group_seq_0";

  static final String METRIC_COMMIT_FAILURES = "ingest/shareGroup/commitFailures";

  private final ShareGroupIndexTask task;
  private final TaskToolbox toolbox;
  private final ObjectMapper configMapper;
  private final Function<ShareGroupIndexTaskIOConfig,
      AcknowledgingRecordSupplier<KafkaTopicPartition, Long, KafkaRecordEntity>> supplierFactory;

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

    // Raw type mirrors SeekableStreamIndexTaskRunner; works around
    // OrderedPartitionableRecord.getData() returning a wildcard list.
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

    final org.apache.druid.indexing.common.stats.TaskRealtimeMetricsMonitor metricsMonitor =
        new org.apache.druid.indexing.common.stats.TaskRealtimeMetricsMonitor(
            segmentGenerationMetrics,
            rowIngestionMeters,
            task.getMetricBuilder()
        );
    toolbox.addMonitor(metricsMonitor);

    boolean runLoopCompleted = false;
    try (final AcknowledgingRecordSupplier<KafkaTopicPartition, Long, KafkaRecordEntity> recordSupplier =
             supplierFactory.apply(ioConfig)) {
      activeSupplier.set(recordSupplier);
      final TaskStatus status = runLoop(driver, appenderator, recordSupplier, chunkReader, ioConfig, tuningConfig, toolbox);
      runLoopCompleted = true;
      return status;
    }
    finally {
      activeSupplier.set(null);
      if (!runLoopCompleted) {
        try {
          appenderator.closeNow();
        }
        catch (Exception e) {
          log.warn(e, "Exception during emergency closeNow() of Appenderator in run(); continuing teardown.");
        }
      }
      try {
        toolbox.removeMonitor(metricsMonitor);
      }
      catch (Exception e) {
        log.warn(e, "Exception removing TaskRealtimeMetricsMonitor; continuing teardown.");
      }
      try {
        driver.close();
      }
      catch (Exception e) {
        log.warn(e, "Exception closing StreamAppenderatorDriver; continuing teardown.");
      }
    }
  }

  @VisibleForTesting
  TaskStatus runLoop(
      StreamAppenderatorDriver driver,
      Appenderator appenderator,
      AcknowledgingRecordSupplier<KafkaTopicPartition, Long, KafkaRecordEntity> recordSupplier,
      StreamChunkReader chunkReader,
      ShareGroupIndexTaskIOConfig ioConfig,
      KafkaIndexTaskTuningConfig tuningConfig,
      TaskToolbox toolbox
  ) throws Exception
  {
    // Share groups manage delivery state on the broker; the Committer is a
    // no-op placeholder required by the appenderator driver contract.
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
      }
    };

    log.info("Starting share group ingestion loop for topic[%s], group[%s].",
             ioConfig.getTopic(), ioConfig.getGroupId());

    long totalRowsIngested = 0;
    boolean lockTimeoutLogged = false;
    boolean appenderatorClosedNormally = false;

    try {
      recordSupplier.subscribe(Collections.singleton(ioConfig.getTopic()));
      driver.startJob(segmentId -> true);
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

        final Map<KafkaTopicPartition, List<Long>> batchOffsets = new HashMap<>();
        boolean midBatchPersistNeeded = false;
        boolean pushThresholdLogged = false;

        for (OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> record : records) {
          batchOffsets.computeIfAbsent(record.getPartitionId(), k -> new ArrayList<>())
                      .add(record.getSequenceNumber());

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
              // Throw without acknowledging so the broker redelivers this batch.
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

        if (midBatchPersistNeeded) {
          driver.persist(committerSupplier.get());
        }

        driver.persist(committerSupplier.get());

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

        // Acknowledge only after the segment is published; any earlier failure
        // leaves records unacknowledged so the broker redelivers them.
        for (Map.Entry<KafkaTopicPartition, List<Long>> entry : batchOffsets.entrySet()) {
          for (Long offset : entry.getValue()) {
            recordSupplier.acknowledge(entry.getKey(), offset, AcknowledgeType.ACCEPT);
          }
        }

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

      driver.persist(committerSupplier.get());
      log.info("Share group ingestion complete. Total rows ingested: %d", totalRowsIngested);
      appenderator.close();
      appenderatorClosedNormally = true;
    }
    finally {
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

  /** Interrupts an in-flight poll so the loop can exit on graceful stop. */
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
