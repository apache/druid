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
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.data.input.kafka.KafkaTopicPartition;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.appenderator.ActionBasedPublishedSegmentRetriever;
import org.apache.druid.indexing.appenderator.ActionBasedSegmentAllocator;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskLocks;
import org.apache.druid.indexing.common.actions.LockReleaseAction;
import org.apache.druid.indexing.common.actions.SegmentAllocateAction;
import org.apache.druid.indexing.common.actions.SegmentTransactionalAppendAction;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.input.InputRowSchemas;
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.indexing.seekablestream.SeekableStreamAppenderatorConfig;
import org.apache.druid.indexing.seekablestream.common.AcknowledgeType;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.common.config.Configs;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.ParseException;
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

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import com.google.common.base.Supplier;

/**
 * Ingestion loop for {@link ShareGroupIndexTask}.
 *
 * Phase 1: single-threaded. Polls from KafkaShareConsumer, parses rows,
 * adds to appenderator, persists, publishes segments, then ACKs records
 * and commits. Records are only acknowledged AFTER segment publication
 * succeeds (INVARIANT 1).
 *
 * On failure, unacknowledged records are redelivered by the broker.
 */
public class ShareGroupIndexTaskRunner
{
  private static final Logger log = new Logger(ShareGroupIndexTaskRunner.class);
  private static final String SEQUENCE_NAME = "share_group_seq_0";

  private final ShareGroupIndexTask task;
  private final TaskToolbox toolbox;
  private final ObjectMapper configMapper;

  ShareGroupIndexTaskRunner(
      ShareGroupIndexTask task,
      TaskToolbox toolbox,
      ObjectMapper configMapper
  )
  {
    this.task = task;
    this.toolbox = toolbox;
    this.configMapper = configMapper;
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

    try (final KafkaShareGroupRecordSupplier recordSupplier = createRecordSupplier(ioConfig)) {
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

      while (!task.isStopRequested()) {

        final List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> records =
            recordSupplier.poll(ioConfig.getPollTimeout());

        if (records.isEmpty()) {
          continue;
        }

        // Track offsets of records in this batch for acknowledgement after publish
        final Map<KafkaTopicPartition, List<Long>> batchOffsets = new HashMap<>();

        for (OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity> record : records) {
          batchOffsets.computeIfAbsent(record.getPartitionId(), k -> new ArrayList<>())
                      .add(record.getSequenceNumber());

          if (record.getData() == null) {
            continue;
          }

          for (ByteEntity entity : record.getData()) {
            try {
              final InputRow row = parseRow(entity, inputFormat, inputRowSchema, toolbox);

              final AppenderatorDriverAddResult addResult = driver.add(
                  row,
                  SEQUENCE_NAME,
                  committerSupplier,
                  true,
                  false
              );

              if (addResult.isOk()) {
                totalRowsIngested++;
                rowIngestionMeters.incrementProcessed();
              } else {
                throw new ISE("Could not allocate segment for row with timestamp[%s]", row.getTimestamp());
              }
            }
            catch (ParseException e) {
              parseExceptionHandler.handle(e);
            }
          }
        }

        // Persist before publish
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

        // Commit acknowledgements to the broker
        final Map<KafkaTopicPartition, Optional<Exception>> commitResult = recordSupplier.commitSync();
        for (Map.Entry<KafkaTopicPartition, Optional<Exception>> entry : commitResult.entrySet()) {
          if (entry.getValue().isPresent()) {
            log.warn(
                entry.getValue().get(),
                "Commit failed for partition[%s]. Records may be redelivered.",
                entry.getKey()
            );
          }
        }
      }

      // Final persist and close
      driver.persist(committerSupplier.get());

      log.info("Share group ingestion complete. Total rows ingested: %d", totalRowsIngested);
      appenderator.close();
    }
    finally {
      driver.close();
    }

    return TaskStatus.success(task.getId());
  }

  private KafkaShareGroupRecordSupplier createRecordSupplier(ShareGroupIndexTaskIOConfig ioConfig)
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

  private static InputRow parseRow(
      ByteEntity entity,
      InputFormat inputFormat,
      InputRowSchema inputRowSchema,
      TaskToolbox toolbox
  ) throws IOException
  {
    try (org.apache.druid.java.util.common.parsers.CloseableIterator<InputRow> iterator =
             inputFormat.createReader(inputRowSchema, entity, toolbox.getIndexingTmpDir()).read()) {
      return iterator.hasNext() ? iterator.next() : null;
    }
  }
}
