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
import org.apache.druid.common.config.Configs;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.data.input.kafka.KafkaTopicPartition;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.SegmentLockAcquireAction;
import org.apache.druid.indexing.common.actions.TaskLocks;
import org.apache.druid.indexing.common.actions.TimeChunkLockAcquireAction;
import org.apache.druid.indexing.common.task.InputRowFilter;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.input.InputRowSchemas;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTask;
import org.apache.druid.indexing.seekablestream.StreamChunkReader;
import org.apache.druid.indexing.seekablestream.common.AcknowledgingRecordSupplier;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.incremental.ParseExceptionHandler;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.realtime.SegmentGenerationMetrics;
import org.apache.druid.segment.realtime.appenderator.Appenderator;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.segment.realtime.appenderator.StreamAppenderatorDriver;
import org.apache.druid.storage.StorageConnector;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import javax.annotation.Nullable;
import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Runs durable acquisition and inbox processing for {@link ShareGroupIndexTask}.
 */
public class ShareGroupIndexTaskRunner
{
  private static final Logger log = new Logger(ShareGroupIndexTaskRunner.class);
  private static final long DEFAULT_MAX_POLL_INTERVAL_MS = 300_000L;

  private final ShareGroupIndexTask task;
  private final TaskToolbox toolbox;
  private final ObjectMapper configMapper;
  private final LockGranularity lockGranularity;
  private final TaskLockType lockType;
  private final Function<ShareGroupIndexTaskIOConfig,
      AcknowledgingRecordSupplier<KafkaTopicPartition, Long, KafkaRecordEntity>> supplierFactory;
  private final Function<ShareGroupIndexTaskIOConfig, ShareGroupSourceIdentity> sourceIdentityFactory;

  @Nullable
  private ShareGroupSourceIdentity sourceIdentity;
  @Nullable
  private String specFingerprint;

  private final AtomicReference<AcknowledgingRecordSupplier<KafkaTopicPartition, Long, KafkaRecordEntity>>
      activeSupplier = new AtomicReference<>();
  private final AtomicReference<ShareGroupAcquisitionLoop> activeAcquisitionLoop = new AtomicReference<>();
  private final AtomicReference<ShareInboxProcessor> activeInboxProcessor = new AtomicReference<>();

  ShareGroupIndexTaskRunner(
      ShareGroupIndexTask task,
      TaskToolbox toolbox,
      ObjectMapper configMapper
  )
  {
    this(task, toolbox, configMapper, null, null);
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
    this(task, toolbox, configMapper, supplierFactory, null);
  }

  @VisibleForTesting
  ShareGroupIndexTaskRunner(
      ShareGroupIndexTask task,
      TaskToolbox toolbox,
      ObjectMapper configMapper,
      @Nullable Function<ShareGroupIndexTaskIOConfig,
          AcknowledgingRecordSupplier<KafkaTopicPartition, Long, KafkaRecordEntity>> supplierFactory,
      @Nullable Function<ShareGroupIndexTaskIOConfig, ShareGroupSourceIdentity> sourceIdentityFactory
  )
  {
    this.task = task;
    this.toolbox = toolbox;
    this.configMapper = configMapper;
    final boolean useTimeChunkLocks = Configs.valueOrDefault(
        task.getContextValue(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, Tasks.DEFAULT_FORCE_TIME_CHUNK_LOCK),
        Tasks.DEFAULT_FORCE_TIME_CHUNK_LOCK
    );
    this.lockGranularity = useTimeChunkLocks ? LockGranularity.TIME_CHUNK : LockGranularity.SEGMENT;
    this.lockType = TaskLocks.determineLockTypeForAppend(task.getContext());
    this.supplierFactory = supplierFactory != null ? supplierFactory : this::createDefaultRecordSupplier;
    this.sourceIdentityFactory = sourceIdentityFactory != null ? sourceIdentityFactory : this::resolveSourceIdentity;
  }

  public TaskStatus run() throws Exception
  {
    final DataSchema dataSchema = task.getDataSchema();
    final KafkaIndexTaskTuningConfig tuningConfig = task.getTuningConfig();
    final ShareGroupIndexTaskIOConfig ioConfig = task.getIOConfig();
    initializeInboxContext(ioConfig);

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

    final Appenderator appenderator = SeekableStreamIndexTask.newAppenderator(
        toolbox,
        task.getId(),
        dataSchema,
        tuningConfig,
        segmentGenerationMetrics,
        rowIngestionMeters,
        parseExceptionHandler
    );

    final StreamAppenderatorDriver driver = SeekableStreamIndexTask.newDriver(
        appenderator,
        toolbox,
        segmentGenerationMetrics,
        dataSchema,
        lockGranularity,
        lockType
    );

    final org.apache.druid.indexing.common.stats.TaskRealtimeMetricsMonitor metricsMonitor =
        new org.apache.druid.indexing.common.stats.TaskRealtimeMetricsMonitor(
            segmentGenerationMetrics,
            rowIngestionMeters,
            task.getMetricBuilder()
        );
    toolbox.addMonitor(metricsMonitor);

    boolean runLoopCompleted = false;
    try {
      final TaskStatus status = runDurableInbox(driver, appenderator, chunkReader, ioConfig);
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
  boolean acquireLockForRestoredSegment(SegmentIdWithShardSpec segmentId)
  {
    try {
      if (lockGranularity == LockGranularity.SEGMENT) {
        return toolbox.getTaskActionClient().submit(
            new SegmentLockAcquireAction(
                lockType,
                segmentId.getInterval(),
                segmentId.getVersion(),
                segmentId.getShardSpec().getPartitionNum(),
                1000L
            )
        ).isOk();
      } else {
        final TaskLock lock = toolbox.getTaskActionClient().submit(
            new TimeChunkLockAcquireAction(
                lockType,
                segmentId.getInterval(),
                1000L
            )
        );
        if (lock == null) {
          return false;
        }
        lock.assertNotRevoked();
        return true;
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Interrupts an in-flight poll so the loop can exit on graceful stop. */
  void requestWakeup()
  {
    final ShareInboxProcessor inboxProcessor = activeInboxProcessor.get();
    if (inboxProcessor != null) {
      inboxProcessor.requestStop();
    }
    final ShareGroupAcquisitionLoop acquisitionLoop = activeAcquisitionLoop.get();
    if (acquisitionLoop != null) {
      acquisitionLoop.requestStop();
      return;
    }
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

  private TaskStatus runDurableInbox(
      StreamAppenderatorDriver driver,
      Appenderator appenderator,
      StreamChunkReader<KafkaRecordEntity> chunkReader,
      ShareGroupIndexTaskIOConfig ioConfig
  ) throws Exception
  {
    if (sourceIdentity == null || specFingerprint == null) {
      throw new ISE("Durable share inbox context is not initialized");
    }
    final StorageConnector storageConnector = ioConfig.getInboxStorage()
                                                      .createStorageConnector(toolbox.getIndexingTmpDir());
    final ShareGroupBatchStore batchStore = new ShareGroupBatchStore(storageConnector, task.getId());
    final ShareGroupBatchStager batchStager = new DurableShareGroupBatchStager(
        batchStore,
        toolbox.getTaskActionClient(),
        task.getDataSource(),
        ioConfig.getInboxId(),
        ioConfig.getGroupId(),
        sourceIdentity,
        specFingerprint,
        ioConfig.getReceiptPageSize()
    );
    final int uploadThreads = ioConfig.getMaxConcurrentUploads();
    final ExecutorService stagingExecutor = new ThreadPoolExecutor(
        uploadThreads,
        uploadThreads,
        0L,
        TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(uploadThreads),
        Execs.makeThreadFactory("share-inbox-stager-%d"),
        new ThreadPoolExecutor.AbortPolicy()
    );
    final ScheduledExecutorService claimRenewalExecutor = Execs.scheduledSingleThreaded(
        "share-inbox-claim-renewal-%d"
    );
    final ExecutorService processorExecutor = Execs.singleThreaded("share-inbox-processor-%d");
    final ShareInboxProcessor inboxProcessor = new ShareInboxProcessor(
        toolbox.getTaskActionClient(),
        new ShareInboxManifestReader(
            batchStore,
            task.getDataSource(),
            ioConfig.getInboxId(),
            ioConfig.getGroupId(),
            sourceIdentity,
            specFingerprint
        ),
        new AppenderatorShareInboxBatchHandler(
            driver,
            chunkReader,
            toolbox.getTaskActionClient(),
            task.getId()
        ),
        claimRenewalExecutor,
        task.getDataSource(),
        ioConfig.getInboxId(),
        specFingerprint,
        task.getId(),
        ioConfig.getMaxProcessingManifests(),
        ioConfig.getMaxProcessingRecords(),
        ioConfig.getMaxProcessingBytes(),
        ioConfig.getClaimDurationMillis(),
        ioConfig.getClaimRenewalPeriodMillis(),
        ioConfig.getInboxPollPeriodMillis()
    );
    driver.startJob(this::acquireLockForRestoredSegment);
    try (final AcknowledgingRecordSupplier<KafkaTopicPartition, Long, KafkaRecordEntity> recordSupplier =
             supplierFactory.apply(ioConfig)) {
      final TaskStatus status = runDurableInboxLoops(
          recordSupplier,
          batchStager,
          stagingExecutor,
          inboxProcessor,
          processorExecutor,
          ioConfig
      );
      appenderator.close();
      return status;
    }
    finally {
      inboxProcessor.requestStop();
      processorExecutor.shutdownNow();
      claimRenewalExecutor.shutdownNow();
      stagingExecutor.shutdownNow();
    }
  }

  @VisibleForTesting
  TaskStatus runDurableInboxLoops(
      AcknowledgingRecordSupplier<KafkaTopicPartition, Long, KafkaRecordEntity> recordSupplier,
      ShareGroupBatchStager batchStager,
      ExecutorService stagingExecutor,
      ShareInboxProcessor inboxProcessor,
      ExecutorService processorExecutor,
      ShareGroupIndexTaskIOConfig ioConfig
  ) throws Exception
  {
    final ShareGroupAcquisitionLoop acquisitionLoop = createAcquisitionLoop(
        recordSupplier,
        batchStager,
        stagingExecutor,
        ioConfig
    );
    activeSupplier.set(recordSupplier);
    activeAcquisitionLoop.set(acquisitionLoop);
    activeInboxProcessor.set(inboxProcessor);
    if (task.isStopRequested()) {
      inboxProcessor.requestStop();
      acquisitionLoop.requestStop();
    }

    Future<?> processorFuture = null;
    Exception acquisitionFailure = null;
    try {
      processorFuture = processorExecutor.submit(() -> {
        try {
          inboxProcessor.run();
          return null;
        }
        finally {
          acquisitionLoop.requestStop();
        }
      });
      log.info(
          "Starting durable share inbox acquisition and processing for topic[%s], group[%s], inbox[%s].",
          ioConfig.getTopic(),
          ioConfig.getGroupId(),
          ioConfig.getInboxId()
      );
      try {
        acquisitionLoop.run();
      }
      catch (Exception e) {
        acquisitionFailure = e;
      }
      finally {
        inboxProcessor.requestStop();
      }

      try {
        processorFuture.get();
      }
      catch (ExecutionException e) {
        if (acquisitionFailure != null) {
          acquisitionFailure.addSuppressed(e.getCause());
        } else {
          throwFailure(e.getCause());
        }
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        if (acquisitionFailure != null) {
          acquisitionFailure.addSuppressed(e);
        } else {
          throw e;
        }
      }
      if (acquisitionFailure != null) {
        throw acquisitionFailure;
      }
      return TaskStatus.success(task.getId());
    }
    finally {
      inboxProcessor.requestStop();
      if (processorFuture != null && !processorFuture.isDone()) {
        processorFuture.cancel(true);
      }
      activeInboxProcessor.compareAndSet(inboxProcessor, null);
      activeAcquisitionLoop.compareAndSet(acquisitionLoop, null);
      activeSupplier.compareAndSet(recordSupplier, null);
    }
  }

  @VisibleForTesting
  TaskStatus runDurableInboxLoop(
      AcknowledgingRecordSupplier<KafkaTopicPartition, Long, KafkaRecordEntity> recordSupplier,
      ShareGroupBatchStager batchStager,
      ExecutorService stagingExecutor,
      ShareGroupIndexTaskIOConfig ioConfig
  ) throws Exception
  {
    final ShareGroupAcquisitionLoop acquisitionLoop = createAcquisitionLoop(
        recordSupplier,
        batchStager,
        stagingExecutor,
        ioConfig
    );
    activeSupplier.set(recordSupplier);
    activeAcquisitionLoop.set(acquisitionLoop);
    if (task.isStopRequested()) {
      acquisitionLoop.requestStop();
    }
    try {
      log.info(
          "Starting durable share inbox acquisition for topic[%s], group[%s], inbox[%s].",
          ioConfig.getTopic(),
          ioConfig.getGroupId(),
          ioConfig.getInboxId()
      );
      acquisitionLoop.run();
      return TaskStatus.success(task.getId());
    }
    finally {
      activeAcquisitionLoop.compareAndSet(acquisitionLoop, null);
      activeSupplier.compareAndSet(recordSupplier, null);
    }
  }

  private ShareGroupAcquisitionLoop createAcquisitionLoop(
      AcknowledgingRecordSupplier<KafkaTopicPartition, Long, KafkaRecordEntity> recordSupplier,
      ShareGroupBatchStager batchStager,
      ExecutorService stagingExecutor,
      ShareGroupIndexTaskIOConfig ioConfig
  )
  {
    final ShareGroupAcquisitionRegistry registry = new ShareGroupAcquisitionRegistry(
        ioConfig.getMaxStagingRecords(),
        ioConfig.getMaxStagingBytes(),
        ioConfig.getRenewalFraction(),
        System::nanoTime
    );
    return new ShareGroupAcquisitionLoop(
        ioConfig.getTopic(),
        recordSupplier,
        batchStager,
        stagingExecutor,
        registry,
        ShareGroupRawBatch::estimatedRecordSize,
        ioConfig.getPollTimeout(),
        resolveMaxPollIntervalMs(ioConfig)
    );
  }

  private static void throwFailure(Throwable failure) throws Exception
  {
    if (failure instanceof Exception) {
      throw (Exception) failure;
    }
    if (failure instanceof Error) {
      throw (Error) failure;
    }
    throw new RuntimeException(failure);
  }

  @VisibleForTesting
  long resolveMaxPollIntervalMs(ShareGroupIndexTaskIOConfig ioConfig)
  {
    final Properties properties = new Properties();
    KafkaRecordSupplier.addConsumerPropertiesFromConfig(
        properties,
        configMapper,
        ioConfig.getConsumerProperties()
    );
    final String value = properties.getProperty(
        ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,
        String.valueOf(DEFAULT_MAX_POLL_INTERVAL_MS)
    );
    try {
      final long interval = Long.parseLong(value);
      if (interval <= 0) {
        throw new NumberFormatException("must be positive");
      }
      return interval;
    }
    catch (NumberFormatException e) {
      throw new ISE(e, "Invalid Kafka max.poll.interval.ms[%s]", value);
    }
  }

  @VisibleForTesting
  void initializeInboxContext(ShareGroupIndexTaskIOConfig ioConfig)
  {
    sourceIdentity = sourceIdentityFactory.apply(ioConfig);
    specFingerprint = ShareGroupIngestionSpecFingerprint.compute(
        configMapper,
        task.getDataSchema(),
        task.getTuningConfig(),
        ioConfig
    );
  }

  @Nullable
  ShareGroupSourceIdentity getSourceIdentity()
  {
    return sourceIdentity;
  }

  @Nullable
  String getSpecFingerprint()
  {
    return specFingerprint;
  }

  private ShareGroupSourceIdentity resolveSourceIdentity(ShareGroupIndexTaskIOConfig ioConfig)
  {
    try (Admin admin = KafkaShareGroupSourceIdentityResolver.createAdmin(
        ioConfig.getConsumerProperties(),
        configMapper
    )) {
      return KafkaShareGroupSourceIdentityResolver.resolve(
          admin,
          ioConfig.getTopic(),
          Duration.ofSeconds(30)
      );
    }
  }

}
