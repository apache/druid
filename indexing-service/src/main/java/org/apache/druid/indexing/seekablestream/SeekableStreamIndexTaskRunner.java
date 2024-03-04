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

package org.apache.druid.indexing.seekablestream;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.data.input.Committer;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.LookupNodeService;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.ErrorResponse;
import org.apache.druid.indexer.IngestionState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexing.common.IngestionStatsAndErrorsTaskReport;
import org.apache.druid.indexing.common.IngestionStatsAndErrorsTaskReportData;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TaskRealtimeMetricsMonitorBuilder;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.CheckPointDataSourceMetadataAction;
import org.apache.druid.indexing.common.actions.ResetDataSourceMetadataAction;
import org.apache.druid.indexing.common.actions.SegmentLockAcquireAction;
import org.apache.druid.indexing.common.actions.TaskLocks;
import org.apache.druid.indexing.common.actions.TimeChunkLockAcquireAction;
import org.apache.druid.indexing.common.stats.TaskRealtimeMetricsMonitor;
import org.apache.druid.indexing.common.task.IndexTaskUtils;
import org.apache.druid.indexing.common.task.RealtimeIndexTask;
import org.apache.druid.indexing.input.InputRowSchemas;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.OrderedSequenceNumber;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisor;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.incremental.ParseExceptionHandler;
import org.apache.druid.segment.incremental.ParseExceptionReport;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.indexing.RealtimeIOConfig;
import org.apache.druid.segment.realtime.FireDepartment;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;
import org.apache.druid.segment.realtime.appenderator.Appenderator;
import org.apache.druid.segment.realtime.appenderator.AppenderatorDriverAddResult;
import org.apache.druid.segment.realtime.appenderator.SegmentsAndCommitMetadata;
import org.apache.druid.segment.realtime.appenderator.StreamAppenderator;
import org.apache.druid.segment.realtime.appenderator.StreamAppenderatorDriver;
import org.apache.druid.segment.realtime.firehose.ChatHandler;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.utils.CollectionUtils;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * Interface for abstracting the indexing task run logic.
 *
 * @param <PartitionIdType>    Partition Number Type
 * @param <SequenceOffsetType> Sequence Number Type
 */
@SuppressWarnings("CheckReturnValue")
public abstract class SeekableStreamIndexTaskRunner<PartitionIdType, SequenceOffsetType, RecordType extends ByteEntity> implements ChatHandler
{
  public enum Status
  {
    NOT_STARTED,
    STARTING,
    READING,
    PAUSED,
    PUBLISHING
  }

  private static final EmittingLogger log = new EmittingLogger(SeekableStreamIndexTaskRunner.class);
  static final String METADATA_NEXT_PARTITIONS = "nextPartitions";
  static final String METADATA_PUBLISH_PARTITIONS = "publishPartitions";

  private final Map<PartitionIdType, SequenceOffsetType> endOffsets;

  // lastReadOffsets are the last offsets that were read and processed.
  private final Map<PartitionIdType, SequenceOffsetType> lastReadOffsets = new HashMap<>();

  // currOffsets are what should become the start offsets of the next reader, if we stopped reading now. They are
  // initialized to the start offsets when the task begins.
  private final ConcurrentMap<PartitionIdType, SequenceOffsetType> currOffsets = new ConcurrentHashMap<>();
  private final ConcurrentMap<PartitionIdType, SequenceOffsetType> lastPersistedOffsets = new ConcurrentHashMap<>();

  // The pause lock and associated conditions are to support coordination between the Jetty threads and the main
  // ingestion loop. The goal is to provide callers of the API a guarantee that if pause() returns successfully
  // the ingestion loop has been stopped at the returned sequences and will not ingest any more data until resumed. The
  // fields are used as follows (every step requires acquiring [pauseLock]):
  //   Pausing:
  //   - In pause(), [pauseRequested] is set to true and then execution waits for [status] to change to PAUSED, with the
  //     condition checked when [hasPaused] is signalled.
  //   - In possiblyPause() called from the main loop, if [pauseRequested] is true, [status] is set to PAUSED,
  //     [hasPaused] is signalled, and execution pauses until [pauseRequested] becomes false, either by being set or by
  //     the [pauseMillis] timeout elapsing. [pauseRequested] is checked when [shouldResume] is signalled.
  //   Resuming:
  //   - In resume(), [pauseRequested] is set to false, [shouldResume] is signalled, and execution waits for [status] to
  //     change to something other than PAUSED, with the condition checked when [shouldResume] is signalled.
  //   - In possiblyPause(), when [shouldResume] is signalled, if [pauseRequested] has become false the pause loop ends,
  //     [status] is changed to STARTING and [shouldResume] is signalled.
  private final Lock pauseLock = new ReentrantLock();
  private final Condition hasPaused = pauseLock.newCondition();
  private final Condition shouldResume = pauseLock.newCondition();

  protected final AtomicBoolean stopRequested = new AtomicBoolean(false);
  private final AtomicBoolean publishOnStop = new AtomicBoolean(false);

  // [statusLock] is used to synchronize the Jetty thread calling stopGracefully() with the main run thread. It prevents
  // the main run thread from switching into a publishing state while the stopGracefully() thread thinks it's still in
  // a pre-publishing state. This is important because stopGracefully() will try to use the [stopRequested] flag to stop
  // the main thread where possible, but this flag is not honored once publishing has begun so in this case we must
  // interrupt the thread. The lock ensures that if the run thread is about to transition into publishing state, it
  // blocks until after stopGracefully() has set [stopRequested] and then does a final check on [stopRequested] before
  // transitioning to publishing state.
  private final Object statusLock = new Object();

  protected final Lock pollRetryLock = new ReentrantLock();
  protected final Condition isAwaitingRetry = pollRetryLock.newCondition();

  private final SeekableStreamIndexTask<PartitionIdType, SequenceOffsetType, RecordType> task;
  private final SeekableStreamIndexTaskIOConfig<PartitionIdType, SequenceOffsetType> ioConfig;
  private final SeekableStreamIndexTaskTuningConfig tuningConfig;
  private final InputRowSchema inputRowSchema;
  @Nullable
  private final InputFormat inputFormat;
  @Nullable
  private final InputRowParser<ByteBuffer> parser;
  private final String stream;

  private final Set<String> publishingSequences = Sets.newConcurrentHashSet();
  private final Set<String> publishedSequences = Sets.newConcurrentHashSet();
  private final List<ListenableFuture<SegmentsAndCommitMetadata>> publishWaitList = new ArrayList<>();
  private final List<ListenableFuture<SegmentsAndCommitMetadata>> handOffWaitList = new ArrayList<>();

  private final LockGranularity lockGranularityToUse;

  @MonotonicNonNull
  private RowIngestionMeters rowIngestionMeters;
  @MonotonicNonNull
  private ParseExceptionHandler parseExceptionHandler;
  @MonotonicNonNull
  private FireDepartmentMetrics fireDepartmentMetrics;

  @MonotonicNonNull
  private AuthorizerMapper authorizerMapper;

  private volatile DateTime startTime;
  private volatile Status status = Status.NOT_STARTED; // this is only ever set by the task runner thread (runThread)
  private volatile TaskToolbox toolbox;
  private volatile Thread runThread;
  private volatile Appenderator appenderator;
  private volatile StreamAppenderatorDriver driver;
  private volatile IngestionState ingestionState;

  protected volatile boolean pauseRequested = false;
  private volatile long nextCheckpointTime;

  private volatile CopyOnWriteArrayList<SequenceMetadata<PartitionIdType, SequenceOffsetType>> sequences;
  private volatile Throwable backgroundThreadException;

  private final Map<PartitionIdType, Long> partitionsThroughput = new HashMap<>();

  public SeekableStreamIndexTaskRunner(
      final SeekableStreamIndexTask<PartitionIdType, SequenceOffsetType, RecordType> task,
      @Nullable final InputRowParser<ByteBuffer> parser,
      final AuthorizerMapper authorizerMapper,
      final LockGranularity lockGranularityToUse
  )
  {
    Preconditions.checkNotNull(task);
    this.task = task;
    this.ioConfig = task.getIOConfig();
    this.tuningConfig = task.getTuningConfig();
    this.inputRowSchema = InputRowSchemas.fromDataSchema(task.getDataSchema());
    this.inputFormat = ioConfig.getInputFormat();
    this.parser = parser;
    this.authorizerMapper = authorizerMapper;
    this.stream = ioConfig.getStartSequenceNumbers().getStream();
    this.endOffsets = new ConcurrentHashMap<>(ioConfig.getEndSequenceNumbers().getPartitionSequenceNumberMap());
    this.sequences = new CopyOnWriteArrayList<>();
    this.ingestionState = IngestionState.NOT_STARTED;
    this.lockGranularityToUse = lockGranularityToUse;

    resetNextCheckpointTime();
  }

  public TaskStatus run(TaskToolbox toolbox)
  {
    try {
      return runInternal(toolbox);
    }
    catch (Exception e) {
      log.error(e, "Encountered exception while running task.");
      final String errorMsg = Throwables.getStackTraceAsString(e);
      toolbox.getTaskReportFileWriter().write(task.getId(), getTaskCompletionReports(errorMsg, 0L));
      return TaskStatus.failure(
          task.getId(),
          errorMsg
      );
    }
  }

  private Set<PartitionIdType> computeExclusiveStartPartitionsForSequence(
      Map<PartitionIdType, SequenceOffsetType> sequenceStartOffsets
  )
  {
    if (sequenceStartOffsets.equals(ioConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap())) {
      return ioConfig.getStartSequenceNumbers().getExclusivePartitions();
    } else {
      return isEndOffsetExclusive() ? Collections.emptySet() : sequenceStartOffsets.keySet();
    }
  }

  @VisibleForTesting
  public void setToolbox(TaskToolbox toolbox)
  {
    this.toolbox = toolbox;
  }

  @VisibleForTesting
  public void initializeSequences() throws IOException
  {
    if (!restoreSequences()) {
      final TreeMap<Integer, Map<PartitionIdType, SequenceOffsetType>> checkpoints = getCheckPointsFromContext(
          toolbox,
          task.getContextValue(SeekableStreamSupervisor.CHECKPOINTS_CTX_KEY)
      );
      if (checkpoints != null) {
        Iterator<Map.Entry<Integer, Map<PartitionIdType, SequenceOffsetType>>> sequenceOffsets = checkpoints.entrySet()
                                                                                                            .iterator();
        Map.Entry<Integer, Map<PartitionIdType, SequenceOffsetType>> previous = sequenceOffsets.next();
        while (sequenceOffsets.hasNext()) {
          Map.Entry<Integer, Map<PartitionIdType, SequenceOffsetType>> current = sequenceOffsets.next();
          final Set<PartitionIdType> exclusiveStartPartitions = computeExclusiveStartPartitionsForSequence(
              previous.getValue()
          );
          addSequence(
              new SequenceMetadata<>(
                  previous.getKey(),
                  StringUtils.format("%s_%s", ioConfig.getBaseSequenceName(), previous.getKey()),
                  previous.getValue(),
                  current.getValue(),
                  true,
                  exclusiveStartPartitions,
                  getTaskLockType()
              )
          );
          previous = current;
        }
        final Set<PartitionIdType> exclusiveStartPartitions = computeExclusiveStartPartitionsForSequence(
            previous.getValue()
        );
        addSequence(
            new SequenceMetadata<>(
                previous.getKey(),
                StringUtils.format("%s_%s", ioConfig.getBaseSequenceName(), previous.getKey()),
                previous.getValue(),
                endOffsets,
                false,
                exclusiveStartPartitions,
                getTaskLockType()
            )
        );
      } else {
        addSequence(
            new SequenceMetadata<>(
                0,
                StringUtils.format("%s_%s", ioConfig.getBaseSequenceName(), 0),
                ioConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap(),
                endOffsets,
                false,
                ioConfig.getStartSequenceNumbers().getExclusivePartitions(),
                getTaskLockType()
            )
        );
      }
    }

    log.info("Starting with sequences: %s", sequences);
  }

  private TaskStatus runInternal(TaskToolbox toolbox) throws Exception
  {
    startTime = DateTimes.nowUtc();
    status = Status.STARTING;

    setToolbox(toolbox);

    authorizerMapper = toolbox.getAuthorizerMapper();
    rowIngestionMeters = toolbox.getRowIngestionMetersFactory().createRowIngestionMeters();
    parseExceptionHandler = new ParseExceptionHandler(
        rowIngestionMeters,
        tuningConfig.isLogParseExceptions(),
        tuningConfig.getMaxParseExceptions(),
        tuningConfig.getMaxSavedParseExceptions()
    );

    // Now we can initialize StreamChunkReader with the given toolbox.
    final StreamChunkParser parser = new StreamChunkParser<RecordType>(
        this.parser,
        inputFormat,
        inputRowSchema,
        task.getDataSchema().getTransformSpec(),
        toolbox.getIndexingTmpDir(),
        row -> row != null && task.withinMinMaxRecordTime(row),
        rowIngestionMeters,
        parseExceptionHandler
    );

    initializeSequences();

    log.debug("Found chat handler of class[%s]", toolbox.getChatHandlerProvider().getClass().getName());
    toolbox.getChatHandlerProvider().register(task.getId(), this, false);

    runThread = Thread.currentThread();

    // Set up FireDepartmentMetrics
    final FireDepartment fireDepartmentForMetrics = new FireDepartment(
        task.getDataSchema(),
        new RealtimeIOConfig(null, null),
        null
    );
    this.fireDepartmentMetrics = fireDepartmentForMetrics.getMetrics();
    TaskRealtimeMetricsMonitor metricsMonitor = TaskRealtimeMetricsMonitorBuilder.build(task, fireDepartmentForMetrics, rowIngestionMeters);
    toolbox.addMonitor(metricsMonitor);

    final String lookupTier = task.getContextValue(RealtimeIndexTask.CTX_KEY_LOOKUP_TIER);
    final LookupNodeService lookupNodeService = lookupTier == null ?
                                                toolbox.getLookupNodeService() :
                                                new LookupNodeService(lookupTier);

    final DiscoveryDruidNode discoveryDruidNode = new DiscoveryDruidNode(
        toolbox.getDruidNode(),
        NodeRole.PEON,
        ImmutableMap.of(
            toolbox.getDataNodeService().getName(), toolbox.getDataNodeService(),
            lookupNodeService.getName(), lookupNodeService
        )
    );

    Throwable caughtExceptionOuter = null;

    //milliseconds waited for created segments to be handed off
    long handoffWaitMs = 0L;

    try (final RecordSupplier<PartitionIdType, SequenceOffsetType, RecordType> recordSupplier =
             task.newTaskRecordSupplier(toolbox)) {
      if (toolbox.getAppenderatorsManager().shouldTaskMakeNodeAnnouncements()) {
        toolbox.getDataSegmentServerAnnouncer().announce();
        toolbox.getDruidNodeAnnouncer().announce(discoveryDruidNode);
      }
      appenderator = task.newAppenderator(toolbox, fireDepartmentMetrics, rowIngestionMeters, parseExceptionHandler);
      driver = task.newDriver(appenderator, toolbox, fireDepartmentMetrics);

      // Start up, set up initial sequences.
      final Object restoredMetadata = driver.startJob(
          segmentId -> {
            try {
              if (lockGranularityToUse == LockGranularity.SEGMENT) {
                return toolbox.getTaskActionClient().submit(
                    new SegmentLockAcquireAction(
                        TaskLockType.EXCLUSIVE,
                        segmentId.getInterval(),
                        segmentId.getVersion(),
                        segmentId.getShardSpec().getPartitionNum(),
                        1000L
                    )
                ).isOk();
              } else {
                final TaskLock lock = toolbox.getTaskActionClient().submit(
                    new TimeChunkLockAcquireAction(
                        TaskLocks.determineLockTypeForAppend(task.getContext()),
                        segmentId.getInterval(),
                        1000L
                    )
                );
                if (lock == null) {
                  return false;
                }
                if (lock.isRevoked()) {
                  throw new ISE(StringUtils.format("Lock for interval [%s] was revoked.", segmentId.getInterval()));
                }
                return true;
              }
            }
            catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
      );
      if (restoredMetadata == null) {
        // no persist has happened so far
        // so either this is a brand new task or replacement of a failed task
        Preconditions.checkState(sequences.get(0).startOffsets.entrySet().stream().allMatch(
            partitionOffsetEntry ->
                createSequenceNumber(partitionOffsetEntry.getValue()).compareTo(
                    createSequenceNumber(ioConfig.getStartSequenceNumbers()
                                                 .getPartitionSequenceNumberMap()
                                                 .get(partitionOffsetEntry.getKey())
                    )) >= 0
        ), "Sequence sequences are not compatible with start sequences of task");
        currOffsets.putAll(sequences.get(0).startOffsets);
      } else {
        @SuppressWarnings("unchecked")
        final Map<String, Object> restoredMetadataMap = (Map) restoredMetadata;
        final SeekableStreamEndSequenceNumbers<PartitionIdType, SequenceOffsetType> restoredNextPartitions =
            deserializePartitionsFromMetadata(
                toolbox.getJsonMapper(),
                restoredMetadataMap.get(METADATA_NEXT_PARTITIONS)
            );

        currOffsets.putAll(restoredNextPartitions.getPartitionSequenceNumberMap());

        // Sanity checks.
        if (!restoredNextPartitions.getStream().equals(ioConfig.getStartSequenceNumbers().getStream())) {
          throw new ISE(
              "Restored stream[%s] but expected stream[%s]",
              restoredNextPartitions.getStream(),
              ioConfig.getStartSequenceNumbers().getStream()
          );
        }

        if (!currOffsets.keySet().equals(ioConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().keySet())) {
          throw new ISE(
              "Restored partitions[%s] but expected partitions[%s]",
              currOffsets.keySet(),
              ioConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().keySet()
          );
        }
        // sequences size can be 0 only when all sequences got published and task stopped before it could finish
        // which is super rare
        if (sequences.size() == 0 || getLastSequenceMetadata().isCheckpointed()) {
          this.endOffsets.putAll(sequences.size() == 0
                                 ? currOffsets
                                 : getLastSequenceMetadata().getEndOffsets());
        }
      }

      log.info(
          "Initialized sequences: %s",
          sequences.stream().map(SequenceMetadata::toString).collect(Collectors.joining(", "))
      );

      // Filter out partitions with END_OF_SHARD markers since these partitions have already been fully read. This
      // should have been done by the supervisor already so this is defensive.
      int numPreFilterPartitions = currOffsets.size();
      if (currOffsets.entrySet().removeIf(x -> isEndOfShard(x.getValue()))) {
        log.info(
            "Removed [%d] partitions from assignment which have already been closed.",
            numPreFilterPartitions - currOffsets.size()
        );
      }

      // Initialize lastReadOffsets immediately after restoring currOffsets. This is only done when end offsets are
      // inclusive, because the point of initializing lastReadOffsets here is so we know when to skip the start record.
      // When end offsets are exclusive, we never skip the start record.
      if (!isEndOffsetExclusive()) {
        for (Map.Entry<PartitionIdType, SequenceOffsetType> entry : currOffsets.entrySet()) {
          final boolean isAtStart = entry.getValue().equals(
              ioConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap().get(entry.getKey())
          );

          if (!isAtStart || ioConfig.getStartSequenceNumbers().getExclusivePartitions().contains(entry.getKey())) {
            lastReadOffsets.put(entry.getKey(), entry.getValue());
          }
        }
      }

      // Set up committer.
      final Supplier<Committer> committerSupplier = () -> {
        final Map<PartitionIdType, SequenceOffsetType> snapshot = ImmutableMap.copyOf(currOffsets);
        lastPersistedOffsets.clear();
        lastPersistedOffsets.putAll(snapshot);

        return new Committer()
        {
          @Override
          public Object getMetadata()
          {
            return ImmutableMap.of(METADATA_NEXT_PARTITIONS, new SeekableStreamEndSequenceNumbers<>(stream, snapshot));
          }

          @Override
          public void run()
          {
            // Do nothing.
          }
        };
      };

      // restart publishing of sequences (if any)
      maybePersistAndPublishSequences(committerSupplier);

      Set<StreamPartition<PartitionIdType>> assignment = assignPartitions(recordSupplier);
      possiblyResetDataSourceMetadata(toolbox, recordSupplier, assignment);
      seekToStartingSequence(recordSupplier, assignment);

      ingestionState = IngestionState.BUILD_SEGMENTS;

      // Main loop.
      // Could eventually support leader/follower mode (for keeping replicas more in sync)
      boolean stillReading = !assignment.isEmpty();
      status = Status.READING;
      Throwable caughtExceptionInner = null;

      try {
        while (stillReading) {
          if (possiblyPause()) {
            // The partition assignments may have changed while paused by a call to setEndOffsets() so reassign
            // partitions upon resuming. Don't call "seekToStartingSequence" after "assignPartitions", because there's
            // no need to re-seek here. All we're going to be doing is dropping partitions.
            assignment = assignPartitions(recordSupplier);
            possiblyResetDataSourceMetadata(toolbox, recordSupplier, assignment);

            if (assignment.isEmpty()) {
              log.debug("All partitions have been fully read.");
              publishOnStop.set(true);
              stopRequested.set(true);
            }
          }

          // if stop is requested or task's end sequence is set by call to setEndOffsets method with finish set to true
          if (stopRequested.get() || sequences.size() == 0 || getLastSequenceMetadata().isCheckpointed()) {
            status = Status.PUBLISHING;
          }

          if (stopRequested.get()) {
            break;
          }

          if (backgroundThreadException != null) {
            throw new RuntimeException(backgroundThreadException);
          }

          checkPublishAndHandoffFailure();

          maybePersistAndPublishSequences(committerSupplier);

          // calling getRecord() ensures that exceptions specific to kafka/kinesis like OffsetOutOfRangeException
          // are handled in the subclasses.
          List<OrderedPartitionableRecord<PartitionIdType, SequenceOffsetType, RecordType>> records = getRecords(
              recordSupplier,
              toolbox
          );

          // note: getRecords() also updates assignment
          stillReading = !assignment.isEmpty();

          SequenceMetadata<PartitionIdType, SequenceOffsetType> sequenceToCheckpoint = null;
          for (OrderedPartitionableRecord<PartitionIdType, SequenceOffsetType, RecordType> record : records) {
            final boolean shouldProcess = verifyRecordInRange(record.getPartitionId(), record.getSequenceNumber());

            log.trace(
                "Got stream[%s] partition[%s] sequenceNumber[%s], shouldProcess[%s].",
                record.getStream(),
                record.getPartitionId(),
                record.getSequenceNumber(),
                shouldProcess
            );

            if (shouldProcess) {
              final List<InputRow> rows = parser.parse(record.getData(), isEndOfShard(record.getSequenceNumber()));
              boolean isPersistRequired = false;

              final SequenceMetadata<PartitionIdType, SequenceOffsetType> sequenceToUse = sequences
                  .stream()
                  .filter(sequenceMetadata -> sequenceMetadata.canHandle(this, record))
                  .findFirst()
                  .orElse(null);

              if (sequenceToUse == null) {
                throw new ISE(
                    "Cannot find any valid sequence for record with partition [%s] and sequenceNumber [%s]. Current sequences: %s",
                    record.getPartitionId(),
                    record.getSequenceNumber(),
                    sequences
                );
              }

              for (InputRow row : rows) {
                final AppenderatorDriverAddResult addResult = driver.add(
                    row,
                    sequenceToUse.getSequenceName(),
                    committerSupplier,
                    true,
                    // do not allow incremental persists to happen until all the rows from this batch
                    // of rows are indexed
                    false
                );

                if (addResult.isOk()) {
                  // If the number of rows in the segment exceeds the threshold after adding a row,
                  // move the segment out from the active segments of BaseAppenderatorDriver to make a new segment.
                  final boolean isPushRequired = addResult.isPushRequired(
                      tuningConfig.getPartitionsSpec().getMaxRowsPerSegment(),
                      tuningConfig.getPartitionsSpec()
                                  .getMaxTotalRowsOr(DynamicPartitionsSpec.DEFAULT_MAX_TOTAL_ROWS)
                  );
                  if (isPushRequired && !sequenceToUse.isCheckpointed()) {
                    sequenceToCheckpoint = sequenceToUse;
                  }
                  isPersistRequired |= addResult.isPersistRequired();
                  partitionsThroughput.merge(record.getPartitionId(), 1L, Long::sum);
                } else {
                  // Failure to allocate segment puts determinism at risk, bail out to be safe.
                  // May want configurable behavior here at some point.
                  // If we allow continuing, then consider blacklisting the interval for a while to avoid constant checks.
                  throw new ISE("Could not allocate segment for row with timestamp[%s]", row.getTimestamp());
                }
              }
              if (isPersistRequired) {
                Futures.addCallback(
                    driver.persistAsync(committerSupplier.get()),
                    new FutureCallback<Object>()
                    {
                      @Override
                      public void onSuccess(@Nullable Object result)
                      {
                        log.debug("Persist completed with metadata: %s", result);
                      }

                      @Override
                      public void onFailure(Throwable t)
                      {
                        log.error("Persist failed, dying");
                        backgroundThreadException = t;
                      }
                    },
                    MoreExecutors.directExecutor()
                );
              }

              // in kafka, we can easily get the next offset by adding 1, but for kinesis, there's no way
              // to get the next sequence number without having to make an expensive api call. So the behavior
              // here for kafka is to +1 while for kinesis we simply save the current sequence number
              lastReadOffsets.put(record.getPartitionId(), record.getSequenceNumber());
              currOffsets.put(record.getPartitionId(), getNextStartOffset(record.getSequenceNumber()));
            }

            // Use record.getSequenceNumber() in the moreToRead check, since currOffsets might not have been
            // updated if we were skipping records for being beyond the end.
            final boolean moreToReadAfterThisRecord = isMoreToReadAfterReadingRecord(
                record.getSequenceNumber(),
                endOffsets.get(record.getPartitionId())
            );

            if (!moreToReadAfterThisRecord && assignment.remove(record.getStreamPartition())) {
              log.info("Finished reading stream[%s], partition[%s].", record.getStream(), record.getPartitionId());
              recordSupplier.assign(assignment);
              stillReading = !assignment.isEmpty();
            }
          }

          if (!stillReading) {
            // We let the fireDepartmentMetrics know that all messages have been read. This way, some metrics such as
            // high message gap need not be reported
            fireDepartmentMetrics.markProcessingDone();
          }

          if (System.currentTimeMillis() > nextCheckpointTime) {
            sequenceToCheckpoint = getLastSequenceMetadata();
          }

          if (sequenceToCheckpoint != null && stillReading) {
            Preconditions.checkArgument(
                getLastSequenceMetadata()
                    .getSequenceName()
                    .equals(sequenceToCheckpoint.getSequenceName()),
                "Cannot checkpoint a sequence [%s] which is not the latest one, sequences %s",
                sequenceToCheckpoint,
                sequences
            );
            requestPause();
            final CheckPointDataSourceMetadataAction checkpointAction = new CheckPointDataSourceMetadataAction(
                task.getDataSource(),
                ioConfig.getTaskGroupId(),
                null,
                createDataSourceMetadata(
                    new SeekableStreamStartSequenceNumbers<>(
                        stream,
                        sequenceToCheckpoint.getStartOffsets(),
                        sequenceToCheckpoint.getExclusiveStartPartitions()
                    )
                )
            );
            if (!toolbox.getTaskActionClient().submit(checkpointAction)) {
              throw new ISE("Checkpoint request with sequences [%s] failed, dying", currOffsets);
            }
          }
        }
        ingestionState = IngestionState.COMPLETED;
      }
      catch (Exception e) {
        // (1) catch all exceptions while reading from kafka
        caughtExceptionInner = e;
        log.error(e, "Encountered exception in run() before persisting.");
        throw e;
      }
      finally {
        try {
          // To handle cases where tasks stop reading due to stop request or exceptions
          fireDepartmentMetrics.markProcessingDone();
          driver.persist(committerSupplier.get()); // persist pending data
        }
        catch (Exception e) {
          if (caughtExceptionInner != null) {
            caughtExceptionInner.addSuppressed(e);
          } else {
            throw e;
          }
        }
      }

      synchronized (statusLock) {
        if (stopRequested.get() && !publishOnStop.get()) {
          throw new InterruptedException("Stopping without publishing");
        }

        status = Status.PUBLISHING;
      }

      // We need to copy sequences here, because the success callback in publishAndRegisterHandoff removes items from
      // the sequence list. If a publish finishes before we finish iterating through the sequence list, we can
      // end up skipping some sequences.
      List<SequenceMetadata<PartitionIdType, SequenceOffsetType>> sequencesSnapshot = new ArrayList<>(sequences);
      for (int i = 0; i < sequencesSnapshot.size(); i++) {
        final SequenceMetadata<PartitionIdType, SequenceOffsetType> sequenceMetadata = sequencesSnapshot.get(i);
        if (!publishingSequences.contains(sequenceMetadata.getSequenceName())
            && !publishedSequences.contains(sequenceMetadata.getSequenceName())) {
          final boolean isLast = i == (sequencesSnapshot.size() - 1);
          if (isLast) {
            // Shorten endOffsets of the last sequence to match currOffsets.
            sequenceMetadata.setEndOffsets(currOffsets);
          }

          // Update assignments of the sequence, which should clear them. (This will be checked later, when the
          // Committer is built.)
          sequenceMetadata.updateAssignments(currOffsets, this::isMoreToReadAfterReadingRecord);
          publishingSequences.add(sequenceMetadata.getSequenceName());
          // persist already done in finally, so directly add to publishQueue
          publishAndRegisterHandoff(sequenceMetadata);
        }
      }

      if (backgroundThreadException != null) {
        throw new RuntimeException(backgroundThreadException);
      }

      // Wait for publish futures to complete.
      Futures.allAsList(publishWaitList).get();

      // Wait for handoff futures to complete.
      // Note that every publishing task (created by calling AppenderatorDriver.publish()) has a corresponding
      // handoffFuture. handoffFuture can throw an exception if 1) the corresponding publishFuture failed or 2) it
      // failed to persist sequences. It might also return null if handoff failed, but was recoverable.
      // See publishAndRegisterHandoff() for details.
      List<SegmentsAndCommitMetadata> handedOffList = Collections.emptyList();
      if (tuningConfig.getHandoffConditionTimeout() == 0) {
        handedOffList = Futures.allAsList(handOffWaitList).get();
      } else {
        final long start = System.nanoTime();
        try {
          handedOffList = Futures.allAsList(handOffWaitList)
                                 .get(tuningConfig.getHandoffConditionTimeout(), TimeUnit.MILLISECONDS);
        }
        catch (TimeoutException e) {
          // Handoff timeout is not an indexing failure, but coordination failure. We simply ignore timeout exception
          // here.
          log.makeAlert("Timeout waiting for handoff")
             .addData("taskId", task.getId())
             .addData("handoffConditionTimeout", tuningConfig.getHandoffConditionTimeout())
             .emit();
        }
        finally {
          handoffWaitMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        }
      }

      for (SegmentsAndCommitMetadata handedOff : handedOffList) {
        log.info(
            "Handoff complete for segments: %s",
            String.join(", ", Lists.transform(handedOff.getSegments(), DataSegment::toString))
        );
      }

      appenderator.close();
    }
    catch (InterruptedException | RejectedExecutionException e) {
      // (2) catch InterruptedException and RejectedExecutionException thrown for the whole ingestion steps including
      // the final publishing.
      caughtExceptionOuter = e;
      try {
        Futures.allAsList(publishWaitList).cancel(true);
        Futures.allAsList(handOffWaitList).cancel(true);
        if (appenderator != null) {
          appenderator.closeNow();
        }
      }
      catch (Exception e2) {
        e.addSuppressed(e2);
      }

      // handle the InterruptedException that gets wrapped in a RejectedExecutionException
      if (e instanceof RejectedExecutionException
          && (e.getCause() == null || !(e.getCause() instanceof InterruptedException))) {
        throw e;
      }

      // if we were interrupted because we were asked to stop, handle the exception and return success, else rethrow
      if (!stopRequested.get()) {
        Thread.currentThread().interrupt();
        throw e;
      }
    }
    catch (Exception e) {
      // (3) catch all other exceptions thrown for the whole ingestion steps including the final publishing.
      caughtExceptionOuter = e;
      try {
        Futures.allAsList(publishWaitList).cancel(true);
        Futures.allAsList(handOffWaitList).cancel(true);
        if (appenderator != null) {
          appenderator.closeNow();
        }
      }
      catch (Exception e2) {
        e.addSuppressed(e2);
      }
      throw e;
    }
    finally {
      try {
        if (driver != null) {
          driver.close();
        }
        toolbox.getChatHandlerProvider().unregister(task.getId());
        toolbox.removeMonitor(metricsMonitor);
        if (toolbox.getAppenderatorsManager().shouldTaskMakeNodeAnnouncements()) {
          toolbox.getDruidNodeAnnouncer().unannounce(discoveryDruidNode);
          toolbox.getDataSegmentServerAnnouncer().unannounce();
        }
      }
      catch (Throwable e) {
        if (caughtExceptionOuter != null) {
          caughtExceptionOuter.addSuppressed(e);
        } else {
          throw e;
        }
      }
    }

    toolbox.getTaskReportFileWriter().write(task.getId(), getTaskCompletionReports(null, handoffWaitMs));
    return TaskStatus.success(task.getId());
  }

  private TaskLockType getTaskLockType()
  {
    return TaskLocks.determineLockTypeForAppend(task.getContext());
  }

  private void checkPublishAndHandoffFailure() throws ExecutionException, InterruptedException
  {
    // Check if any publishFuture failed.
    final List<ListenableFuture<SegmentsAndCommitMetadata>> publishFinished = publishWaitList
        .stream()
        .filter(Future::isDone)
        .collect(Collectors.toList());

    for (ListenableFuture<SegmentsAndCommitMetadata> publishFuture : publishFinished) {
      // If publishFuture failed, the below line will throw an exception and catched by (1), and then (2) or (3).
      publishFuture.get();
    }

    publishWaitList.removeAll(publishFinished);

    // Check if any handoffFuture failed.
    final List<ListenableFuture<SegmentsAndCommitMetadata>> handoffFinished = handOffWaitList
        .stream()
        .filter(Future::isDone)
        .collect(Collectors.toList());

    for (ListenableFuture<SegmentsAndCommitMetadata> handoffFuture : handoffFinished) {
      // If handoffFuture failed, the below line will throw an exception and catched by (1), and then (2) or (3).
      handoffFuture.get();
    }

    handOffWaitList.removeAll(handoffFinished);
  }

  private void publishAndRegisterHandoff(SequenceMetadata<PartitionIdType, SequenceOffsetType> sequenceMetadata)
  {
    log.debug("Publishing segments for sequence [%s].", sequenceMetadata);

    final ListenableFuture<SegmentsAndCommitMetadata> publishFuture = Futures.transform(
        driver.publish(
            sequenceMetadata.createPublisher(this, toolbox, ioConfig.isUseTransaction()),
            sequenceMetadata.getCommitterSupplier(this, stream, lastPersistedOffsets).get(),
            Collections.singletonList(sequenceMetadata.getSequenceName())
        ),
        (Function<SegmentsAndCommitMetadata, SegmentsAndCommitMetadata>) publishedSegmentsAndMetadata -> {
          if (publishedSegmentsAndMetadata == null) {
            throw new ISE(
                "Transaction failure publishing segments for sequence [%s]",
                sequenceMetadata
            );
          } else {
            return publishedSegmentsAndMetadata;
          }
        },
        MoreExecutors.directExecutor()
    );
    publishWaitList.add(publishFuture);

    // Create a handoffFuture for every publishFuture. The created handoffFuture must fail if publishFuture fails.
    final SettableFuture<SegmentsAndCommitMetadata> handoffFuture = SettableFuture.create();
    handOffWaitList.add(handoffFuture);

    Futures.addCallback(
        publishFuture,
        new FutureCallback<SegmentsAndCommitMetadata>()
        {
          @Override
          public void onSuccess(SegmentsAndCommitMetadata publishedSegmentsAndCommitMetadata)
          {
            log.info(
                "Published %s segments for sequence [%s] with metadata [%s].",
                publishedSegmentsAndCommitMetadata.getSegments().size(),
                sequenceMetadata.getSequenceName(),
                Preconditions.checkNotNull(publishedSegmentsAndCommitMetadata.getCommitMetadata(), "commitMetadata")
            );
            log.infoSegments(publishedSegmentsAndCommitMetadata.getSegments(), "Published segments");

            publishedSequences.add(sequenceMetadata.getSequenceName());
            sequences.remove(sequenceMetadata);
            publishingSequences.remove(sequenceMetadata.getSequenceName());

            try {
              persistSequences();
            }
            catch (IOException e) {
              log.error(e, "Unable to persist state, dying");
              handoffFuture.setException(e);
              throw new RuntimeException(e);
            }

            Futures.transform(
                driver.registerHandoff(publishedSegmentsAndCommitMetadata),
                new Function<SegmentsAndCommitMetadata, Void>()
                {
                  @Nullable
                  @Override
                  public Void apply(@Nullable SegmentsAndCommitMetadata handoffSegmentsAndCommitMetadata)
                  {
                    if (handoffSegmentsAndCommitMetadata == null) {
                      log.warn(
                          "Failed to hand off %s segments",
                          publishedSegmentsAndCommitMetadata.getSegments().size()
                      );
                      log.warnSegments(
                          publishedSegmentsAndCommitMetadata.getSegments(),
                          "Failed to hand off segments"
                      );
                    }
                    handoffFuture.set(handoffSegmentsAndCommitMetadata);
                    return null;
                  }
                },
                MoreExecutors.directExecutor()
            );
            // emit segment count metric:
            int segmentCount = 0;
            if (publishedSegmentsAndCommitMetadata != null
                && publishedSegmentsAndCommitMetadata.getSegments() != null) {
              segmentCount = publishedSegmentsAndCommitMetadata.getSegments().size();
            }
            task.emitMetric(
                toolbox.getEmitter(),
                "ingest/segments/count",
                segmentCount
            );
          }

          @Override
          public void onFailure(Throwable t)
          {
            log.error(t, "Error while publishing segments for sequenceNumber[%s]", sequenceMetadata);
            handoffFuture.setException(t);
          }
        },
        MoreExecutors.directExecutor()
    );
  }

  private static File getSequencesPersistFile(TaskToolbox toolbox)
  {
    return new File(toolbox.getPersistDir(), "sequences.json");
  }

  private boolean restoreSequences() throws IOException
  {
    final File sequencesPersistFile = getSequencesPersistFile(toolbox);
    if (sequencesPersistFile.exists()) {
      sequences = new CopyOnWriteArrayList<>(
          toolbox.getJsonMapper().readValue(
              sequencesPersistFile,
              getSequenceMetadataTypeReference()
          )
      );
      return true;
    } else {
      return false;
    }
  }

  private synchronized void persistSequences() throws IOException
  {
    toolbox.getJsonMapper().writerFor(
        getSequenceMetadataTypeReference()
    ).writeValue(getSequencesPersistFile(toolbox), sequences);

    log.info("Saved sequence metadata to disk: %s", sequences);
  }

  /**
   * Return a map of reports for the task.
   *
   * A successfull task should always have a null errorMsg. Segments availability is inherently confirmed
   * if the task was succesful.
   *
   * A falied task should always have a non-null errorMsg. Segment availability is never confirmed if the task
   * was not successful.
   *
   * @param errorMsg Nullable error message for the task. null if task succeeded.
   * @param handoffWaitMs Milliseconds waited for segments to be handed off.
   * @return Map of reports for the task.
   */
  private Map<String, TaskReport> getTaskCompletionReports(@Nullable String errorMsg, long handoffWaitMs)
  {
    return TaskReport.buildTaskReports(
        new IngestionStatsAndErrorsTaskReport(
            task.getId(),
            new IngestionStatsAndErrorsTaskReportData(
                ingestionState,
                getTaskCompletionUnparseableEvents(),
                getTaskCompletionRowStats(),
                errorMsg,
                errorMsg == null,
                handoffWaitMs,
                getPartitionStats()
            )
        )
    );
  }

  private Map<String, Long> getPartitionStats()
  {
    return CollectionUtils.mapKeys(partitionsThroughput, key -> key.toString());
  }

  private Map<String, Object> getTaskCompletionUnparseableEvents()
  {
    Map<String, Object> unparseableEventsMap = new HashMap<>();
    List<ParseExceptionReport> buildSegmentsParseExceptionMessages = IndexTaskUtils.getReportListFromSavedParseExceptions(
        parseExceptionHandler.getSavedParseExceptionReports()
    );
    if (buildSegmentsParseExceptionMessages != null) {
      unparseableEventsMap.put(RowIngestionMeters.BUILD_SEGMENTS, buildSegmentsParseExceptionMessages);
    }
    return unparseableEventsMap;
  }

  private Map<String, Object> getTaskCompletionRowStats()
  {
    Map<String, Object> metrics = new HashMap<>();
    metrics.put(
        RowIngestionMeters.BUILD_SEGMENTS,
        rowIngestionMeters.getTotals()
    );
    return metrics;
  }


  private void maybePersistAndPublishSequences(Supplier<Committer> committerSupplier)
      throws InterruptedException
  {
    for (SequenceMetadata<PartitionIdType, SequenceOffsetType> sequenceMetadata : sequences) {
      sequenceMetadata.updateAssignments(currOffsets, this::isMoreToReadBeforeReadingRecord);
      if (!sequenceMetadata.isOpen()
          && !publishingSequences.contains(sequenceMetadata.getSequenceName())
          && !publishedSequences.contains(sequenceMetadata.getSequenceName())) {
        publishingSequences.add(sequenceMetadata.getSequenceName());
        try {
          final Object result = driver.persist(committerSupplier.get());
          log.debug(
              "Persist completed with metadata [%s], adding sequence [%s] to publish queue.",
              result,
              sequenceMetadata.getSequenceName()
          );
          publishAndRegisterHandoff(sequenceMetadata);
        }
        catch (InterruptedException e) {
          log.warn("Interrupted while persisting metadata for sequence [%s].", sequenceMetadata.getSequenceName());
          throw e;
        }
      }
    }
  }

  private Set<StreamPartition<PartitionIdType>> assignPartitions(
      RecordSupplier<PartitionIdType, SequenceOffsetType, RecordType> recordSupplier
  )
  {
    final Set<StreamPartition<PartitionIdType>> assignment = new HashSet<>();
    for (Map.Entry<PartitionIdType, SequenceOffsetType> entry : currOffsets.entrySet()) {
      final PartitionIdType partition = entry.getKey();
      final SequenceOffsetType currOffset = entry.getValue();
      final SequenceOffsetType endOffset = endOffsets.get(partition);

      if (!isRecordAlreadyRead(partition, endOffset) && isMoreToReadBeforeReadingRecord(currOffset, endOffset)) {
        log.info(
            "Adding partition[%s], start[%s] -> end[%s] to assignment.",
            partition,
            currOffset,
            endOffset
        );

        assignment.add(StreamPartition.of(stream, partition));
      } else {
        log.info("Finished reading partition[%s], up to[%s].", partition, currOffset);
      }
    }

    recordSupplier.assign(assignment);

    return assignment;
  }

  private void addSequence(final SequenceMetadata<PartitionIdType, SequenceOffsetType> sequenceMetadata)
  {
    // Sanity check that the start of the new sequence matches up with the end of the prior sequence.
    for (Map.Entry<PartitionIdType, SequenceOffsetType> entry : sequenceMetadata.getStartOffsets().entrySet()) {
      final PartitionIdType partition = entry.getKey();
      final SequenceOffsetType startOffset = entry.getValue();

      if (!sequences.isEmpty()) {
        final SequenceOffsetType priorOffset = getLastSequenceMetadata().endOffsets.get(partition);

        if (!startOffset.equals(priorOffset)) {
          throw new ISE(
              "New sequence startOffset[%s] does not equal expected prior offset[%s]",
              startOffset,
              priorOffset
          );
        }
      }
    }

    if (!isEndOffsetExclusive() && !sequences.isEmpty()) {
      final SequenceMetadata<PartitionIdType, SequenceOffsetType> lastMetadata = getLastSequenceMetadata();
      if (!lastMetadata.endOffsets.keySet().equals(sequenceMetadata.getExclusiveStartPartitions())) {
        throw new ISE(
            "Exclusive start partitions[%s] for new sequence don't match to the prior offset[%s]",
            sequenceMetadata.getExclusiveStartPartitions(),
            lastMetadata
        );
      }
    }

    // Actually do the add.
    sequences.add(sequenceMetadata);
  }

  @VisibleForTesting
  public SequenceMetadata<PartitionIdType, SequenceOffsetType> getLastSequenceMetadata()
  {
    Preconditions.checkState(!sequences.isEmpty(), "Empty sequences");
    return sequences.get(sequences.size() - 1);
  }

  /**
   * Returns true if the given record has already been read, based on lastReadOffsets.
   */
  private boolean isRecordAlreadyRead(
      final PartitionIdType recordPartition,
      final SequenceOffsetType recordSequenceNumber
  )
  {
    final SequenceOffsetType lastReadOffset = lastReadOffsets.get(recordPartition);

    if (lastReadOffset == null) {
      return false;
    } else {
      return createSequenceNumber(recordSequenceNumber).compareTo(createSequenceNumber(lastReadOffset)) <= 0;
    }
  }

  /**
   * Returns true if, given that we want to start reading from recordSequenceNumber and end at endSequenceNumber, there
   * is more left to read. Used in pre-read checks to determine if there is anything left to read.
   */
  private boolean isMoreToReadBeforeReadingRecord(
      final SequenceOffsetType recordSequenceNumber,
      final SequenceOffsetType endSequenceNumber
  )
  {
    return createSequenceNumber(recordSequenceNumber).isMoreToReadBeforeReadingRecord(
        createSequenceNumber(endSequenceNumber),
        isEndOffsetExclusive()
    );
  }

  /**
   * Returns true if, given that recordSequenceNumber has already been read and we want to end at endSequenceNumber,
   * there is more left to read. Used in post-read checks to determine if there is anything left to read.
   */
  private boolean isMoreToReadAfterReadingRecord(
      final SequenceOffsetType recordSequenceNumber,
      final SequenceOffsetType endSequenceNumber
  )
  {
    final int compareNextToEnd = createSequenceNumber(getNextStartOffset(recordSequenceNumber))
        .compareTo(createSequenceNumber(endSequenceNumber));

    // Unlike isMoreToReadBeforeReadingRecord, we don't care if the end is exclusive or not. If we read it, we're done.
    return compareNextToEnd < 0;
  }

  private void seekToStartingSequence(
      RecordSupplier<PartitionIdType, SequenceOffsetType, RecordType> recordSupplier,
      Set<StreamPartition<PartitionIdType>> partitions
  ) throws InterruptedException
  {
    for (final StreamPartition<PartitionIdType> partition : partitions) {
      final SequenceOffsetType sequence = currOffsets.get(partition.getPartitionId());
      log.info("Seeking partition[%s] to[%s].", partition.getPartitionId(), sequence);
      recordSupplier.seek(partition, sequence);
    }
  }

  /**
   * Checks if the pauseRequested flag was set and if so blocks:
   * a) if pauseMillis == PAUSE_FOREVER, until pauseRequested is cleared
   * b) if pauseMillis != PAUSE_FOREVER, until pauseMillis elapses -or- pauseRequested is cleared
   * <p>
   * If pauseMillis is changed while paused, the new pause timeout will be applied. This allows adjustment of the
   * pause timeout (making a timed pause into an indefinite pause and vice versa is valid) without having to resume
   * and ensures that the loop continues to stay paused without ingesting any new events. You will need to signal
   * shouldResume after adjusting pauseMillis for the new value to take effect.
   * <p>
   * Sets paused = true and signals paused so callers can be notified when the pause command has been accepted.
   * <p>
   * Additionally, pauses if all partitions assignments have been read and pauseAfterRead flag is set.
   *
   * @return true if a pause request was handled, false otherwise
   */
  private boolean possiblyPause() throws InterruptedException
  {
    pauseLock.lockInterruptibly();
    try {
      if (pauseRequested) {
        status = Status.PAUSED;
        hasPaused.signalAll();

        long pauseTime = System.currentTimeMillis();
        log.info("Received pause command, pausing ingestion until resumed.");
        while (pauseRequested) {
          shouldResume.await();
        }

        status = Status.READING;
        shouldResume.signalAll();
        log.info("Received resume command, resuming ingestion.");
        task.emitMetric(toolbox.getEmitter(), "ingest/pause/time", System.currentTimeMillis() - pauseTime);
        return true;
      }
    }
    finally {
      pauseLock.unlock();
    }

    return false;
  }

  private boolean isPaused()
  {
    return status == Status.PAUSED;
  }

  private void requestPause()
  {
    pauseRequested = true;
  }


  protected void sendResetRequestAndWait(
      Map<StreamPartition<PartitionIdType>, SequenceOffsetType> outOfRangePartitions,
      TaskToolbox taskToolbox
  ) throws IOException
  {
    Map<PartitionIdType, SequenceOffsetType> partitionOffsetMap = CollectionUtils.mapKeys(
        outOfRangePartitions,
        StreamPartition::getPartitionId
    );

    boolean result = taskToolbox
        .getTaskActionClient()
        .submit(
            new ResetDataSourceMetadataAction(
                task.getDataSource(),
                createDataSourceMetadata(
                    new SeekableStreamEndSequenceNumbers<>(
                        ioConfig.getStartSequenceNumbers().getStream(),
                        partitionOffsetMap
                    )
                )
            )
        );

    if (result) {
      log.makeAlert("Offsets were reset automatically, potential data duplication or loss")
         .addData("task", task.getId())
         .addData("dataSource", task.getDataSource())
         .addData("partitions", partitionOffsetMap.keySet())
         .emit();

      requestPause();
    } else {
      log.makeAlert("Failed to send offset reset request")
         .addData("task", task.getId())
         .addData("dataSource", task.getDataSource())
         .addData("partitions", ImmutableSet.copyOf(partitionOffsetMap.keySet()))
         .emit();
    }
  }

  /**
   * Authorizes action to be performed on this task's datasource
   *
   * @return authorization result
   */
  private Access authorizationCheck(final HttpServletRequest req, Action action)
  {
    return IndexTaskUtils.datasourceAuthorizationCheck(req, action, task.getDataSource(), authorizerMapper);
  }

  public Appenderator getAppenderator()
  {
    return appenderator;
  }

  @VisibleForTesting
  public RowIngestionMeters getRowIngestionMeters()
  {
    return rowIngestionMeters;
  }

  @VisibleForTesting
  public FireDepartmentMetrics getFireDepartmentMetrics()
  {
    return fireDepartmentMetrics;
  }

  public void stopForcefully()
  {
    log.info("Stopping forcefully (status: [%s])", status);
    stopRequested.set(true);
    // Interrupt if the task has started to run
    if (runThread != null) {
      runThread.interrupt();
    }
  }

  public void stopGracefully()
  {
    log.info("Stopping gracefully (status: [%s])", status);
    stopRequested.set(true);

    synchronized (statusLock) {
      if (status == Status.PUBLISHING) {
        runThread.interrupt();
        return;
      }
    }

    try {
      if (pauseLock.tryLock(SeekableStreamIndexTask.LOCK_ACQUIRE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        try {
          if (pauseRequested) {
            pauseRequested = false;
            shouldResume.signalAll();
          }
        }
        finally {
          pauseLock.unlock();
        }
      } else {
        log.warn("While stopping: failed to acquire pauseLock before timeout, interrupting run thread");
        runThread.interrupt();
        return;
      }

      if (pollRetryLock.tryLock(SeekableStreamIndexTask.LOCK_ACQUIRE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        try {
          isAwaitingRetry.signalAll();
        }
        finally {
          pollRetryLock.unlock();
        }
      } else {
        log.warn("While stopping: failed to acquire pollRetryLock before timeout, interrupting run thread");
        runThread.interrupt();
      }
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @POST
  @Path("/stop")
  public Response stop(@Context final HttpServletRequest req)
  {
    authorizationCheck(req, Action.WRITE);
    stopGracefully();
    return Response.status(Response.Status.OK).build();
  }

  @GET
  @Path("/status")
  @Produces(MediaType.APPLICATION_JSON)
  public Status getStatusHTTP(@Context final HttpServletRequest req)
  {
    authorizationCheck(req, Action.READ);
    return status;
  }

  @VisibleForTesting
  public Status getStatus()
  {
    return status;
  }

  @GET
  @Path("/offsets/current")
  @Produces(MediaType.APPLICATION_JSON)
  public Map<PartitionIdType, SequenceOffsetType> getCurrentOffsets(@Context final HttpServletRequest req)
  {
    authorizationCheck(req, Action.READ);
    return getCurrentOffsets();
  }

  public ConcurrentMap<PartitionIdType, SequenceOffsetType> getCurrentOffsets()
  {
    return currOffsets;
  }

  @GET
  @Path("/offsets/end")
  @Produces(MediaType.APPLICATION_JSON)
  public Map<PartitionIdType, SequenceOffsetType> getEndOffsetsHTTP(@Context final HttpServletRequest req)
  {
    authorizationCheck(req, Action.READ);
    return getEndOffsets();
  }

  public Map<PartitionIdType, SequenceOffsetType> getEndOffsets()
  {
    return endOffsets;
  }

  @POST
  @Path("/offsets/end")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response setEndOffsetsHTTP(
      Map<PartitionIdType, SequenceOffsetType> sequences,
      @QueryParam("finish") @DefaultValue("true") final boolean finish,
      // this field is only for internal purposes, shouldn't be usually set by users
      @Context final HttpServletRequest req
  ) throws InterruptedException
  {
    authorizationCheck(req, Action.WRITE);
    return setEndOffsets(sequences, finish);
  }

  @POST
  @Path("/pendingSegmentVersion")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response registerNewVersionOfPendingSegment(
      PendingSegmentVersions pendingSegmentVersions,
      // this field is only for internal purposes, shouldn't be usually set by users
      @Context final HttpServletRequest req
  )
  {
    authorizationCheck(req, Action.WRITE);
    try {
      ((StreamAppenderator) appenderator).registerNewVersionOfPendingSegment(
          pendingSegmentVersions.getBaseSegment(),
          pendingSegmentVersions.getNewVersion()
      );
      return Response.ok().build();
    }
    catch (DruidException e) {
      return Response
          .status(e.getStatusCode())
          .entity(new ErrorResponse(e))
          .build();
    }
    catch (Exception e) {
      log.error(
          e,
          "Could not register new version[%s] of pending segment[%s]",
          pendingSegmentVersions.getNewVersion(), pendingSegmentVersions.getBaseSegment()
      );
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  public Map<String, Object> doGetRowStats()
  {
    Map<String, Object> returnMap = new HashMap<>();
    Map<String, Object> totalsMap = new HashMap<>();
    Map<String, Object> averagesMap = new HashMap<>();

    totalsMap.put(
        RowIngestionMeters.BUILD_SEGMENTS,
        rowIngestionMeters.getTotals()
    );
    averagesMap.put(
        RowIngestionMeters.BUILD_SEGMENTS,
        rowIngestionMeters.getMovingAverages()
    );

    returnMap.put("movingAverages", averagesMap);
    returnMap.put("totals", totalsMap);
    return returnMap;
  }

  public Map<String, Object> doGetLiveReports()
  {
    Map<String, Object> returnMap = new HashMap<>();
    Map<String, Object> ingestionStatsAndErrors = new HashMap<>();
    Map<String, Object> payload = new HashMap<>();
    Map<String, Object> events = getTaskCompletionUnparseableEvents();

    payload.put("ingestionState", ingestionState);
    payload.put("unparseableEvents", events);
    payload.put("rowStats", doGetRowStats());

    ingestionStatsAndErrors.put("taskId", task.getId());
    ingestionStatsAndErrors.put("payload", payload);
    ingestionStatsAndErrors.put("type", "ingestionStatsAndErrors");

    returnMap.put("ingestionStatsAndErrors", ingestionStatsAndErrors);
    return returnMap;
  }

  @GET
  @Path("/rowStats")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getRowStats(
      @Context final HttpServletRequest req
  )
  {
    authorizationCheck(req, Action.READ);
    return Response.ok(doGetRowStats()).build();
  }

  @GET
  @Path("/liveReports")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getLiveReport(
      @Context final HttpServletRequest req
  )
  {
    authorizationCheck(req, Action.READ);
    return Response.ok(doGetLiveReports()).build();
  }


  @GET
  @Path("/unparseableEvents")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getUnparseableEvents(
      @Context final HttpServletRequest req
  )
  {
    authorizationCheck(req, Action.READ);
    List<ParseExceptionReport> events = IndexTaskUtils.getReportListFromSavedParseExceptions(
        parseExceptionHandler.getSavedParseExceptionReports()
    );
    return Response.ok(events).build();
  }

  @VisibleForTesting
  public Response setEndOffsets(
      Map<PartitionIdType, SequenceOffsetType> sequenceNumbers,
      boolean finish // this field is only for internal purposes, shouldn't be usually set by users
  ) throws InterruptedException
  {
    if (sequenceNumbers == null) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity("Request body must contain a map of { partition:endOffset }")
                     .build();
    } else if (!endOffsets.keySet().containsAll(sequenceNumbers.keySet())) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(
                         StringUtils.format(
                             "Request contains partitions not being handled by this task, my partitions: %s",
                             endOffsets.keySet()
                         )
                     )
                     .build();
    } else {
      try {
        pauseLock.lockInterruptibly();
        // Perform all sequence related checks before checking for isPaused()
        // and after acquiring pauseLock to correctly guard against duplicate requests
        Preconditions.checkState(sequenceNumbers.size() > 0, "No sequences found to set end sequences");

        final SequenceMetadata<PartitionIdType, SequenceOffsetType> latestSequence = getLastSequenceMetadata();
        final Set<PartitionIdType> exclusiveStartPartitions;

        if (isEndOffsetExclusive()) {
          // When end offsets are exclusive, there's no need for marking the next sequence as having any
          // exclusive-start partitions. It should always start from the end offsets of the prior sequence.
          exclusiveStartPartitions = Collections.emptySet();
        } else {
          // When end offsets are inclusive, we must mark all partitions as exclusive-start, to avoid reading
          // their final messages (which have already been read).
          exclusiveStartPartitions = sequenceNumbers.keySet();
        }

        if ((latestSequence.getStartOffsets().equals(sequenceNumbers)
             && latestSequence.getExclusiveStartPartitions().equals(exclusiveStartPartitions)
             && !finish)
            || (latestSequence.getEndOffsets().equals(sequenceNumbers) && finish)) {
          log.warn("Ignoring duplicate request, end offsets already set for sequences [%s]", sequenceNumbers);
          resetNextCheckpointTime();
          resume();
          return Response.ok(sequenceNumbers).build();
        } else if (latestSequence.isCheckpointed()) {
          return Response.status(Response.Status.BAD_REQUEST)
                         .type(MediaType.TEXT_PLAIN)
                         .entity(StringUtils.format(
                             "Sequence [%s] has already endOffsets set, cannot set to [%s]",
                             latestSequence,
                             sequenceNumbers
                         )).build();
        } else if (!isPaused()) {
          return Response.status(Response.Status.BAD_REQUEST)
                         .entity("Task must be paused before changing the end offsets")
                         .build();
        }

        for (Map.Entry<PartitionIdType, SequenceOffsetType> entry : sequenceNumbers.entrySet()) {
          if (createSequenceNumber(entry.getValue()).compareTo(createSequenceNumber(currOffsets.get(entry.getKey())))
              < 0) {
            return Response.status(Response.Status.BAD_REQUEST)
                           .entity(
                               StringUtils.format(
                                   "End sequence must be >= current sequence for partition [%s] (current: %s)",
                                   entry.getKey(),
                                   currOffsets.get(entry.getKey())
                               )
                           )
                           .build();
          }
        }

        resetNextCheckpointTime();
        latestSequence.setEndOffsets(sequenceNumbers);

        if (finish) {
          log.info(
              "Sequence[%s] end offsets updated from [%s] to [%s].",
              latestSequence.getSequenceName(),
              endOffsets,
              sequenceNumbers
          );
          endOffsets.putAll(sequenceNumbers);
        } else {
          // create new sequence
          final SequenceMetadata<PartitionIdType, SequenceOffsetType> newSequence = new SequenceMetadata<>(
              latestSequence.getSequenceId() + 1,
              StringUtils.format("%s_%d", ioConfig.getBaseSequenceName(), latestSequence.getSequenceId() + 1),
              sequenceNumbers,
              endOffsets,
              false,
              exclusiveStartPartitions,
              getTaskLockType()
          );

          log.info(
              "Sequence[%s] created with start offsets [%s] and end offsets [%s].",
              newSequence.getSequenceName(),
              sequenceNumbers,
              endOffsets
          );

          addSequence(newSequence);
        }
        persistSequences();
      }
      catch (Exception e) {
        log.error(e, "Failed to set end offsets.");
        backgroundThreadException = e;
        // should resume to immediately finish kafka index task as failed
        resume();
        return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                       .entity(Throwables.getStackTraceAsString(e))
                       .build();
      }
      finally {
        pauseLock.unlock();
      }
    }

    resume();

    return Response.ok(sequenceNumbers).build();
  }

  private void resetNextCheckpointTime()
  {
    nextCheckpointTime = DateTimes.nowUtc().plus(tuningConfig.getIntermediateHandoffPeriod()).getMillis();
  }

  @VisibleForTesting
  public CopyOnWriteArrayList<SequenceMetadata<PartitionIdType, SequenceOffsetType>> getSequences()
  {
    return sequences;
  }

  @GET
  @Path("/checkpoints")
  @Produces(MediaType.APPLICATION_JSON)
  public Map<Integer, Map<PartitionIdType, SequenceOffsetType>> getCheckpointsHTTP(
      @Context final HttpServletRequest req
  )
  {
    authorizationCheck(req, Action.READ);
    return getCheckpoints();
  }

  private Map<Integer, Map<PartitionIdType, SequenceOffsetType>> getCheckpoints()
  {
    return new TreeMap<>(sequences.stream()
                                  .collect(Collectors.toMap(
                                      SequenceMetadata::getSequenceId,
                                      SequenceMetadata::getStartOffsets
                                  )));
  }

  /**
   * Signals the ingestion loop to pause.
   *
   * @return one of the following Responses: 409 Bad Request if the task has started publishing; 202 Accepted if the
   * method has timed out and returned before the task has paused; 200 OK with a map of the current partition sequences
   * in the response body if the task successfully paused
   */
  @POST
  @Path("/pause")
  @Produces(MediaType.APPLICATION_JSON)
  public Response pauseHTTP(
      @Context final HttpServletRequest req
  ) throws InterruptedException
  {
    authorizationCheck(req, Action.WRITE);
    return pause();
  }

  @VisibleForTesting
  public Response pause() throws InterruptedException
  {
    if (!(status == Status.PAUSED || status == Status.READING)) {
      return Response.status(Response.Status.CONFLICT)
                     .type(MediaType.TEXT_PLAIN)
                     .entity(StringUtils.format("Can't pause, task is not in a pausable state (state: [%s])", status))
                     .build();
    }

    pauseLock.lockInterruptibly();
    try {
      pauseRequested = true;

      pollRetryLock.lockInterruptibly();
      try {
        isAwaitingRetry.signalAll();
      }
      finally {
        pollRetryLock.unlock();
      }

      if (isPaused()) {
        shouldResume.signalAll(); // kick the monitor so it re-awaits with the new pauseMillis
      }

      long nanos = TimeUnit.SECONDS.toNanos(2);
      while (!isPaused()) {
        if (nanos <= 0L) {
          return Response.status(Response.Status.ACCEPTED)
                         .entity("Request accepted but task has not yet paused")
                         .build();
        }
        nanos = hasPaused.awaitNanos(nanos);
      }
    }
    finally {
      pauseLock.unlock();
    }

    try {
      return Response.ok().entity(toolbox.getJsonMapper().writeValueAsString(getCurrentOffsets())).build();
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @POST
  @Path("/resume")
  public Response resumeHTTP(@Context final HttpServletRequest req) throws InterruptedException
  {
    authorizationCheck(req, Action.WRITE);
    resume();
    return Response.status(Response.Status.OK).build();
  }


  @VisibleForTesting
  public void resume() throws InterruptedException
  {
    pauseLock.lockInterruptibly();
    try {
      pauseRequested = false;
      shouldResume.signalAll();

      long nanos = TimeUnit.SECONDS.toNanos(5);
      while (isPaused()) {
        if (nanos <= 0L) {
          throw new RuntimeException("Resume command was not accepted within 5 seconds");
        }
        nanos = shouldResume.awaitNanos(nanos);
      }
    }
    finally {
      pauseLock.unlock();
    }
  }


  @GET
  @Path("/time/start")
  @Produces(MediaType.APPLICATION_JSON)
  public DateTime getStartTime(@Context final HttpServletRequest req)
  {
    authorizationCheck(req, Action.WRITE);
    return startTime;
  }

  @VisibleForTesting
  public long getNextCheckpointTime()
  {
    return nextCheckpointTime;
  }

  /**
   * This method does two things:
   * <p>
   * 1) Verifies that the sequence numbers we read are at least as high as those read previously, and throws an
   * exception if not.
   * 2) Returns false if we should skip this record because it's either (a) the first record in a partition that we are
   * needing to be exclusive on; (b) too late to read, past the endOffsets.
   */
  private boolean verifyRecordInRange(
      final PartitionIdType partition,
      final SequenceOffsetType recordOffset
  )
  {
    // Verify that the record is at least as high as its currOffset.
    final SequenceOffsetType currOffset = Preconditions.checkNotNull(
        currOffsets.get(partition),
        "Current offset is null for partition[%s]",
        partition
    );

    final OrderedSequenceNumber<SequenceOffsetType> recordSequenceNumber = createSequenceNumber(recordOffset);
    final OrderedSequenceNumber<SequenceOffsetType> currentSequenceNumber = createSequenceNumber(currOffset);

    final int comparisonToCurrent = recordSequenceNumber.compareTo(currentSequenceNumber);
    if (comparisonToCurrent < 0) {
      throw new ISE(
          "Record sequenceNumber[%s] is smaller than current sequenceNumber[%s] for partition[%s]",
          recordOffset,
          currOffset,
          partition
      );
    }

    // Check if the record has already been read.
    if (isRecordAlreadyRead(partition, recordOffset)) {
      return false;
    }

    // Finally, check if this record comes before the endOffsets for this partition.
    return isMoreToReadBeforeReadingRecord(recordSequenceNumber.get(), endOffsets.get(partition));
  }

  /**
   * checks if the input seqNum marks end of shard. Used by Kinesis only
   */
  protected abstract boolean isEndOfShard(SequenceOffsetType seqNum);

  /**
   * deserializes the checkpoints into of Map<sequenceId, Map<PartitionIdType, SequenceOffsetType>>
   *
   * @param toolbox           task toolbox
   * @param checkpointsString the json-serialized checkpoint string
   *
   * @return checkpoint
   *
   * @throws IOException jsonProcessingException
   */
  @Nullable
  protected abstract TreeMap<Integer, Map<PartitionIdType, SequenceOffsetType>> getCheckPointsFromContext(
      TaskToolbox toolbox,
      String checkpointsString
  ) throws IOException;

  /**
   * Calculates the sequence number used to update currOffsets after finished reading a record.
   * This is what would become the start offsets of the next reader, if we stopped reading now.
   *
   * @param sequenceNumber the sequence number that has already been processed
   *
   * @return next sequence number to be stored
   */
  protected abstract SequenceOffsetType getNextStartOffset(SequenceOffsetType sequenceNumber);

  /**
   * deserializes stored metadata into SeekableStreamStartSequenceNumbers
   *
   * @param mapper json objectMapper
   * @param object metadata
   *
   * @return SeekableStreamEndSequenceNumbers
   */
  protected abstract SeekableStreamEndSequenceNumbers<PartitionIdType, SequenceOffsetType> deserializePartitionsFromMetadata(
      ObjectMapper mapper,
      Object object
  );

  /**
   * polls the next set of records from the recordSupplier, the main purpose of having a separate method here
   * is to catch and handle exceptions specific to Kafka/Kinesis
   *
   * @param recordSupplier
   * @param toolbox
   *
   * @return list of records polled, can be empty but cannot be null
   *
   * @throws Exception
   */
  @NotNull
  protected abstract List<OrderedPartitionableRecord<PartitionIdType, SequenceOffsetType, RecordType>> getRecords(
      RecordSupplier<PartitionIdType, SequenceOffsetType, RecordType> recordSupplier,
      TaskToolbox toolbox
  ) throws Exception;

  /**
   * creates specific implementations of kafka/kinesis datasource metadata
   *
   * @param partitions partitions used to create the datasource metadata
   *
   * @return datasource metadata
   */
  protected abstract SeekableStreamDataSourceMetadata<PartitionIdType, SequenceOffsetType> createDataSourceMetadata(
      SeekableStreamSequenceNumbers<PartitionIdType, SequenceOffsetType> partitions
  );

  /**
   * create a specific implementation of Kafka/Kinesis sequence number/offset used for comparison mostly
   *
   * @param sequenceNumber
   *
   * @return a specific OrderedSequenceNumber instance for Kafka/Kinesis
   */
  protected abstract OrderedSequenceNumber<SequenceOffsetType> createSequenceNumber(SequenceOffsetType sequenceNumber);

  /**
   * check if the sequence offsets stored in currOffsets are still valid sequence offsets compared to
   * earliest sequence offsets fetched from stream
   */
  protected abstract void possiblyResetDataSourceMetadata(
      TaskToolbox toolbox,
      RecordSupplier<PartitionIdType, SequenceOffsetType, RecordType> recordSupplier,
      Set<StreamPartition<PartitionIdType>> assignment
  );

  /**
   * In Kafka, the endOffsets are exclusive, so skip it.
   * In Kinesis the endOffsets are inclusive
   */
  protected abstract boolean isEndOffsetExclusive();

  protected abstract TypeReference<List<SequenceMetadata<PartitionIdType, SequenceOffsetType>>> getSequenceMetadataTypeReference();
}
