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


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.data.input.Committer;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.LookupNodeService;
import org.apache.druid.discovery.NodeType;
import org.apache.druid.indexer.IngestionState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.IngestionStatsAndErrorsTaskReport;
import org.apache.druid.indexing.common.IngestionStatsAndErrorsTaskReportData;
import org.apache.druid.indexing.common.TaskRealtimeMetricsMonitorBuilder;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.CheckPointDataSourceMetadataAction;
import org.apache.druid.indexing.common.actions.ResetDataSourceMetadataAction;
import org.apache.druid.indexing.common.actions.SegmentTransactionalInsertAction;
import org.apache.druid.indexing.common.stats.RowIngestionMeters;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.IndexTaskUtils;
import org.apache.druid.indexing.common.task.RealtimeIndexTask;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.OrderedSequenceNumber;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.collect.Utils;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.indexing.RealtimeIOConfig;
import org.apache.druid.segment.realtime.FireDepartment;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;
import org.apache.druid.segment.realtime.appenderator.Appenderator;
import org.apache.druid.segment.realtime.appenderator.AppenderatorDriverAddResult;
import org.apache.druid.segment.realtime.appenderator.SegmentsAndMetadata;
import org.apache.druid.segment.realtime.appenderator.StreamAppenderatorDriver;
import org.apache.druid.segment.realtime.appenderator.TransactionalSegmentPublisher;
import org.apache.druid.segment.realtime.firehose.ChatHandler;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.utils.CircularBuffer;
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
public abstract class SeekableStreamIndexTaskRunner<PartitionIdType, SequenceOffsetType> implements ChatHandler
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
  private static final String METADATA_NEXT_PARTITIONS = "nextPartitions";
  private static final String METADATA_PUBLISH_PARTITIONS = "publishPartitions";

  private final Map<PartitionIdType, SequenceOffsetType> endOffsets;
  private final Map<PartitionIdType, SequenceOffsetType> currOffsets = new ConcurrentHashMap<>();
  private final Map<PartitionIdType, SequenceOffsetType> lastPersistedOffsets = new ConcurrentHashMap<>();

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

  private final SeekableStreamIndexTask<PartitionIdType, SequenceOffsetType> task;
  private final SeekableStreamIndexTaskIOConfig<PartitionIdType, SequenceOffsetType> ioConfig;
  private final SeekableStreamIndexTaskTuningConfig tuningConfig;
  private final InputRowParser<ByteBuffer> parser;
  private final AuthorizerMapper authorizerMapper;
  private final Optional<ChatHandlerProvider> chatHandlerProvider;
  private final CircularBuffer<Throwable> savedParseExceptions;
  private final String stream;
  private final RowIngestionMeters rowIngestionMeters;

  private final Set<String> publishingSequences = Sets.newConcurrentHashSet();
  private final List<ListenableFuture<SegmentsAndMetadata>> publishWaitList = new ArrayList<>();
  private final List<ListenableFuture<SegmentsAndMetadata>> handOffWaitList = new ArrayList<>();
  private final Map<PartitionIdType, SequenceOffsetType> initialOffsetsSnapshot = new HashMap<>();
  private final Set<PartitionIdType> exclusiveStartingPartitions = new HashSet<>();

  private volatile DateTime startTime;
  private volatile Status status = Status.NOT_STARTED; // this is only ever set by the task runner thread (runThread)
  private volatile TaskToolbox toolbox;
  private volatile Thread runThread;
  private volatile Appenderator appenderator;
  private volatile StreamAppenderatorDriver driver;
  private volatile IngestionState ingestionState;

  protected volatile boolean pauseRequested = false;
  private volatile long nextCheckpointTime;

  private volatile CopyOnWriteArrayList<SequenceMetadata> sequences;
  private volatile Throwable backgroundThreadException;

  public SeekableStreamIndexTaskRunner(
      final SeekableStreamIndexTask<PartitionIdType, SequenceOffsetType> task,
      final InputRowParser<ByteBuffer> parser,
      final AuthorizerMapper authorizerMapper,
      final Optional<ChatHandlerProvider> chatHandlerProvider,
      final CircularBuffer<Throwable> savedParseExceptions,
      final RowIngestionMetersFactory rowIngestionMetersFactory
  )
  {
    Preconditions.checkNotNull(task);
    this.task = task;
    this.ioConfig = task.getIOConfig();
    this.tuningConfig = task.getTuningConfig();
    this.parser = parser;
    this.authorizerMapper = authorizerMapper;
    this.chatHandlerProvider = chatHandlerProvider;
    this.savedParseExceptions = savedParseExceptions;
    this.stream = ioConfig.getStartPartitions().getStream();
    this.rowIngestionMeters = rowIngestionMetersFactory.createRowIngestionMeters();
    this.endOffsets = new ConcurrentHashMap<>(ioConfig.getEndPartitions().getPartitionSequenceNumberMap());
    this.sequences = new CopyOnWriteArrayList<>();
    this.ingestionState = IngestionState.NOT_STARTED;

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
      toolbox.getTaskReportFileWriter().write(getTaskCompletionReports(errorMsg));
      return TaskStatus.failure(
          task.getId(),
          errorMsg
      );
    }
  }

  private TaskStatus runInternal(TaskToolbox toolbox) throws Exception
  {
    log.info("SeekableStream indexing task starting up!");
    startTime = DateTimes.nowUtc();
    status = Status.STARTING;
    this.toolbox = toolbox;


    if (!restoreSequences()) {
      final TreeMap<Integer, Map<PartitionIdType, SequenceOffsetType>> checkpoints = getCheckPointsFromContext(
          toolbox,
          task.getContextValue("checkpoints")
      );
      if (checkpoints != null) {
        boolean exclusive = false;
        Iterator<Map.Entry<Integer, Map<PartitionIdType, SequenceOffsetType>>> sequenceOffsets = checkpoints.entrySet()
                                                                                                            .iterator();
        Map.Entry<Integer, Map<PartitionIdType, SequenceOffsetType>> previous = sequenceOffsets.next();
        while (sequenceOffsets.hasNext()) {
          Map.Entry<Integer, Map<PartitionIdType, SequenceOffsetType>> current = sequenceOffsets.next();
          sequences.add(new SequenceMetadata(
              previous.getKey(),
              StringUtils.format("%s_%s", ioConfig.getBaseSequenceName(), previous.getKey()),
              previous.getValue(),
              current.getValue(),
              true,
              exclusive ? previous.getValue().keySet() : null
          ));
          previous = current;
          exclusive = true;
        }
        sequences.add(new SequenceMetadata(
            previous.getKey(),
            StringUtils.format("%s_%s", ioConfig.getBaseSequenceName(), previous.getKey()),
            previous.getValue(),
            endOffsets,
            false,
            exclusive ? previous.getValue().keySet() : null
        ));
      } else {
        sequences.add(new SequenceMetadata(
            0,
            StringUtils.format("%s_%s", ioConfig.getBaseSequenceName(), 0),
            ioConfig.getStartPartitions().getPartitionSequenceNumberMap(),
            endOffsets,
            false,
            null
        ));
      }
    }

    log.info("Starting with sequences:  %s", sequences);

    if (chatHandlerProvider.isPresent()) {
      log.info("Found chat handler of class[%s]", chatHandlerProvider.get().getClass().getName());
      chatHandlerProvider.get().register(task.getId(), this, false);
    } else {
      log.warn("No chat handler detected");
    }

    runThread = Thread.currentThread();

    // Set up FireDepartmentMetrics
    final FireDepartment fireDepartmentForMetrics = new FireDepartment(
        task.getDataSchema(),
        new RealtimeIOConfig(null, null, null),
        null
    );
    FireDepartmentMetrics fireDepartmentMetrics = fireDepartmentForMetrics.getMetrics();
    toolbox.getMonitorScheduler()
           .addMonitor(TaskRealtimeMetricsMonitorBuilder.build(task, fireDepartmentForMetrics, rowIngestionMeters));

    final String lookupTier = task.getContextValue(RealtimeIndexTask.CTX_KEY_LOOKUP_TIER);
    final LookupNodeService lookupNodeService = lookupTier == null ?
                                                toolbox.getLookupNodeService() :
                                                new LookupNodeService(lookupTier);

    final DiscoveryDruidNode discoveryDruidNode = new DiscoveryDruidNode(
        toolbox.getDruidNode(),
        NodeType.PEON,
        ImmutableMap.of(
            toolbox.getDataNodeService().getName(), toolbox.getDataNodeService(),
            lookupNodeService.getName(), lookupNodeService
        )
    );

    Throwable caughtExceptionOuter = null;
    try (final RecordSupplier<PartitionIdType, SequenceOffsetType> recordSupplier = task.newTaskRecordSupplier()) {
      toolbox.getDataSegmentServerAnnouncer().announce();
      toolbox.getDruidNodeAnnouncer().announce(discoveryDruidNode);

      appenderator = task.newAppenderator(fireDepartmentMetrics, toolbox);
      driver = task.newDriver(appenderator, toolbox, fireDepartmentMetrics);

      final String stream = ioConfig.getStartPartitions().getStream();

      // Start up, set up initial sequences.
      final Object restoredMetadata = driver.startJob();
      if (restoredMetadata == null) {
        // no persist has happened so far
        // so either this is a brand new task or replacement of a failed task
        Preconditions.checkState(sequences.get(0).startOffsets.entrySet().stream().allMatch(
            partitionOffsetEntry ->
                createSequenceNumber(partitionOffsetEntry.getValue()).compareTo(
                    createSequenceNumber(ioConfig.getStartPartitions()
                                                 .getPartitionSequenceNumberMap()
                                                 .get(partitionOffsetEntry.getKey())
                    )) >= 0
        ), "Sequence sequences are not compatible with start sequences of task");
        currOffsets.putAll(sequences.get(0).startOffsets);
      } else {
        @SuppressWarnings("unchecked")
        final Map<String, Object> restoredMetadataMap = (Map) restoredMetadata;
        final SeekableStreamPartitions<PartitionIdType, SequenceOffsetType> restoredNextPartitions = deserializeSeekableStreamPartitionsFromMetadata(
            toolbox.getObjectMapper(),
            restoredMetadataMap.get(METADATA_NEXT_PARTITIONS)
        );

        currOffsets.putAll(restoredNextPartitions.getPartitionSequenceNumberMap());

        // Sanity checks.
        if (!restoredNextPartitions.getStream().equals(ioConfig.getStartPartitions().getStream())) {
          throw new ISE(
              "WTF?! Restored stream[%s] but expected stream[%s]",
              restoredNextPartitions.getStream(),
              ioConfig.getStartPartitions().getStream()
          );
        }

        if (!currOffsets.keySet().equals(ioConfig.getStartPartitions().getPartitionSequenceNumberMap().keySet())) {
          throw new ISE(
              "WTF?! Restored partitions[%s] but expected partitions[%s]",
              currOffsets.keySet(),
              ioConfig.getStartPartitions().getPartitionSequenceNumberMap().keySet()
          );
        }
        // sequences size can be 0 only when all sequences got published and task stopped before it could finish
        // which is super rare
        if (sequences.size() == 0 || sequences.get(sequences.size() - 1).isCheckpointed()) {
          this.endOffsets.putAll(sequences.size() == 0
                                 ? currOffsets
                                 : sequences.get(sequences.size() - 1).getEndOffsets());
          log.info("End sequences changed to [%s]", endOffsets);
        }
      }

      // Filter out partitions with END_OF_SHARD markers since these partitions have already been fully read. This
      // should have been done by the supervisor already so this is defensive.
      int numPreFilterPartitions = currOffsets.size();
      if (currOffsets.entrySet().removeIf(x -> isEndOfShard(x.getValue()))) {
        log.info(
            "Removed [%d] partitions from assignment which have already been closed",
            numPreFilterPartitions - currOffsets.size()
        );
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
            return ImmutableMap.of(
                METADATA_NEXT_PARTITIONS, new SeekableStreamPartitions<>(
                    ioConfig.getStartPartitions().getStream(),
                    snapshot
                )
            );
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
      possiblyResetDataSourceMetadata(toolbox, recordSupplier, assignment, currOffsets);
      seekToStartingSequence(recordSupplier, assignment);

      ingestionState = IngestionState.BUILD_SEGMENTS;

      // Main loop.
      // Could eventually support leader/follower mode (for keeping replicas more in sync)
      boolean stillReading = !assignment.isEmpty();
      status = Status.READING;
      Throwable caughtExceptionInner = null;

      initialOffsetsSnapshot.putAll(currOffsets);
      exclusiveStartingPartitions.addAll(ioConfig.getExclusiveStartSequenceNumberPartitions());

      try {
        while (stillReading) {
          if (possiblyPause()) {
            // The partition assignments may have changed while paused by a call to setEndOffsets() so reassign
            // partitions upon resuming. This is safe even if the end sequences have not been modified.
            assignment = assignPartitions(recordSupplier);
            possiblyResetDataSourceMetadata(toolbox, recordSupplier, assignment, currOffsets);
            seekToStartingSequence(recordSupplier, assignment);

            if (assignment.isEmpty()) {
              log.info("All partitions have been fully read");
              publishOnStop.set(true);
              stopRequested.set(true);
            }
          }

          // if stop is requested or task's end sequence is set by call to setEndOffsets method with finish set to true
          if (stopRequested.get() || sequences.get(sequences.size() - 1).isCheckpointed()) {
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
          List<OrderedPartitionableRecord<PartitionIdType, SequenceOffsetType>> records = getRecords(
              recordSupplier,
              toolbox
          );

          // note: getRecords() also updates assignment
          stillReading = !assignment.isEmpty();

          SequenceMetadata sequenceToCheckpoint = null;
          for (OrderedPartitionableRecord<PartitionIdType, SequenceOffsetType> record : records) {

            // for Kafka, the end offsets are exclusive, so skip it
            if (isEndSequenceOffsetsExclusive() &&
                createSequenceNumber(record.getSequenceNumber()).compareTo(
                    createSequenceNumber(endOffsets.get(record.getPartitionId()))) == 0) {
              continue;
            }

            // for the first message we receive, check that we were given a message with a sequenceNumber that matches our
            // expected starting sequenceNumber
            if (!verifyInitialRecordAndSkipExclusivePartition(record, initialOffsetsSnapshot)) {
              continue;
            }

            log.trace(
                "Got stream[%s] partition[%s] sequence[%s].",
                record.getStream(),
                record.getPartitionId(),
                record.getSequenceNumber()
            );

            if (isEndOfShard(record.getSequenceNumber())) {
              // shard is closed, applies to Kinesis only
              currOffsets.put(record.getPartitionId(), record.getSequenceNumber());
            } else if (createSequenceNumber(record.getSequenceNumber()).compareTo(
                createSequenceNumber(endOffsets.get(record.getPartitionId()))) <= 0) {


              if (!record.getSequenceNumber().equals(currOffsets.get(record.getPartitionId()))
                  && !ioConfig.isSkipOffsetGaps()) {
                throw new ISE(
                    "WTF?! Got sequence[%s] after sequence[%s] in partition[%s].",
                    record.getSequenceNumber(),
                    currOffsets.get(record.getPartitionId()),
                    record.getPartitionId()
                );
              }

              try {
                final List<byte[]> valueBytess = record.getData();
                final List<InputRow> rows;
                if (valueBytess == null || valueBytess.isEmpty()) {
                  rows = Utils.nullableListOf((InputRow) null);
                } else {
                  rows = new ArrayList<>();
                  for (byte[] valueBytes : valueBytess) {
                    rows.addAll(parser.parseBatch(ByteBuffer.wrap(valueBytes)));
                  }
                }
                boolean isPersistRequired = false;

                final SequenceMetadata sequenceToUse = sequences
                    .stream()
                    .filter(sequenceMetadata -> sequenceMetadata.canHandle(record))
                    .findFirst()
                    .orElse(null);

                if (sequenceToUse == null) {
                  throw new ISE(
                      "WTH?! cannot find any valid sequence for record with partition [%d] and sequence [%d]. Current sequences: %s",
                      record.getPartitionId(),
                      record.getSequenceNumber(),
                      sequences
                  );
                }

                for (InputRow row : rows) {
                  if (row != null && task.withinMinMaxRecordTime(row)) {
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
                      if (addResult.isPushRequired(tuningConfig) && !sequenceToUse.isCheckpointed()) {
                        sequenceToCheckpoint = sequenceToUse;
                      }
                      isPersistRequired |= addResult.isPersistRequired();
                    } else {
                      // Failure to allocate segment puts determinism at risk, bail out to be safe.
                      // May want configurable behavior here at some point.
                      // If we allow continuing, then consider blacklisting the interval for a while to avoid constant checks.
                      throw new ISE("Could not allocate segment for row with timestamp[%s]", row.getTimestamp());
                    }

                    if (addResult.getParseException() != null) {
                      handleParseException(addResult.getParseException(), record);
                    } else {
                      rowIngestionMeters.incrementProcessed();
                    }
                  } else {
                    rowIngestionMeters.incrementThrownAway();
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
                          log.info("Persist completed with metadata [%s]", result);
                        }

                        @Override
                        public void onFailure(Throwable t)
                        {
                          log.error("Persist failed, dying");
                          backgroundThreadException = t;
                        }
                      }
                  );
                }
              }
              catch (ParseException e) {
                handleParseException(e, record);
              }

              // in kafka, we can easily get the next offset by adding 1, but for kinesis, there's no way
              // to get the next sequence number without having to make an expensive api call. So the behavior
              // here for kafka is to +1 while for kinesis we simply save the current sequence number
              currOffsets.put(record.getPartitionId(), getSequenceNumberToStoreAfterRead(record.getSequenceNumber()));
            }

            if ((currOffsets.get(record.getPartitionId()).equals(endOffsets.get(record.getPartitionId()))
                 || isEndOfShard(currOffsets.get(record.getPartitionId())))
                && assignment.remove(record.getStreamPartition())) {
              log.info("Finished reading stream[%s], partition[%s].", record.getStream(), record.getPartitionId());
              recordSupplier.assign(assignment);
              stillReading = !assignment.isEmpty();
            }
          }

          if (System.currentTimeMillis() > nextCheckpointTime) {
            sequenceToCheckpoint = sequences.get(sequences.size() - 1);
          }

          if (sequenceToCheckpoint != null && stillReading) {
            Preconditions.checkArgument(
                sequences.get(sequences.size() - 1)
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
                task.getIOConfig().getBaseSequenceName(),
                createDataSourceMetadata(new SeekableStreamPartitions<>(
                    stream,
                    sequenceToCheckpoint.getStartOffsets()
                )),
                createDataSourceMetadata(new SeekableStreamPartitions<>(stream, currOffsets))
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
        log.info("Persisting all pending data");
        try {
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

      for (SequenceMetadata sequenceMetadata : sequences) {
        if (!publishingSequences.contains(sequenceMetadata.getSequenceName())) {
          // this is done to prevent checks in sequence specific commit supplier from failing
          sequenceMetadata.setEndOffsets(currOffsets);
          sequenceMetadata.updateAssignments(currOffsets);
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
      List<SegmentsAndMetadata> handedOffList = Collections.emptyList();
      if (tuningConfig.getHandoffConditionTimeout() == 0) {
        handedOffList = Futures.allAsList(handOffWaitList).get();
      } else {
        try {
          handedOffList = Futures.allAsList(handOffWaitList)
                                 .get(tuningConfig.getHandoffConditionTimeout(), TimeUnit.MILLISECONDS);
        }
        catch (TimeoutException e) {
          // Handoff timeout is not an indexing failure, but coordination failure. We simply ignore timeout exception
          // here.
          log.makeAlert("Timed out after [%d] millis waiting for handoffs", tuningConfig.getHandoffConditionTimeout())
             .addData("TaskId", task.getId())
             .emit();
        }
      }

      for (SegmentsAndMetadata handedOff : handedOffList) {
        log.info(
            "Handoff completed for segments %s with metadata[%s].",
            Lists.transform(handedOff.getSegments(), DataSegment::getId),
            Preconditions.checkNotNull(handedOff.getCommitMetadata(), "commitMetadata")
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

      log.info("The task was asked to stop before completing");
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
        if (chatHandlerProvider.isPresent()) {
          chatHandlerProvider.get().unregister(task.getId());
        }

        toolbox.getDruidNodeAnnouncer().unannounce(discoveryDruidNode);
        toolbox.getDataSegmentServerAnnouncer().unannounce();
      }
      catch (Exception e) {
        if (caughtExceptionOuter != null) {
          caughtExceptionOuter.addSuppressed(e);
        } else {
          throw e;
        }
      }
    }

    toolbox.getTaskReportFileWriter().write(getTaskCompletionReports(null));
    return TaskStatus.success(task.getId());
  }

  /**
   * checks if the input seqNum marks end of shard. Used by Kinesis only
   */
  protected abstract boolean isEndOfShard(SequenceOffsetType seqNum);

  private void checkPublishAndHandoffFailure() throws ExecutionException, InterruptedException
  {
    // Check if any publishFuture failed.
    final List<ListenableFuture<SegmentsAndMetadata>> publishFinished = publishWaitList
        .stream()
        .filter(Future::isDone)
        .collect(Collectors.toList());

    for (ListenableFuture<SegmentsAndMetadata> publishFuture : publishFinished) {
      // If publishFuture failed, the below line will throw an exception and catched by (1), and then (2) or (3).
      publishFuture.get();
    }

    publishWaitList.removeAll(publishFinished);

    // Check if any handoffFuture failed.
    final List<ListenableFuture<SegmentsAndMetadata>> handoffFinished = handOffWaitList
        .stream()
        .filter(Future::isDone)
        .collect(Collectors.toList());

    for (ListenableFuture<SegmentsAndMetadata> handoffFuture : handoffFinished) {
      // If handoffFuture failed, the below line will throw an exception and catched by (1), and then (2) or (3).
      handoffFuture.get();
    }

    handOffWaitList.removeAll(handoffFinished);
  }

  private void publishAndRegisterHandoff(SequenceMetadata sequenceMetadata)
  {
    log.info("Publishing segments for sequence [%s]", sequenceMetadata);

    final ListenableFuture<SegmentsAndMetadata> publishFuture = Futures.transform(
        driver.publish(
            sequenceMetadata.createPublisher(toolbox, ioConfig.isUseTransaction()),
            sequenceMetadata.getCommitterSupplier(stream, lastPersistedOffsets).get(),
            Collections.singletonList(sequenceMetadata.getSequenceName())
        ),
        (Function<SegmentsAndMetadata, SegmentsAndMetadata>) publishedSegmentsAndMetadata -> {
          if (publishedSegmentsAndMetadata == null) {
            throw new ISE(
                "Transaction failure publishing segments for sequence [%s]",
                sequenceMetadata
            );
          } else {
            return publishedSegmentsAndMetadata;
          }
        }
    );
    publishWaitList.add(publishFuture);

    // Create a handoffFuture for every publishFuture. The created handoffFuture must fail if publishFuture fails.
    final SettableFuture<SegmentsAndMetadata> handoffFuture = SettableFuture.create();
    handOffWaitList.add(handoffFuture);

    Futures.addCallback(
        publishFuture,
        new FutureCallback<SegmentsAndMetadata>()
        {
          @Override
          public void onSuccess(SegmentsAndMetadata publishedSegmentsAndMetadata)
          {
            log.info(
                "Published segments %s with metadata[%s].",
                Lists.transform(publishedSegmentsAndMetadata.getSegments(), DataSegment::getId),
                Preconditions.checkNotNull(publishedSegmentsAndMetadata.getCommitMetadata(), "commitMetadata")
            );

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
                driver.registerHandoff(publishedSegmentsAndMetadata),
                new Function<SegmentsAndMetadata, Void>()
                {
                  @Nullable
                  @Override
                  public Void apply(@Nullable SegmentsAndMetadata handoffSegmentsAndMetadata)
                  {
                    if (handoffSegmentsAndMetadata == null) {
                      log.warn(
                          "Failed to handoff segments %s",
                          Lists.transform(publishedSegmentsAndMetadata.getSegments(), DataSegment::getId)
                      );
                    }
                    handoffFuture.set(handoffSegmentsAndMetadata);
                    return null;
                  }
                }
            );
          }

          @Override
          public void onFailure(Throwable t)
          {
            log.error(t, "Error while publishing segments for sequence[%s]", sequenceMetadata);
            handoffFuture.setException(t);
          }
        }
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
          toolbox.getObjectMapper().<List<SequenceMetadata>>readValue(
              sequencesPersistFile,
              new TypeReference<List<SequenceMetadata>>()
              {
              }
          )
      );
      return true;
    } else {
      return false;
    }
  }

  private synchronized void persistSequences() throws IOException
  {
    log.info("Persisting Sequences Metadata [%s]", sequences);
    toolbox.getObjectMapper().writerWithType(
        new TypeReference<List<SequenceMetadata>>()
        {
        }
    ).writeValue(getSequencesPersistFile(toolbox), sequences);
  }

  private Map<String, TaskReport> getTaskCompletionReports(@Nullable String errorMsg)
  {
    return TaskReport.buildTaskReports(
        new IngestionStatsAndErrorsTaskReport(
            task.getId(),
            new IngestionStatsAndErrorsTaskReportData(
                ingestionState,
                getTaskCompletionUnparseableEvents(),
                getTaskCompletionRowStats(),
                errorMsg
            )
        )
    );
  }

  private Map<String, Object> getTaskCompletionUnparseableEvents()
  {
    Map<String, Object> unparseableEventsMap = new HashMap<>();
    List<String> buildSegmentsParseExceptionMessages = IndexTaskUtils.getMessagesFromSavedParseExceptions(
        savedParseExceptions
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
    for (SequenceMetadata sequenceMetadata : sequences) {
      sequenceMetadata.updateAssignments(currOffsets);
      if (!sequenceMetadata.isOpen() && !publishingSequences.contains(sequenceMetadata.getSequenceName())) {
        publishingSequences.add(sequenceMetadata.getSequenceName());
        try {
          Object result = driver.persist(committerSupplier.get());
          log.info(
              "Persist completed with results: [%s], adding sequence [%s] to publish queue",
              result,
              sequenceMetadata
          );
          publishAndRegisterHandoff(sequenceMetadata);
        }
        catch (InterruptedException e) {
          log.warn("Interrupted while persisting sequence [%s]", sequenceMetadata);
          throw e;
        }
      }
    }
  }

  private Set<StreamPartition<PartitionIdType>> assignPartitions(
      RecordSupplier<PartitionIdType, SequenceOffsetType> recordSupplier
  )
  {
    final Set<StreamPartition<PartitionIdType>> assignment = new HashSet<>();
    for (Map.Entry<PartitionIdType, SequenceOffsetType> entry : currOffsets.entrySet()) {
      final SequenceOffsetType endOffset = endOffsets.get(entry.getKey());
      if (isEndOfShard(endOffset)
          || SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER.equals(endOffset)
          || createSequenceNumber(entry.getValue()).compareTo(createSequenceNumber(endOffset)) < 0) {
        assignment.add(StreamPartition.of(stream, entry.getKey()));
      } else if (entry.getValue().equals(endOffset)) {
        log.info("Finished reading partition[%s].", entry.getKey());
      } else {
        throw new ISE(
            "WTF?! Cannot start from sequence[%,d] > endOffset[%,d]",
            entry.getValue(),
            endOffset
        );
      }
    }

    recordSupplier.assign(assignment);

    return assignment;
  }


  private void seekToStartingSequence(
      RecordSupplier<PartitionIdType, SequenceOffsetType> recordSupplier,
      Set<StreamPartition<PartitionIdType>> partitions
  ) throws InterruptedException
  {
    for (final StreamPartition<PartitionIdType> partition : partitions) {
      final SequenceOffsetType sequence = currOffsets.get(partition.getPartitionId());
      log.info("Seeking partition[%s] to sequence[%s].", partition.getPartitionId(), sequence);
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

        while (pauseRequested) {
          log.info("Pausing ingestion until resumed");
          shouldResume.await();
        }

        status = Status.READING;
        shouldResume.signalAll();
        log.info("Ingestion loop resumed");
        return true;
      }
    }
    finally {
      pauseLock.unlock();
    }

    return false;
  }


  private void handleParseException(ParseException pe, OrderedPartitionableRecord record)
  {
    if (pe.isFromPartiallyValidRow()) {
      rowIngestionMeters.incrementProcessedWithError();
    } else {
      rowIngestionMeters.incrementUnparseable();
    }

    if (tuningConfig.isLogParseExceptions()) {
      log.error(
          pe,
          "Encountered parse exception on row from partition[%s] sequence[%s]",
          record.getPartitionId(),
          record.getSequenceNumber()
      );
    }

    if (savedParseExceptions != null) {
      savedParseExceptions.add(pe);
    }

    if (rowIngestionMeters.getUnparseable() + rowIngestionMeters.getProcessedWithError()
        > tuningConfig.getMaxParseExceptions()) {
      log.error("Max parse exceptions exceeded, terminating task...");
      throw new RuntimeException("Max parse exceptions exceeded, terminating task...");
    }
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
  )
      throws IOException
  {
    Map<PartitionIdType, SequenceOffsetType> partitionOffsetMap = outOfRangePartitions
        .entrySet().stream().collect(Collectors.toMap(x -> x.getKey().getPartitionId(), Map.Entry::getValue));

    boolean result = taskToolbox
        .getTaskActionClient()
        .submit(
            new ResetDataSourceMetadataAction(
                task.getDataSource(),
                createDataSourceMetadata(
                    new SeekableStreamPartitions<>(
                        ioConfig.getStartPartitions().getStream(),
                        partitionOffsetMap
                    )
                )
            )
        );

    if (result) {
      log.makeAlert("Resetting sequences for datasource [%s]", task.getDataSource())
         .addData("partitions", partitionOffsetMap.keySet())
         .emit();

      requestPause();
    } else {
      log.makeAlert("Failed to send reset request for partitions [%s]", partitionOffsetMap.keySet()).emit();
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
      Throwables.propagate(e);
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

  public Map<PartitionIdType, SequenceOffsetType> getCurrentOffsets()
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

  @GET
  @Path("/rowStats")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getRowStats(
      @Context final HttpServletRequest req
  )
  {
    authorizationCheck(req, Action.READ);
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
    return Response.ok(returnMap).build();
  }

  @GET
  @Path("/unparseableEvents")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getUnparseableEvents(
      @Context final HttpServletRequest req
  )
  {
    authorizationCheck(req, Action.READ);
    List<String> events = IndexTaskUtils.getMessagesFromSavedParseExceptions(savedParseExceptions);
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
        Preconditions.checkState(sequenceNumbers.size() > 0, "WTH?! No Sequences found to set end sequences");

        final SequenceMetadata latestSequence = sequences.get(sequences.size() - 1);
        // if a partition has not been read yet (contained in initialOffsetsSnapshot), then
        // do not mark the starting sequence number as exclusive
        Set<PartitionIdType> exclusivePartitions = sequenceNumbers.keySet()
                                                                  .stream()
                                                                  .filter(x -> !initialOffsetsSnapshot.containsKey(x)
                                                                               || ioConfig.getExclusiveStartSequenceNumberPartitions()
                                                                                          .contains(x))
                                                                  .collect(Collectors.toSet());

        if ((latestSequence.getStartOffsets().equals(sequenceNumbers) && latestSequence.exclusiveStartPartitions.equals(
            exclusivePartitions) && !finish) ||
            (latestSequence.getEndOffsets().equals(sequenceNumbers) && finish)) {
          log.warn("Ignoring duplicate request, end sequences already set for sequences [%s]", sequenceNumbers);
          return Response.ok(sequenceNumbers).build();
        } else if (latestSequence.isCheckpointed()) {
          return Response.status(Response.Status.BAD_REQUEST)
                         .entity(StringUtils.format(
                             "WTH?! Sequence [%s] has already endOffsets set, cannot set to [%s]",
                             latestSequence,
                             sequenceNumbers
                         )).build();
        } else if (!isPaused()) {
          return Response.status(Response.Status.BAD_REQUEST)
                         .entity("Task must be paused before changing the end sequences")
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
          log.info("Updating endOffsets from [%s] to [%s]", endOffsets, sequenceNumbers);
          endOffsets.putAll(sequenceNumbers);
        } else {
          exclusiveStartingPartitions.addAll(exclusivePartitions);

          // create new sequence
          final SequenceMetadata newSequence = new SequenceMetadata(
              latestSequence.getSequenceId() + 1,
              StringUtils.format("%s_%d", ioConfig.getBaseSequenceName(), latestSequence.getSequenceId() + 1),
              sequenceNumbers,
              endOffsets,
              false,
              exclusivePartitions
          );
          sequences.add(newSequence);
          initialOffsetsSnapshot.putAll(sequenceNumbers);
        }
        persistSequences();
      }
      catch (Exception e) {
        log.error(e, "Unable to set end sequences, dying");
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
   * @return one of the following Responses: 400 Bad Request if the task has started publishing; 202 Accepted if the
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
      return Response.status(Response.Status.BAD_REQUEST)
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
      return Response.ok().entity(toolbox.getObjectMapper().writeValueAsString(getCurrentOffsets())).build();
    }
    catch (JsonProcessingException e) {
      throw Throwables.propagate(e);
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

  private class SequenceMetadata
  {
    private final int sequenceId;
    private final String sequenceName;
    private final Set<PartitionIdType> exclusiveStartPartitions;
    private final Set<PartitionIdType> assignments;
    private final boolean sentinel;
    private boolean checkpointed;
    /**
     * Lock for accessing {@link #endOffsets} and {@link #checkpointed}. This lock is required because
     * {@link #setEndOffsets)} can be called by both the main thread and the HTTP thread.
     */
    private final ReentrantLock lock = new ReentrantLock();

    final Map<PartitionIdType, SequenceOffsetType> startOffsets;
    final Map<PartitionIdType, SequenceOffsetType> endOffsets;

    @JsonCreator
    public SequenceMetadata(
        @JsonProperty("sequenceId") int sequenceId,
        @JsonProperty("sequenceName") String sequenceName,
        @JsonProperty("startOffsets") Map<PartitionIdType, SequenceOffsetType> startOffsets,
        @JsonProperty("endOffsets") Map<PartitionIdType, SequenceOffsetType> endOffsets,
        @JsonProperty("checkpointed") boolean checkpointed,
        @JsonProperty("exclusiveStartPartitions") Set<PartitionIdType> exclusiveStartPartitions
    )
    {
      Preconditions.checkNotNull(sequenceName);
      Preconditions.checkNotNull(startOffsets);
      Preconditions.checkNotNull(endOffsets);
      this.sequenceId = sequenceId;
      this.sequenceName = sequenceName;
      this.startOffsets = ImmutableMap.copyOf(startOffsets);
      this.endOffsets = new HashMap<>(endOffsets);
      this.assignments = new HashSet<>(startOffsets.keySet());
      this.checkpointed = checkpointed;
      this.sentinel = false;
      this.exclusiveStartPartitions = exclusiveStartPartitions == null
                                      ? Collections.emptySet()
                                      : exclusiveStartPartitions;
    }

    @JsonProperty
    public Set<PartitionIdType> getExclusiveStartPartitions()
    {
      return exclusiveStartPartitions;
    }

    @JsonProperty
    public int getSequenceId()
    {
      return sequenceId;
    }

    @JsonProperty
    public boolean isCheckpointed()
    {
      lock.lock();
      try {
        return checkpointed;
      }
      finally {
        lock.unlock();
      }
    }

    @JsonProperty
    public String getSequenceName()
    {
      return sequenceName;
    }

    @JsonProperty
    public Map<PartitionIdType, SequenceOffsetType> getStartOffsets()
    {
      return startOffsets;
    }

    @JsonProperty
    public Map<PartitionIdType, SequenceOffsetType> getEndOffsets()
    {
      lock.lock();
      try {
        return endOffsets;
      }
      finally {
        lock.unlock();
      }
    }

    @JsonProperty
    public boolean isSentinel()
    {
      return sentinel;
    }

    void setEndOffsets(Map<PartitionIdType, SequenceOffsetType> newEndOffsets)
    {
      lock.lock();
      try {
        endOffsets.putAll(newEndOffsets);
        checkpointed = true;
      }
      finally {
        lock.unlock();
      }
    }

    void updateAssignments(Map<PartitionIdType, SequenceOffsetType> nextPartitionOffset)
    {
      lock.lock();
      try {
        assignments.clear();
        nextPartitionOffset.forEach((key, value) -> {
          if (endOffsets.get(key).equals(SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER)
              || createSequenceNumber(endOffsets.get(key)).compareTo(createSequenceNumber(nextPartitionOffset.get(key)))
                 > 0) {
            assignments.add(key);
          }
        });
      }
      finally {
        lock.unlock();
      }
    }

    boolean isOpen()
    {
      return !assignments.isEmpty();
    }

    boolean canHandle(OrderedPartitionableRecord<PartitionIdType, SequenceOffsetType> record)
    {
      lock.lock();
      try {
        final OrderedSequenceNumber<SequenceOffsetType> partitionEndOffset = createSequenceNumber(endOffsets.get(record.getPartitionId()));
        final OrderedSequenceNumber<SequenceOffsetType> partitionStartOffset = createSequenceNumber(startOffsets.get(
            record.getPartitionId()));
        final OrderedSequenceNumber<SequenceOffsetType> recordOffset = createSequenceNumber(record.getSequenceNumber());
        if (!isOpen() || recordOffset == null || partitionEndOffset == null || partitionStartOffset == null) {
          return false;
        }
        boolean ret;
        if (isStartingSequenceOffsetsExclusive()) {
          ret = recordOffset.compareTo(partitionStartOffset)
                >= (getExclusiveStartPartitions().contains(record.getPartitionId()) ? 1 : 0);
        } else {
          ret = recordOffset.compareTo(partitionStartOffset) >= 0;
        }

        if (isEndSequenceOffsetsExclusive()) {
          ret &= recordOffset.compareTo(partitionEndOffset) < 0;
        } else {
          ret &= recordOffset.compareTo(partitionEndOffset) <= 0;
        }

        return ret;
      }
      finally {
        lock.unlock();
      }
    }

    @Override
    public String toString()
    {
      lock.lock();
      try {
        return "SequenceMetadata{" +
               "sequenceName='" + sequenceName + '\'' +
               ", sequenceId=" + sequenceId +
               ", startOffsets=" + startOffsets +
               ", endOffsets=" + endOffsets +
               ", assignments=" + assignments +
               ", sentinel=" + sentinel +
               ", checkpointed=" + checkpointed +
               '}';
      }
      finally {
        lock.unlock();
      }
    }

    Supplier<Committer> getCommitterSupplier(
        String stream,
        Map<PartitionIdType, SequenceOffsetType> lastPersistedOffsets
    )
    {
      // Set up committer.
      return () ->
          new Committer()
          {
            @Override
            public Object getMetadata()
            {
              lock.lock();

              try {
                Preconditions.checkState(
                    assignments.isEmpty(),
                    "This committer can be used only once all the records till sequences [%s] have been consumed, also make"
                    + " sure to call updateAssignments before using this committer",
                    endOffsets
                );


                // merge endOffsets for this sequence with globally lastPersistedOffsets
                // This is done because this committer would be persisting only sub set of segments
                // corresponding to the current sequence. Generally, lastPersistedOffsets should already
                // cover endOffsets but just to be sure take max of sequences and persist that
                for (Map.Entry<PartitionIdType, SequenceOffsetType> partitionOffset : endOffsets.entrySet()) {
                  SequenceOffsetType newOffsets = partitionOffset.getValue();
                  if (lastPersistedOffsets.containsKey(partitionOffset.getKey()) &&
                      createSequenceNumber(lastPersistedOffsets.get(partitionOffset.getKey())).compareTo(
                          createSequenceNumber(newOffsets)) > 0) {
                    newOffsets = lastPersistedOffsets.get(partitionOffset.getKey());
                  }
                  lastPersistedOffsets.put(
                      partitionOffset.getKey(),
                      newOffsets
                  );
                }

                // Publish metadata can be different from persist metadata as we are going to publish only
                // subset of segments
                return ImmutableMap.of(
                    METADATA_NEXT_PARTITIONS, new SeekableStreamPartitions<>(stream, lastPersistedOffsets),
                    METADATA_PUBLISH_PARTITIONS, new SeekableStreamPartitions<>(stream, endOffsets)
                );
              }
              finally {
                lock.unlock();
              }
            }

            @Override
            public void run()
            {
              // Do nothing.
            }
          };

    }

    TransactionalSegmentPublisher createPublisher(TaskToolbox toolbox, boolean useTransaction)
    {
      return (segments, commitMetadata) -> {
        final SeekableStreamPartitions<PartitionIdType, SequenceOffsetType> finalPartitions = deserializeSeekableStreamPartitionsFromMetadata(
            toolbox.getObjectMapper(),
            ((Map) Preconditions
                .checkNotNull(commitMetadata, "commitMetadata")).get(METADATA_PUBLISH_PARTITIONS)
        );

        // Sanity check, we should only be publishing things that match our desired end state.
        if (!getEndOffsets().equals(finalPartitions.getPartitionSequenceNumberMap())) {
          throw new ISE(
              "WTF?! Driver for sequence [%s], attempted to publish invalid metadata[%s].",
              toString(),
              commitMetadata
          );
        }

        final SegmentTransactionalInsertAction action;

        if (useTransaction) {
          action = new SegmentTransactionalInsertAction(
              segments,
              createDataSourceMetadata(new SeekableStreamPartitions<>(
                  finalPartitions.getStream(),
                  getStartOffsets()
              )),
              createDataSourceMetadata(finalPartitions)
          );
        } else {
          action = new SegmentTransactionalInsertAction(segments, null, null);
        }

        log.info("Publishing with isTransaction[%s].", useTransaction);

        return toolbox.getTaskActionClient().submit(action);
      };
    }

  }

  private boolean verifyInitialRecordAndSkipExclusivePartition(
      final OrderedPartitionableRecord<PartitionIdType, SequenceOffsetType> record,
      final Map<PartitionIdType, SequenceOffsetType> intialSequenceSnapshot
  )
  {
    if (intialSequenceSnapshot.containsKey(record.getPartitionId())) {
      if (!intialSequenceSnapshot.get(record.getPartitionId()).equals(record.getSequenceNumber())) {
        throw new ISE(
            "Starting sequenceNumber [%s] does not match expected [%s] for partition [%s]",
            record.getSequenceNumber(),
            intialSequenceSnapshot.get(record.getPartitionId()),
            record.getPartitionId()
        );
      }

      log.info(
          "Verified starting sequenceNumber [%s] for partition [%s]",
          record.getSequenceNumber(), record.getPartitionId()
      );

      intialSequenceSnapshot.remove(record.getPartitionId());
      if (intialSequenceSnapshot.isEmpty()) {
        log.info("Verified starting sequences for all partitions");
      }

      // check exclusive starting sequence
      if (isStartingSequenceOffsetsExclusive() && exclusiveStartingPartitions.contains(record.getPartitionId())) {
        log.info("Skipping starting sequenceNumber for partition [%s] marked exclusive", record.getPartitionId());

        return false;
      }
    }

    return true;
  }

  /**
   * deserailizes the checkpoints into of Map<sequenceId, Map<PartitionIdType, SequenceOffsetType>>
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
   * Calculates the sequence number used to update `currentOffsets` after finishing reading a record.
   * In Kafka this returns sequenceNumeber + 1 since that's the next expected offset
   * In Kinesis this simply returns sequenceNumber, since the sequence numbers in Kinesis are not
   * contiguous and finding the next sequence number requires an expensive API call
   *
   * @param sequenceNumber the sequence number that has already been processed
   *
   * @return next sequence number to be stored
   */
  protected abstract SequenceOffsetType getSequenceNumberToStoreAfterRead(SequenceOffsetType sequenceNumber);

  /**
   * deserialzies stored metadata into SeekableStreamPartitions
   *
   * @param mapper json objectMapper
   * @param object metadata
   *
   * @return SeekableStreamPartitions
   */
  protected abstract SeekableStreamPartitions<PartitionIdType, SequenceOffsetType> deserializeSeekableStreamPartitionsFromMetadata(
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
  protected abstract List<OrderedPartitionableRecord<PartitionIdType, SequenceOffsetType>> getRecords(
      RecordSupplier<PartitionIdType, SequenceOffsetType> recordSupplier,
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
      SeekableStreamPartitions<PartitionIdType, SequenceOffsetType> partitions
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
   *
   * @param toolbox
   * @param recordSupplier
   * @param assignment
   * @param currOffsets
   */
  protected abstract void possiblyResetDataSourceMetadata(
      TaskToolbox toolbox,
      RecordSupplier<PartitionIdType, SequenceOffsetType> recordSupplier,
      Set<StreamPartition<PartitionIdType>> assignment,
      Map<PartitionIdType, SequenceOffsetType> currOffsets
  );

  /**
   * In Kafka, the endOffsets are exclusive, so skip it.
   * In Kinesis the endOffsets are inclusive
   */
  protected abstract boolean isEndSequenceOffsetsExclusive();

  /**
   * In Kafka, the startingOffsets are inclusive.
   * In Kinesis, the startingOffsets are exclusive, except for the first
   * partition we read from stream
   */
  protected abstract boolean isStartingSequenceOffsetsExclusive();
}
