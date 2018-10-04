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

package org.apache.druid.indexing.kinesis;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.druid.data.input.Committer;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.LookupNodeService;
import org.apache.druid.indexer.IngestionState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.appenderator.ActionBasedSegmentAllocator;
import org.apache.druid.indexing.appenderator.ActionBasedUsedSegmentChecker;
import org.apache.druid.indexing.common.IngestionStatsAndErrorsTaskReport;
import org.apache.druid.indexing.common.IngestionStatsAndErrorsTaskReportData;
import org.apache.druid.indexing.common.TaskRealtimeMetricsMonitorBuilder;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.ResetDataSourceMetadataAction;
import org.apache.druid.indexing.common.actions.SegmentAllocateAction;
import org.apache.druid.indexing.common.actions.SegmentTransactionalInsertAction;
import org.apache.druid.indexing.common.stats.RowIngestionMeters;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.IndexTaskUtils;
import org.apache.druid.indexing.common.task.RealtimeIndexTask;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTask;
import org.apache.druid.indexing.seekablestream.SeekableStreamPartitions;
import org.apache.druid.indexing.seekablestream.common.Record;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.collect.Utils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.NoopQueryRunner;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.RealtimeIOConfig;
import org.apache.druid.segment.realtime.FireDepartment;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;
import org.apache.druid.segment.realtime.appenderator.Appenderator;
import org.apache.druid.segment.realtime.appenderator.AppenderatorDriverAddResult;
import org.apache.druid.segment.realtime.appenderator.Appenderators;
import org.apache.druid.segment.realtime.appenderator.SegmentIdentifier;
import org.apache.druid.segment.realtime.appenderator.SegmentsAndMetadata;
import org.apache.druid.segment.realtime.appenderator.StreamAppenderatorDriver;
import org.apache.druid.segment.realtime.appenderator.TransactionalSegmentPublisher;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

// TODO: kinesis task read from startPartitions to endPartitions inclusive, whereas kafka is exclusive, should change behavior to that of kafka's
public class KinesisIndexTask extends SeekableStreamIndexTask<String, String>
{
  public static final long PAUSE_FOREVER = -1L;

  private static final EmittingLogger log = new EmittingLogger(KinesisIndexTask.class);

  private static final long POLL_TIMEOUT = 100;
  private static final long POLL_RETRY_MS = 30000;
  private static final long LOCK_ACQUIRE_TIMEOUT_SECONDS = 15;
  private static final String METADATA_NEXT_PARTITIONS = "nextPartitions";

  private final AuthorizerMapper authorizerMapper;

  private final Map<String, String> endOffsets = new ConcurrentHashMap<>();
  private final Map<String, String> lastOffsets = new ConcurrentHashMap<>();
  private final KinesisIOConfig ioConfig;
  private final KinesisTuningConfig tuningConfig;
  private ObjectMapper mapper;

  private volatile Appenderator appenderator = null;
  private volatile FireDepartmentMetrics fireDepartmentMetrics = null;
  private volatile DateTime startTime;
  private volatile Status status = Status.NOT_STARTED; // this is only ever set by the task runner thread (runThread)
  private volatile Thread runThread = null;
  private volatile boolean stopRequested = false;
  private volatile boolean publishOnStop = false;

  // The pause lock and associated conditions are to support coordination between the Jetty threads and the main
  // ingestion loop. The goal is to provide callers of the API a guarantee that if pause() returns successfully
  // the ingestion loop has been stopped at the returned offsets and will not ingest any more data until resumed. The
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

  // [pollRetryLock] and [isAwaitingRetry] is used when the Kafka consumer returns an OffsetOutOfRangeException and we
  // pause polling from Kafka for POLL_RETRY_MS before trying again. This allows us to signal the sleeping thread and
  // resume the main run loop in the case of a pause or stop request from a Jetty thread.
  private final Lock pollRetryLock = new ReentrantLock();
  private final Condition isAwaitingRetry = pollRetryLock.newCondition();

  // [statusLock] is used to synchronize the Jetty thread calling stopGracefully() with the main run thread. It prevents
  // the main run thread from switching into a publishing state while the stopGracefully() thread thinks it's still in
  // a pre-publishing state. This is important because stopGracefully() will try to use the [stopRequested] flag to stop
  // the main thread where possible, but this flag is not honored once publishing has begun so in this case we must
  // interrupt the thread. The lock ensures that if the run thread is about to transition into publishing state, it
  // blocks until after stopGracefully() has set [stopRequested] and then does a final check on [stopRequested] before
  // transitioning to publishing state.
  private final Object statusLock = new Object();

  private final RowIngestionMeters rowIngestionMeters;
  private IngestionState ingestionState;

  private volatile boolean pauseRequested = false;
  private volatile long pauseMillis = 0;

  @JsonCreator
  public KinesisIndexTask(
      @JsonProperty("id") String id,
      @JsonProperty("resource") TaskResource taskResource,
      @JsonProperty("dataSchema") DataSchema dataSchema,
      @JsonProperty("tuningConfig") KinesisTuningConfig tuningConfig,
      @JsonProperty("ioConfig") KinesisIOConfig ioConfig,
      @JsonProperty("context") Map<String, Object> context,
      @JacksonInject ChatHandlerProvider chatHandlerProvider,
      @JacksonInject AuthorizerMapper authorizerMapper,
      @JacksonInject RowIngestionMetersFactory rowIngestionMetersFactory
  )
  {
    super(
        id,
        taskResource,
        dataSchema,
        tuningConfig,
        ioConfig,
        context,
        chatHandlerProvider,
        authorizerMapper,
        rowIngestionMetersFactory,
        "index_kinesis"
    );

    this.authorizerMapper = authorizerMapper;
    this.rowIngestionMeters = rowIngestionMetersFactory.createRowIngestionMeters();
    this.ingestionState = IngestionState.NOT_STARTED;
    this.ioConfig = ioConfig;
    this.tuningConfig = tuningConfig;
    this.endOffsets.putAll(ioConfig.getEndPartitions().getMap());
  }

  @Override
  public TaskStatus run(final TaskToolbox toolbox) throws Exception
  {
    log.info("Starting up!");
    startTime = DateTime.now();
    mapper = toolbox.getObjectMapper();
    status = Status.STARTING;

    if (chatHandlerProvider.isPresent()) {
      log.info("Found chat handler of class[%s]", chatHandlerProvider.get().getClass().getName());
      chatHandlerProvider.get().register(getId(), this, false);
    } else {
      log.warn("No chat handler detected");
    }

    runThread = Thread.currentThread();

    // Set up FireDepartmentMetrics
    final FireDepartment fireDepartmentForMetrics = new FireDepartment(
        dataSchema,
        new RealtimeIOConfig(null, null, null),
        null
    );
    fireDepartmentMetrics = fireDepartmentForMetrics.getMetrics();
    toolbox.getMonitorScheduler()
           .addMonitor(TaskRealtimeMetricsMonitorBuilder.build(this, fireDepartmentForMetrics, rowIngestionMeters));

    final LookupNodeService lookupNodeService = getContextValue(RealtimeIndexTask.CTX_KEY_LOOKUP_TIER) == null ?
                                                toolbox.getLookupNodeService() :
                                                new LookupNodeService((String) getContextValue(RealtimeIndexTask.CTX_KEY_LOOKUP_TIER));

    final DiscoveryDruidNode discoveryDruidNode = new DiscoveryDruidNode(
        toolbox.getDruidNode(),
        DruidNodeDiscoveryProvider.NODE_TYPE_PEON,
        ImmutableMap.of(
            toolbox.getDataNodeService().getName(), toolbox.getDataNodeService(),
            lookupNodeService.getName(), lookupNodeService
        )
    );

    try (
        final RecordSupplier<String, String> recordSupplier = getRecordSupplier()
    ) {
      toolbox.getDruidNodeAnnouncer().announce(discoveryDruidNode);
      toolbox.getDataSegmentServerAnnouncer().announce();

      final Appenderator appenderator0 = newAppenderator(fireDepartmentMetrics, toolbox);
      final StreamAppenderatorDriver driver = newDriver(appenderator0, toolbox, fireDepartmentMetrics);

      appenderator = appenderator0;

      final String topic = ioConfig.getStartPartitions().getId();

      // Start up, set up initial offsets.
      final Object restoredMetadata = driver.startJob();
      if (restoredMetadata == null) {
        lastOffsets.putAll(ioConfig.getStartPartitions().getMap());
      } else {
        final Map<String, Object> restoredMetadataMap = (Map) restoredMetadata;
        final SeekableStreamPartitions<String, String> restoredNextPartitions = toolbox.getObjectMapper().convertValue(
            restoredMetadataMap.get(METADATA_NEXT_PARTITIONS),
            SeekableStreamPartitions.class
        );
        lastOffsets.putAll(restoredNextPartitions.getMap());

        // Sanity checks.
        if (!restoredNextPartitions.getId().equals(ioConfig.getStartPartitions().getId())) {
          throw new ISE(
              "WTF?! Restored stream[%s] but expected stream[%s]",
              restoredNextPartitions.getId(),
              ioConfig.getStartPartitions().getId()
          );
        }

        if (!lastOffsets.keySet().equals(ioConfig.getStartPartitions().getMap().keySet())) {
          throw new ISE(
              "WTF?! Restored partitions[%s] but expected partitions[%s]",
              lastOffsets.keySet(),
              ioConfig.getStartPartitions().getMap().keySet()
          );
        }
      }

      // Filter out partitions with END_OF_SHARD markers since these partitions have already been fully read. This
      // should have been done by the supervisor already so this is defensive.
      int numPreFilterPartitions = lastOffsets.size();
      if (lastOffsets.entrySet().removeIf(x -> Record.END_OF_SHARD_MARKER.equals(x.getValue()))) {
        log.info(
            "Removed [%d] partitions from assignment which have already been closed",
            numPreFilterPartitions - lastOffsets.size()
        );
      }

      // Set up sequenceNames.
      final Map<String, String> sequenceNames = Maps.newHashMap();
      for (String partitionNum : lastOffsets.keySet()) {
        sequenceNames.put(partitionNum, String.format("%s_%s", ioConfig.getBaseSequenceName(), partitionNum));
      }

      // Set up committer.
      final Supplier<Committer> committerSupplier = new Supplier<Committer>()
      {
        @Override
        public Committer get()
        {
          final Map<String, String> snapshot = ImmutableMap.copyOf(lastOffsets);

          return new Committer()
          {
            @Override
            public Object getMetadata()
            {
              return ImmutableMap.of(
                  METADATA_NEXT_PARTITIONS, new SeekableStreamPartitions<>(
                      ioConfig.getStartPartitions().getId(),
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
        }
      };

      Set<String> assignment = assignPartitions(recordSupplier, topic);
      seekToStartingRecords(recordSupplier, topic, assignment, toolbox);

      Map<String, String> contiguousOffsetCheck = new HashMap<>(lastOffsets);
      boolean verifiedAllStartingOffsets = false;

      // Main loop.
      // Could eventually support leader/follower mode (for keeping replicas more in sync)
      boolean stillReading = !assignment.isEmpty();
      status = Status.READING;
      try {
        while (stillReading) {
          if (possiblyPause(assignment)) {
            assignment = assignPartitions(recordSupplier, topic);

            if (assignment.isEmpty()) {
              log.info("All partitions have been fully read");
              publishOnStop = true;
              stopRequested = true;
            }
          }

          if (stopRequested) {
            break;
          }

          Record<String, String> record = recordSupplier.poll(POLL_TIMEOUT);

          if (record == null) {
            continue;
          }

          // for the first message we receive, check that we were given a message with a sequenceNumber that matches our
          // expected starting sequenceNumber
          if (!verifiedAllStartingOffsets && contiguousOffsetCheck.containsKey(record.getPartitionId())) {
            if (!contiguousOffsetCheck.get(record.getPartitionId()).equals(record.getSequenceNumber())) {
              throw new ISE(
                  "Starting sequenceNumber [%s] does not match expected [%s] for partition [%s]",
                  record.getSequenceNumber(),
                  contiguousOffsetCheck.get(record.getPartitionId()),
                  record.getPartitionId()
              );
            }

            log.info(
                "Verified starting sequenceNumber [%s] for partition [%s]",
                record.getSequenceNumber(), record.getPartitionId()
            );

            contiguousOffsetCheck.remove(record.getPartitionId());
            if (contiguousOffsetCheck.isEmpty()) {
              verifiedAllStartingOffsets = true;
              log.info("Verified starting offsets for all partitions");
            }

            if (ioConfig.getExclusiveStartSequenceNumberPartitions() != null
                && ioConfig.getExclusiveStartSequenceNumberPartitions().contains(record.getPartitionId())) {
              log.info("Skipping starting sequenceNumber for partition [%s] marked exclusive", record.getPartitionId());

              continue;
            }
          }

          if (log.isTraceEnabled()) {
            log.trace(
                "Got topic[%s] partition[%s] offset[%s].",
                record.getStreamName(),
                record.getPartitionId(),
                record.getSequenceNumber()
            );
          }

          if (Record.END_OF_SHARD_MARKER.equals(record.getSequenceNumber())) {
            lastOffsets.put(record.getPartitionId(), record.getSequenceNumber());

          } else if (SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER.equals(endOffsets.get(record.getPartitionId()))
                     || record.getSequenceNumber().compareTo(endOffsets.get(record.getPartitionId())) <= 0) {

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
              final Map<String, Set<SegmentIdentifier>> segmentsToMoveOut = new HashMap<>();

              for (final InputRow row : rows) {
                if (row != null && withinMinMaxRecordTime(row)) {
                  final String sequenceName = sequenceNames.get(record.getPartitionId());
                  final AppenderatorDriverAddResult addResult = driver.add(
                      row,
                      sequenceName,
                      committerSupplier,
                      false,
                      false
                  );

                  if (addResult.isOk()) {
                    // If the number of rows in the segment exceeds the threshold after adding a row,
                    // move the segment out from the active segments of BaseAppenderatorDriver to make a new segment.
                    if (addResult.getNumRowsInSegment() > tuningConfig.getMaxRowsPerSegment()) {
                      segmentsToMoveOut.computeIfAbsent(sequenceName, k -> new HashSet<>())
                                       .add(addResult.getSegmentIdentifier());
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
                driver.persist(committerSupplier.get());
              }
              segmentsToMoveOut.entrySet().forEach(sequenceSegments -> driver.moveSegmentOut(
                  sequenceSegments.getKey(),
                  sequenceSegments.getValue().stream().collect(Collectors.toList())
              ));
            }
            catch (ParseException e) {
              handleParseException(e, record);
            }

            lastOffsets.put(record.getPartitionId(), record.getSequenceNumber());


          }
          if ((lastOffsets.get(record.getPartitionId()).equals(endOffsets.get(record.getPartitionId()))
               || Record.END_OF_SHARD_MARKER.equals(lastOffsets.get(record.getPartitionId())))
              && assignment.remove(record.getPartitionId())) {

            log.info("Finished reading stream[%s], partition[%s].", record.getStreamName(), record.getPartitionId());
            assignPartitions(recordSupplier, topic, assignment);
            stillReading = !assignment.isEmpty();
          }
        }
      }
      catch (Exception e) {
        log.error(e, "Encountered exception while running task.");
        final String errorMsg = Throwables.getStackTraceAsString(e);
        toolbox.getTaskReportFileWriter().write(getTaskCompletionReports(errorMsg));
        return TaskStatus.failure(
            getId(),
            errorMsg
        );
      }
      finally {
        log.info("Persisting all pending data");
        driver.persist(committerSupplier.get()); // persist pending data
      }

      synchronized (statusLock) {
        if (stopRequested && !publishOnStop) {
          throw new InterruptedException("Stopping without publishing");
        }

        status = Status.PUBLISHING;
      }

      final TransactionalSegmentPublisher publisher = (segments, commitMetadata) -> {
        final SeekableStreamPartitions<String, String> finalPartitions = toolbox.getObjectMapper().convertValue(
            ((Map) commitMetadata).get(METADATA_NEXT_PARTITIONS),
            SeekableStreamPartitions.class
        );

        // Sanity check, we should only be publishing things that match our desired end state.
        if (!endOffsets.equals(finalPartitions.getMap())) {
          throw new ISE("WTF?! Driver attempted to publish invalid metadata[%s].", commitMetadata);
        }

        final SegmentTransactionalInsertAction action;

        if (ioConfig.isUseTransaction()) {
          action = new SegmentTransactionalInsertAction(
              segments,
              new KinesisDataSourceMetadata(ioConfig.getStartPartitions()),
              new KinesisDataSourceMetadata(finalPartitions)
          );
        } else {
          action = new SegmentTransactionalInsertAction(segments, null, null);
        }

        log.info("Publishing with isTransaction[%s].", ioConfig.isUseTransaction());

        return toolbox.getTaskActionClient().submit(action);
      };

      // Supervised Kinesis tasks are killed by KinesisSupervisor if they are stuck during publishing segments or waiting
      // for hand off. See KinesisSupervisorIOConfig.completionTimeout.
      final SegmentsAndMetadata published = driver.publish(
          publisher,
          committerSupplier.get(),
          sequenceNames.values()
      ).get();

      final SegmentsAndMetadata handedOff;
      if (tuningConfig.getHandoffConditionTimeout() == 0) {
        handedOff = driver.registerHandoff(published)
                          .get();
      } else {
        handedOff = driver.registerHandoff(published)
                          .get(tuningConfig.getHandoffConditionTimeout(), TimeUnit.MILLISECONDS);
      }

      if (handedOff == null) {
        throw new ISE("Transaction failure publishing segments, aborting");
      } else {
        log.info(
            "Published segments[%s] with metadata[%s].",
            Joiner.on(", ").join(
                Iterables.transform(
                    handedOff.getSegments(),
                    new Function<DataSegment, String>()
                    {
                      @Override
                      public String apply(DataSegment input)
                      {
                        return input.getIdentifier();
                      }
                    }
                )
            ),
            handedOff.getCommitMetadata()
        );
      }
    }
    catch (InterruptedException | RejectedExecutionException e) {
      final String errorMsg = Throwables.getStackTraceAsString(e);
      toolbox.getTaskReportFileWriter().write(getTaskCompletionReports(errorMsg));

      if (appenderator != null) {
        appenderator.closeNow();
      }

      // handle the InterruptedException that gets wrapped in a RejectedExecutionException
      if (e instanceof RejectedExecutionException
          && (e.getCause() == null || !(e.getCause() instanceof InterruptedException))) {
        throw e;
      }

      // if we were interrupted because we were asked to stop, handle the exception and return success, else rethrow
      if (!stopRequested) {
        Thread.currentThread().interrupt();
        throw e;
      }

      log.info("The task was asked to stop before completing");
    }
    finally {
      if (chatHandlerProvider.isPresent()) {
        chatHandlerProvider.get().unregister(getId());
      }
    }

    toolbox.getDruidNodeAnnouncer().unannounce(discoveryDruidNode);
    toolbox.getDataSegmentServerAnnouncer().unannounce();

    toolbox.getTaskReportFileWriter().write(getTaskCompletionReports(null));
    return success();
  }

  private Map<String, TaskReport> getTaskCompletionReports(@Nullable String errorMsg)
  {
    return TaskReport.buildTaskReports(
        new IngestionStatsAndErrorsTaskReport(
            getId(),
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
    Map<String, Object> unparseableEventsMap = Maps.newHashMap();
    List<String> buildSegmentsParseExceptionMessages = IndexTaskUtils.getMessagesFromSavedParseExceptions(
        savedParseExceptions
    );
    if (buildSegmentsParseExceptionMessages != null) {
      unparseableEventsMap.put(RowIngestionMeters.BUILD_SEGMENTS, buildSegmentsParseExceptionMessages);
    }
    return unparseableEventsMap;
  }

  @Override
  public boolean canRestore()
  {
    return true;
  }

  /**
   * Authorizes action to be performed on this task's datasource
   *
   * @return authorization result
   */
  private Access authorizationCheck(final HttpServletRequest req, Action action)
  {
    ResourceAction resourceAction = new ResourceAction(
        new Resource(dataSchema.getDataSource(), ResourceType.DATASOURCE),
        action
    );

    Access access = AuthorizationUtils.authorizeResourceAction(req, resourceAction, authorizerMapper);
    if (!access.isAllowed()) {
      throw new ForbiddenException(access.toString());
    }

    return access;
  }

  private void handleParseException(ParseException pe, Record record)
  {
    if (pe.isFromPartiallyValidRow()) {
      rowIngestionMeters.incrementProcessedWithError();
    } else {
      rowIngestionMeters.incrementUnparseable();
    }

    if (tuningConfig.isLogParseExceptions()) {
      log.error(
          pe,
          "Encountered parse exception on row from partition[%s] sequenceNumber[%s]",
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

  @Override
  public void stopGracefully()
  {
    log.info("Stopping gracefully (status: [%s])", status);
    stopRequested = true;

    synchronized (statusLock) {
      if (status == Status.PUBLISHING) {
        runThread.interrupt();
        return;
      }
    }

    try {
      if (pauseLock.tryLock(LOCK_ACQUIRE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
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

      if (pollRetryLock.tryLock(LOCK_ACQUIRE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
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

  @Override
  public <T> QueryRunner<T> getQueryRunner(Query<T> query)
  {
    if (appenderator == null) {
      // Not yet initialized, no data yet, just return a noop runner.
      return new NoopQueryRunner<>();
    }

    return new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(final QueryPlus<T> query, final Map<String, Object> responseContext)
      {
        return query.run(appenderator, responseContext);
      }
    };
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

  public Status getStatus()
  {
    return status;
  }

  @GET
  @Path("/offsets/current")
  @Produces(MediaType.APPLICATION_JSON)
  public Map<String, String> getCurrentOffsets(@Context final HttpServletRequest req)
  {
    authorizationCheck(req, Action.READ);
    return getCurrentOffsets();
  }

  public Map<String, String> getCurrentOffsets()
  {
    return lastOffsets;
  }

  @GET
  @Path("/offsets/end")
  @Produces(MediaType.APPLICATION_JSON)
  public Map<String, String> getEndOffsetsHTTP(@Context final HttpServletRequest req)
  {
    authorizationCheck(req, Action.READ);
    return getEndOffsets();
  }

  public Map<String, String> getEndOffsets()
  {
    return endOffsets;
  }

  @POST
  @Path("/offsets/end")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response setEndOffsetsHTTP(
      Map<String, String> offsets,
      @QueryParam("resume") @DefaultValue("false") final boolean resume,
      @Context final HttpServletRequest req
  ) throws InterruptedException
  {
    authorizationCheck(req, Action.WRITE);
    return setEndOffsets(offsets, resume);
  }

  public Response setEndOffsets(Map<String, String> offsets, final boolean resume) throws InterruptedException
  {
    if (offsets == null) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity("Request body must contain a map of { partition:endOffset }")
                     .build();
    } else if (!endOffsets.keySet().containsAll(offsets.keySet())) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(
                         String.format(
                             "Request contains partitions not being handled by this task, my partitions: %s",
                             endOffsets.keySet()
                         )
                     )
                     .build();
    }

    pauseLock.lockInterruptibly();
    try {
      if (!isPaused()) {
        return Response.status(Response.Status.BAD_REQUEST)
                       .entity("Task must be paused before changing the end offsets")
                       .build();
      }

      for (Map.Entry<String, String> entry : offsets.entrySet()) {
        if (entry.getValue().compareTo(lastOffsets.get(entry.getKey())) < 0) {
          return Response.status(Response.Status.BAD_REQUEST)
                         .entity(
                             String.format(
                                 "End offset must be >= current offset for partition [%s] (current: %s)",
                                 entry.getKey(),
                                 lastOffsets.get(entry.getKey())
                             )
                         )
                         .build();
        }
      }

      endOffsets.putAll(offsets);
      log.info("endOffsets changed to %s", endOffsets);
    }
    finally {
      pauseLock.unlock();
    }

    if (resume) {
      resume();
    }

    return Response.ok(endOffsets).build();
  }

  /**
   * Signals the ingestion loop to pause.
   *
   * @param timeout how long to pause for before resuming in milliseconds, <= 0 means indefinitely
   *
   * @return one of the following Responses: 400 Bad Request if the task has started publishing; 202 Accepted if the
   * method has timed out and returned before the task has paused; 200 OK with a map of the current partition offsets
   * in the response body if the task successfully paused
   */
  @POST
  @Path("/pause")
  @Produces(MediaType.APPLICATION_JSON)
  public Response pauseHTTP(
      @QueryParam("timeout") @DefaultValue("0") final long timeout, @Context final HttpServletRequest req
  ) throws InterruptedException
  {
    authorizationCheck(req, Action.WRITE);
    return pause(timeout);
  }

  public Response pause(final long timeout) throws InterruptedException
  {
    if (!(status == Status.PAUSED || status == Status.READING)) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(String.format("Can't pause, task is not in a pausable state (state: [%s])", status))
                     .build();
    }

    pauseLock.lockInterruptibly();
    try {
      pauseMillis = timeout <= 0 ? PAUSE_FOREVER : timeout;
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
      return Response.ok().entity(mapper.writeValueAsString(getCurrentOffsets())).build();
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
    authorizationCheck(req, Action.READ);
    return startTime;
  }

  @GET
  @Path("/rowStats")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getRowStats(
      @Context final HttpServletRequest req
  )
  {
    authorizationCheck(req, Action.READ);
    Map<String, Object> returnMap = Maps.newHashMap();
    Map<String, Object> totalsMap = Maps.newHashMap();
    Map<String, Object> averagesMap = Maps.newHashMap();

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
  RowIngestionMeters getRowIngestionMeters()
  {
    return rowIngestionMeters;
  }

  @VisibleForTesting
  Appenderator getAppenderator()
  {
    return appenderator;
  }

  @VisibleForTesting
  FireDepartmentMetrics getFireDepartmentMetrics()
  {
    return fireDepartmentMetrics;
  }

  private boolean isPaused()
  {
    return status == Status.PAUSED;
  }

  private Appenderator newAppenderator(FireDepartmentMetrics metrics, TaskToolbox toolbox)
  {
    final int maxRowsInMemoryPerPartition = (tuningConfig.getMaxRowsInMemory() /
                                             ioConfig.getStartPartitions().getMap().size());
    return Appenderators.createRealtime(
        dataSchema,
        tuningConfig.withBasePersistDirectory(toolbox.getPersistDir()),
        metrics,
        toolbox.getSegmentPusher(),
        toolbox.getObjectMapper(),
        toolbox.getIndexIO(),
        toolbox.getIndexMergerV9(),
        toolbox.getQueryRunnerFactoryConglomerate(),
        toolbox.getSegmentAnnouncer(),
        toolbox.getEmitter(),
        toolbox.getQueryExecutorService(),
        toolbox.getCache(),
        toolbox.getCacheConfig(),
        toolbox.getCachePopulatorStats()
    );
  }

  private StreamAppenderatorDriver newDriver(
      final Appenderator appenderator,
      final TaskToolbox toolbox,
      final FireDepartmentMetrics metrics
  )
  {
    return new StreamAppenderatorDriver(
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
                skipSegmentLineageCheck
            )
        ),
        toolbox.getSegmentHandoffNotifierFactory(),
        new ActionBasedUsedSegmentChecker(toolbox.getTaskActionClient()),
        toolbox.getDataSegmentKiller(),
        toolbox.getObjectMapper(),
        metrics
    );
  }

  private RecordSupplier<String, String> getRecordSupplier()
  {
    int fetchThreads = tuningConfig.getFetchThreads() != null
                       ? tuningConfig.getFetchThreads()
                       : Math.max(1, ioConfig.getStartPartitions().getMap().size());

    return new KinesisRecordSupplier(
        ioConfig.getEndpoint(),
        ioConfig.getAwsAccessKeyId(),
        ioConfig.getAwsSecretAccessKey(),
        ioConfig.getRecordsPerFetch(),
        ioConfig.getFetchDelayMillis(),
        fetchThreads,
        ioConfig.getAwsAssumedRoleArn(),
        ioConfig.getAwsExternalId(),
        ioConfig.isDeaggregate(),
        tuningConfig.getRecordBufferSize(),
        tuningConfig.getRecordBufferOfferTimeout(),
        tuningConfig.getRecordBufferFullWait(),
        tuningConfig.getFetchSequenceNumberTimeout()
    );
  }

  private static void assignPartitions(
      final RecordSupplier recordSupplier,
      final String topic,
      final Set<String> partitions
  )
  {
    recordSupplier.assign(partitions.stream().map(x -> StreamPartition.of(topic, x)).collect(Collectors.toSet()));
  }

  private Set<String> assignPartitions(RecordSupplier recordSupplier, String topic)
  {
    // Initialize consumer assignment.
    final Set<String> assignment = Sets.newHashSet();
    for (Map.Entry<String, String> entry : lastOffsets.entrySet()) {
      final String endOffset = endOffsets.get(entry.getKey());
      if (Record.END_OF_SHARD_MARKER.equals(endOffset)
          || SeekableStreamPartitions.NO_END_SEQUENCE_NUMBER.equals(endOffset)
          || KinesisSequenceNumber.of(entry.getValue()).compareTo(KinesisSequenceNumber.of(endOffset)) < 0) {
        assignment.add(entry.getKey());
      } else if (entry.getValue().equals(endOffset)) {
        log.info("Finished reading partition[%s].", entry.getKey());
      } else {
        throw new ISE(
            "WTF?! Cannot start from offset[%s] > endOffset[%s]",
            entry.getValue(),
            endOffset
        );
      }
    }

    assignPartitions(recordSupplier, topic, assignment);

    return assignment;
  }

  private void seekToStartingRecords(
      RecordSupplier<String, String> recordSupplier,
      String topic,
      Set<String> assignment,
      TaskToolbox toolbox
  )
  {
    // Seek to starting offsets.
    for (final String partition : assignment) {
      final String offset = lastOffsets.get(partition);
      final StreamPartition<String> streamPartition = StreamPartition.of(topic, partition);

      if (!tuningConfig.isSkipSequenceNumberAvailabilityCheck()) {
        try {
          String earliestSequenceNumber = recordSupplier.getEarliestSequenceNumber(streamPartition);
          if (earliestSequenceNumber == null
              || KinesisSequenceNumber.of(earliestSequenceNumber).compareTo(KinesisSequenceNumber.of(offset)) > 0) {
            if (tuningConfig.isResetOffsetAutomatically()) {
              log.info("Attempting to reset offsets automatically for all partitions");
              try {
                sendResetRequestAndWait(
                    assignment.stream()
                              .collect(Collectors.toMap((x) -> new StreamPartition<>(topic, x), lastOffsets::get)),
                    toolbox
                );
              }
              catch (IOException e) {
                throw new ISE(e, "Exception while attempting to automatically reset offsets");
              }
            } else {
              throw new ISE(
                  "Starting sequenceNumber [%s] is no longer available for partition [%s] (earliest: [%s]) and resetOffsetAutomatically is not enabled",
                  offset,
                  partition,
                  earliestSequenceNumber
              );
            }
          }
        }
        catch (TimeoutException e) {
          throw new ISE(e, "Timeout while fetching earliest sequence number for partition [%s]", partition);
        }
      }

      log.info("Seeking partition[%s] to sequenceNumber[%s].", partition, offset);

      // We will seek to and start reading from the last offset that we read on the previous run so that we can confirm
      // that the sequenceNumbers match, but we will discard the event instead of indexing it so we don't read it twice.
      recordSupplier.seek(streamPartition, offset);
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
  private boolean possiblyPause(Set<String> assignment) throws InterruptedException
  {
    pauseLock.lockInterruptibly();
    try {
      /*
      if (ioConfig.isPauseAfterRead() && assignment.isEmpty()) {
        pauseMillis = PAUSE_FOREVER;
        pauseRequested = true;
      }
      */

      if (pauseRequested) {
        status = Status.PAUSED;
        long nanos = 0;
        hasPaused.signalAll();

        while (pauseRequested) {
          if (pauseMillis == PAUSE_FOREVER) {
            log.info("Pausing ingestion until resumed");
            shouldResume.await();
          } else {
            if (pauseMillis > 0) {
              log.info("Pausing ingestion for [%,d] ms", pauseMillis);
              nanos = TimeUnit.MILLISECONDS.toNanos(pauseMillis);
              pauseMillis = 0;
            }
            if (nanos <= 0L) {
              pauseRequested = false; // timeout elapsed
            }
            nanos = shouldResume.awaitNanos(nanos);
          }
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

  private void sendResetRequestAndWait(
      Map<StreamPartition<String>, String> outOfRangePartitions,
      TaskToolbox taskToolbox
  )
      throws IOException
  {
    Map<String, String> partitionOffsetMap = outOfRangePartitions
        .entrySet().stream().collect(Collectors.toMap(x -> x.getKey().getPartitionId(), Map.Entry::getValue));

    boolean result = taskToolbox
        .getTaskActionClient()
        .submit(
            new ResetDataSourceMetadataAction(
                getDataSource(),
                new KinesisDataSourceMetadata(
                    new SeekableStreamPartitions<String, String>(ioConfig.getStartPartitions().getId(), partitionOffsetMap)
                )
            )
        );

    if (result) {
      log.makeAlert("Resetting Kinesis offsets for datasource [%s]", getDataSource())
         .addData("partitions", partitionOffsetMap.keySet())
         .emit();
      // wait for being killed by supervisor
      try {
        pause(-1);
      }
      catch (InterruptedException e) {
        throw new RuntimeException("Got interrupted while pausing task");
      }
    } else {
      log.makeAlert("Failed to send reset request for partitions [%s]", partitionOffsetMap.keySet()).emit();
    }
  }

  private boolean withinMinMaxRecordTime(final InputRow row)
  {
    final boolean beforeMinimumMessageTime = ioConfig.getMinimumMessageTime().isPresent()
                                             && ioConfig.getMinimumMessageTime().get().isAfter(row.getTimestamp());

    final boolean afterMaximumMessageTime = ioConfig.getMaximumMessageTime().isPresent()
                                            && ioConfig.getMaximumMessageTime().get().isBefore(row.getTimestamp());

    if (!Intervals.ETERNITY.contains(row.getTimestamp())) {
      final String errorMsg = StringUtils.format(
          "Encountered row with timestamp that cannot be represented as a long: [%s]",
          row
      );
      throw new ParseException(errorMsg);
    }

    if (log.isDebugEnabled()) {
      if (beforeMinimumMessageTime) {
        log.debug(
            "CurrentTimeStamp[%s] is before MinimumMessageTime[%s]",
            row.getTimestamp(),
            ioConfig.getMinimumMessageTime().get()
        );
      } else if (afterMaximumMessageTime) {
        log.debug(
            "CurrentTimeStamp[%s] is after MaximumMessageTime[%s]",
            row.getTimestamp(),
            ioConfig.getMaximumMessageTime().get()
        );
      }
    }

    return !beforeMinimumMessageTime && !afterMaximumMessageTime;
  }

  private Map<String, Object> getTaskCompletionRowStats()
  {
    Map<String, Object> metrics = Maps.newHashMap();
    metrics.put(
        RowIngestionMeters.BUILD_SEGMENTS,
        rowIngestionMeters.getTotals()
    );
    return metrics;
  }

  @Override
  @JsonProperty("ioConfig")
  public KinesisIOConfig getIOConfig()
  {
    return (KinesisIOConfig) super.getIOConfig();
  }
}
