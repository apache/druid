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

package org.apache.druid.indexing.pubsub;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.pubsub.v1.ReceivedMessage;
import org.apache.druid.data.input.Committer;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.LookupNodeService;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.indexer.IngestionState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TaskRealtimeMetricsMonitorBuilder;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.SegmentLockAcquireAction;
import org.apache.druid.indexing.common.actions.TimeChunkLockAcquireAction;
import org.apache.druid.indexing.common.stats.RowIngestionMeters;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.RealtimeIndexTask;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.indexing.RealtimeIOConfig;
import org.apache.druid.segment.realtime.FireDepartment;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;
import org.apache.druid.segment.realtime.appenderator.Appenderator;
import org.apache.druid.segment.realtime.appenderator.AppenderatorDriverAddResult;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.segment.realtime.appenderator.SegmentsAndMetadata;
import org.apache.druid.segment.realtime.appenderator.StreamAppenderatorDriver;
import org.apache.druid.segment.realtime.firehose.ChatHandler;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.utils.CircularBuffer;
import org.joda.time.DateTime;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * Pubsub indexing task runner supporting incremental segments publishing
 */
public class PubsubIndexTaskRunner implements ChatHandler
{
  private static final EmittingLogger log = new EmittingLogger(PubsubIndexTaskRunner.class);
  protected final AtomicBoolean stopRequested = new AtomicBoolean(false);
  protected final Lock pollRetryLock = new ReentrantLock();
  protected final Condition isAwaitingRetry = pollRetryLock.newCondition();
  private final PubsubIndexTaskIOConfig ioConfig;
  private final PubsubIndexTaskTuningConfig tuningConfig;
  private final PubsubIndexTask task;
  private final InputRowParser<ByteBuffer> parser;
  private final AuthorizerMapper authorizerMapper;
  private final Optional<ChatHandlerProvider> chatHandlerProvider;
  private final CircularBuffer<Throwable> savedParseExceptions;
  private final RowIngestionMetersFactory rowIngestionMetersFactory;
  private final AppenderatorsManager appenderatorsManager;
  private final LockGranularity lockGranularityToUse;
  private final InputRowSchema inputRowSchema;
  private final InputFormat inputFormat;
  private final RowIngestionMeters rowIngestionMeters;
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
  private final AtomicBoolean publishOnStop = new AtomicBoolean(false);
  private final List<ListenableFuture<SegmentsAndMetadata>> publishWaitList = new ArrayList<>();
  private final List<ListenableFuture<SegmentsAndMetadata>> handOffWaitList = new ArrayList<>();
  protected volatile boolean pauseRequested = false;
  private volatile DateTime startTime;
  private volatile Status status = Status.NOT_STARTED; // this is only ever set by the task runner thread (runThread)
  private volatile TaskToolbox toolbox;
  private volatile Thread runThread;
  private volatile Appenderator appenderator;
  private volatile StreamAppenderatorDriver driver;
  private volatile IngestionState ingestionState;

  PubsubIndexTaskRunner(
      PubsubIndexTask task,
      @Nullable InputRowParser<ByteBuffer> parser,
      AuthorizerMapper authorizerMapper,
      Optional<ChatHandlerProvider> chatHandlerProvider,
      CircularBuffer<Throwable> savedParseExceptions,
      RowIngestionMetersFactory rowIngestionMetersFactory,
      AppenderatorsManager appenderatorsManager,
      LockGranularity lockGranularityToUse
  )
  {
    this.task = task;
    this.ioConfig = task.getIOConfig();
    this.tuningConfig = task.getTuningConfig();
    this.parser = parser;
    this.authorizerMapper = authorizerMapper;
    this.chatHandlerProvider = chatHandlerProvider;
    this.savedParseExceptions = savedParseExceptions;
    this.rowIngestionMetersFactory = rowIngestionMetersFactory;
    this.appenderatorsManager = appenderatorsManager;
    this.lockGranularityToUse = lockGranularityToUse;
    this.inputFormat = ioConfig.getInputFormat(parser == null ? null : parser.getParseSpec());
    this.rowIngestionMeters = rowIngestionMetersFactory.createRowIngestionMeters();

    this.inputRowSchema = new InputRowSchema(
        task.getDataSchema().getTimestampSpec(),
        task.getDataSchema().getDimensionsSpec(),
        Arrays.stream(task.getDataSchema().getAggregators())
              .map(AggregatorFactory::getName)
              .collect(Collectors.toList())
    );
  }

  private boolean isPaused()
  {
    return status == Status.PAUSED;
  }

  private void requestPause()
  {
    pauseRequested = true;
  }

  public TaskStatus run(TaskToolbox toolbox)
  {
    try {
      log.info("running pubsub task");
      return runInternal(toolbox);
    }
    catch (Exception e) {
      log.error(e, "Encountered exception while running task.");
      final String errorMsg = Throwables.getStackTraceAsString(e);
      // toolbox.getTaskReportFileWriter().write(task.getId(), getTaskCompletionReports(errorMsg));
      return TaskStatus.failure(
          task.getId(),
          errorMsg
      );
    }
  }

  @Nonnull
  protected List<ReceivedMessage> getRecords(
      PubsubRecordSupplier recordSupplier,
      TaskToolbox toolbox
  ) throws Exception
  {
    // Handles OffsetOutOfRangeException, which is thrown if the seeked-to
    // offset is not present in the topic-partition. This can happen if we're asking a task to read from data
    // that has not been written yet (which is totally legitimate). So let's wait for it to show up.
    List<ReceivedMessage> records = new ArrayList<>();
    try {
      records = recordSupplier.poll(task.getIOConfig().getPollTimeout());
    }
    catch (Exception e) {
      log.warn("OffsetOutOfRangeException with message [%s]", e.getMessage());
    }

    return records;
  }

  @VisibleForTesting
  public void setToolbox(TaskToolbox toolbox)
  {
    this.toolbox = toolbox;
  }

  private TaskStatus runInternal(TaskToolbox toolbox) throws Exception
  {
    setToolbox(toolbox);
    log.info("pubsub attempt");
    PubsubRecordSupplier recordSupplier = task.newTaskRecordSupplier();
    List<ReceivedMessage> records = getRecords(recordSupplier, toolbox);
    log.info("pubsub success");
    log.info(records.size() + "");
    log.info(records.toString());

    // Set up FireDepartmentMetrics
    final FireDepartment fireDepartmentForMetrics = new FireDepartment(
        task.getDataSchema(),
        new RealtimeIOConfig(null, null),
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
        NodeRole.PEON,
        ImmutableMap.of(
            toolbox.getDataNodeService().getName(), toolbox.getDataNodeService(),
            lookupNodeService.getName(), lookupNodeService
        )
    );

    if (appenderatorsManager.shouldTaskMakeNodeAnnouncements()) {
      toolbox.getDataSegmentServerAnnouncer().announce();
      toolbox.getDruidNodeAnnouncer().announce(discoveryDruidNode);
    }
    appenderator = task.newAppenderator(fireDepartmentMetrics, toolbox);
    driver = task.newDriver(appenderator, toolbox, fireDepartmentMetrics);
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
              return toolbox.getTaskActionClient().submit(
                  new TimeChunkLockAcquireAction(
                      TaskLockType.EXCLUSIVE,
                      segmentId.getInterval(),
                      1000L
                  )
              ) != null;
            }
          }
          catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
    );

    // Set up committer.
    final Supplier<Committer> committerSupplier = () -> {
      return new Committer()
      {
        @Override
        public Object getMetadata()
        {
          // Do nothing.
          return ImmutableMap.of();
        }

        @Override
        public void run()
        {
          // Do nothing.
        }
      };
    };
    for (ReceivedMessage record : records) {
      List<InputRow> rows = parseBytes(Arrays.asList(record.getMessage().getData().toByteArray()));
      for (InputRow row : rows) {
        final AppenderatorDriverAddResult addResult = driver.add(
            row,
            getSequenceName(),
            committerSupplier,
            true,
            // do not allow incremental persists to happen until all the rows from this batch
            // of rows are indexed
            false
        );
        if (!addResult.isOk()) {
          throw new ISE("failed to add row %s", row);
        }
      }
    }
    driver.persist(committerSupplier.get());
    publishAndRegisterHandoff(committerSupplier.get());
    driver.close();
    return TaskStatus.success(task.getId());
  }

  public Appenderator getAppenderator()
  {
    return appenderator;
  }

  public String getSequenceName()
  {
    return "pubsub-sequence";
  }

  private void publishAndRegisterHandoff(Committer committer)
  {
    final ListenableFuture<SegmentsAndMetadata> publishFuture = Futures.transform(
        driver.publish(
            new PubsubTransactionalSegmentPublisher(this, toolbox, false),
            committer,
            Arrays.asList(getSequenceName())
        ),
        publishedSegmentsAndMetadata -> {
          if (publishedSegmentsAndMetadata == null) {
            throw new ISE(
                "Transaction failure publishing segments for sequence"
            );
          } else {
            return publishedSegmentsAndMetadata;
          }
        }
    );

    // Create a handoffFuture for every publishFuture. The created handoffFuture must fail if publishFuture fails.
    final SettableFuture<SegmentsAndMetadata> handoffFuture = SettableFuture.create();

    Futures.addCallback(
        publishFuture,
        new FutureCallback<SegmentsAndMetadata>()
        {
          @Override
          public void onSuccess(SegmentsAndMetadata publishedSegmentsAndMetadata)
          {
            log.info(
                "Published segments [%s] for sequence [%s] with metadata [%s].",
                String.join(", ", Lists.transform(publishedSegmentsAndMetadata.getSegments(), DataSegment::toString)),
                getSequenceName(),
                Preconditions.checkNotNull(publishedSegmentsAndMetadata.getCommitMetadata(), "commitMetadata")
            );

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
                          "Failed to hand off segments: %s",
                          String.join(
                              ", ",
                              Lists.transform(publishedSegmentsAndMetadata.getSegments(), DataSegment::toString)
                          )
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
            log.error(t, "Error while publishing segments for sequenceNumber");
            handoffFuture.setException(t);
          }
        }
    );

    try {
      publishFuture.get();
      handoffFuture.get();
    }
    catch (InterruptedException e) {
      log.error(e, "pubsub error");
    }
    catch (ExecutionException e) {
      log.error(e, "pubsub error");
    }
  }

  private List<InputRow> parseBytes(List<byte[]> valueBytess) throws IOException
  {
    if (parser != null) {
      return parseWithParser(valueBytess);
    } else {
      return parseWithInputFormat(valueBytess);
    }
  }

  private List<InputRow> parseWithParser(List<byte[]> valueBytess)
  {
    final List<InputRow> rows = new ArrayList<>();
    for (byte[] valueBytes : valueBytess) {
      rows.addAll(parser.parseBatch(ByteBuffer.wrap(valueBytes)));
    }
    return rows;
  }

  private List<InputRow> parseWithInputFormat(List<byte[]> valueBytess) throws IOException
  {
    final List<InputRow> rows = new ArrayList<>();
    for (byte[] valueBytes : valueBytess) {
      final InputEntityReader reader = task.getDataSchema().getTransformSpec().decorate(
          Preconditions.checkNotNull(inputFormat, "inputFormat").createReader(
              inputRowSchema,
              new ByteEntity(valueBytes),
              toolbox.getIndexingTmpDir()
          )
      );
      try (CloseableIterator<InputRow> rowIterator = reader.read()) {
        rowIterator.forEachRemaining(rows::add);
      }
    }
    return rows;
  }

  @GET
  @Path("/checkpoints")
  @Produces(MediaType.APPLICATION_JSON)
  public Map<Integer, Object> getCheckpointsHTTP(
      @Context final HttpServletRequest req
  )
  {
    // authorizationCheck(req, Action.READ);
    // return getCheckpoints();
    return null;
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
    // authorizationCheck(req, Action.WRITE);
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
      return Response.ok().entity(toolbox.getJsonMapper().writeValueAsString("TODO: res")).build();
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @POST
  @Path("/resume")
  public Response resumeHTTP(@Context final HttpServletRequest req) throws InterruptedException
  {
    // authorizationCheck(req, Action.WRITE);
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
    // authorizationCheck(req, Action.WRITE);
    return startTime;
  }


  public enum Status
  {
    NOT_STARTED,
    STARTING,
    READING,
    PAUSED,
    PUBLISHING
  }
}

