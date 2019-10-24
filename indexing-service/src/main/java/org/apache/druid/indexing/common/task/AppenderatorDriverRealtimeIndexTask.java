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

package org.apache.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.io.FileUtils;
import org.apache.druid.data.input.Committer;
import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.FirehoseFactory;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.LookupNodeService;
import org.apache.druid.discovery.NodeType;
import org.apache.druid.indexer.IngestionState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexing.appenderator.ActionBasedSegmentAllocator;
import org.apache.druid.indexing.appenderator.ActionBasedUsedSegmentChecker;
import org.apache.druid.indexing.common.IngestionStatsAndErrorsTaskReport;
import org.apache.druid.indexing.common.IngestionStatsAndErrorsTaskReportData;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TaskRealtimeMetricsMonitorBuilder;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.SegmentAllocateAction;
import org.apache.druid.indexing.common.actions.SegmentLockAcquireAction;
import org.apache.druid.indexing.common.actions.SegmentTransactionalInsertAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TimeChunkLockAcquireAction;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.index.RealtimeAppenderatorIngestionSpec;
import org.apache.druid.indexing.common.index.RealtimeAppenderatorTuningConfig;
import org.apache.druid.indexing.common.stats.RowIngestionMeters;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.common.stats.TaskRealtimeMetricsMonitor;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.ListenableFutures;
import org.apache.druid.java.util.common.guava.CloseQuietly;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.NoopQueryRunner;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.RealtimeIOConfig;
import org.apache.druid.segment.realtime.FireDepartment;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;
import org.apache.druid.segment.realtime.appenderator.Appenderator;
import org.apache.druid.segment.realtime.appenderator.AppenderatorDriverAddResult;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.segment.realtime.appenderator.SegmentsAndMetadata;
import org.apache.druid.segment.realtime.appenderator.StreamAppenderatorDriver;
import org.apache.druid.segment.realtime.appenderator.TransactionalSegmentPublisher;
import org.apache.druid.segment.realtime.firehose.ChatHandler;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.segment.realtime.firehose.ClippedFirehoseFactory;
import org.apache.druid.segment.realtime.firehose.EventReceiverFirehoseFactory;
import org.apache.druid.segment.realtime.firehose.TimedShutoffFirehoseFactory;
import org.apache.druid.segment.realtime.plumber.Committers;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.timeline.partition.NumberedShardSpecFactory;
import org.apache.druid.utils.CircularBuffer;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class AppenderatorDriverRealtimeIndexTask extends AbstractTask implements ChatHandler
{
  private static final String CTX_KEY_LOOKUP_TIER = "lookupTier";

  private static final EmittingLogger log = new EmittingLogger(RealtimeIndexTask.class);

  private static String makeTaskId(RealtimeAppenderatorIngestionSpec spec)
  {
    return StringUtils.format(
        "index_realtime_%s_%d_%s_%s",
        spec.getDataSchema().getDataSource(),
        spec.getTuningConfig().getShardSpec().getPartitionNum(),
        DateTimes.nowUtc(),
        RealtimeIndexTask.makeRandomId()
    );
  }

  @JsonIgnore
  private final RealtimeAppenderatorIngestionSpec spec;

  @JsonIgnore
  private final Queue<ListenableFuture<SegmentsAndMetadata>> pendingHandoffs;

  @JsonIgnore
  private volatile Appenderator appenderator = null;

  @JsonIgnore
  private volatile Firehose firehose = null;

  @JsonIgnore
  private volatile FireDepartmentMetrics metrics = null;

  @JsonIgnore
  private final RowIngestionMeters rowIngestionMeters;

  @JsonIgnore
  private volatile boolean gracefullyStopped = false;

  @JsonIgnore
  private volatile boolean finishingJob = false;

  @JsonIgnore
  private volatile Thread runThread = null;

  @JsonIgnore
  private CircularBuffer<Throwable> savedParseExceptions;

  @JsonIgnore
  private final Optional<ChatHandlerProvider> chatHandlerProvider;

  @JsonIgnore
  private final AuthorizerMapper authorizerMapper;

  @JsonIgnore
  private final LockGranularity lockGranularity;

  @JsonIgnore
  private IngestionState ingestionState;

  @JsonIgnore
  private String errorMsg;

  @JsonIgnore
  private AppenderatorsManager appenderatorsManager;

  @JsonCreator
  public AppenderatorDriverRealtimeIndexTask(
      @JsonProperty("id") String id,
      @JsonProperty("resource") TaskResource taskResource,
      @JsonProperty("spec") RealtimeAppenderatorIngestionSpec spec,
      @JsonProperty("context") Map<String, Object> context,
      @JacksonInject ChatHandlerProvider chatHandlerProvider,
      @JacksonInject AuthorizerMapper authorizerMapper,
      @JacksonInject RowIngestionMetersFactory rowIngestionMetersFactory,
      @JacksonInject AppenderatorsManager appenderatorsManager
  )
  {
    super(
        id == null ? makeTaskId(spec) : id,
        StringUtils.format("index_realtime_appenderator_%s", spec.getDataSchema().getDataSource()),
        taskResource,
        spec.getDataSchema().getDataSource(),
        context
    );
    this.spec = spec;
    this.pendingHandoffs = new ConcurrentLinkedQueue<>();
    this.chatHandlerProvider = Optional.fromNullable(chatHandlerProvider);
    this.authorizerMapper = authorizerMapper;

    if (spec.getTuningConfig().getMaxSavedParseExceptions() > 0) {
      savedParseExceptions = new CircularBuffer<>(spec.getTuningConfig().getMaxSavedParseExceptions());
    }

    this.ingestionState = IngestionState.NOT_STARTED;
    this.rowIngestionMeters = rowIngestionMetersFactory.createRowIngestionMeters();
    this.appenderatorsManager = appenderatorsManager;
    this.lockGranularity = getContextValue(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, Tasks.DEFAULT_FORCE_TIME_CHUNK_LOCK)
                           ? LockGranularity.TIME_CHUNK
                           : LockGranularity.SEGMENT;
  }

  @Override
  public int getPriority()
  {
    return getContextValue(Tasks.PRIORITY_KEY, Tasks.DEFAULT_REALTIME_TASK_PRIORITY);
  }

  @Override
  public String getType()
  {
    return "index_realtime_appenderator";
  }

  @Override
  public String getNodeType()
  {
    return "realtime";
  }

  @Override
  public <T> QueryRunner<T> getQueryRunner(Query<T> query)
  {
    if (appenderator == null) {
      // Not yet initialized, no data yet, just return a noop runner.
      return new NoopQueryRunner<>();
    }

    return (queryPlus, responseContext) -> queryPlus.run(appenderator, responseContext);
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient)
  {
    return true;
  }

  @Override
  public TaskStatus run(final TaskToolbox toolbox)
  {
    runThread = Thread.currentThread();

    setupTimeoutAlert();

    DataSchema dataSchema = spec.getDataSchema();
    RealtimeAppenderatorTuningConfig tuningConfig = spec.getTuningConfig()
                                                        .withBasePersistDirectory(toolbox.getPersistDir());

    final FireDepartment fireDepartmentForMetrics =
        new FireDepartment(dataSchema, new RealtimeIOConfig(null, null), null);

    final TaskRealtimeMetricsMonitor metricsMonitor = TaskRealtimeMetricsMonitorBuilder.build(
        this,
        fireDepartmentForMetrics,
        rowIngestionMeters
    );

    this.metrics = fireDepartmentForMetrics.getMetrics();

    final Supplier<Committer> committerSupplier = Committers.nilSupplier();
    final File firehoseTempDir = toolbox.getFirehoseTemporaryDir();

    DiscoveryDruidNode discoveryDruidNode = createDiscoveryDruidNode(toolbox);

    appenderator = newAppenderator(dataSchema, tuningConfig, metrics, toolbox);
    StreamAppenderatorDriver driver = newDriver(dataSchema, appenderator, toolbox, metrics);

    try {
      if (chatHandlerProvider.isPresent()) {
        log.info("Found chat handler of class[%s]", chatHandlerProvider.get().getClass().getName());
        chatHandlerProvider.get().register(getId(), this, false);
      } else {
        log.warn("No chat handler detected");
      }

      if (appenderatorsManager.shouldTaskMakeNodeAnnouncements()) {
        toolbox.getDataSegmentServerAnnouncer().announce();
        toolbox.getDruidNodeAnnouncer().announce(discoveryDruidNode);
      }

      driver.startJob(
          segmentId -> {
            try {
              if (lockGranularity == LockGranularity.SEGMENT) {
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

      // Set up metrics emission
      toolbox.getMonitorScheduler().addMonitor(metricsMonitor);

      // Firehose temporary directory is automatically removed when this RealtimeIndexTask completes.
      FileUtils.forceMkdir(firehoseTempDir);

      // Delay firehose connection to avoid claiming input resources while the plumber is starting up.
      final FirehoseFactory firehoseFactory = spec.getIOConfig().getFirehoseFactory();
      final boolean firehoseDrainableByClosing = isFirehoseDrainableByClosing(firehoseFactory);

      int sequenceNumber = 0;
      String sequenceName = makeSequenceName(getId(), sequenceNumber);

      final TransactionalSegmentPublisher publisher = (mustBeNullOrEmptySegments, segments, commitMetadata) -> {
        if (mustBeNullOrEmptySegments != null && !mustBeNullOrEmptySegments.isEmpty()) {
          throw new ISE("WTH? stream ingestion tasks are overwriting segments[%s]", mustBeNullOrEmptySegments);
        }
        final SegmentTransactionalInsertAction action = SegmentTransactionalInsertAction.appendAction(
            segments,
            null,
            null
        );
        return toolbox.getTaskActionClient().submit(action);
      };

      // Skip connecting firehose if we've been stopped before we got started.
      synchronized (this) {
        if (!gracefullyStopped) {
          firehose = firehoseFactory.connect(spec.getDataSchema().getParser(), firehoseTempDir);
        }
      }

      ingestionState = IngestionState.BUILD_SEGMENTS;

      // Time to read data!
      while (!gracefullyStopped && firehoseDrainableByClosing && firehose.hasMore()) {
        try {
          InputRow inputRow = firehose.nextRow();

          if (inputRow == null) {
            log.debug("Discarded null row, considering thrownAway.");
            rowIngestionMeters.incrementThrownAway();
          } else {
            AppenderatorDriverAddResult addResult = driver.add(inputRow, sequenceName, committerSupplier);

            if (addResult.isOk()) {
              final boolean isPushRequired = addResult.isPushRequired(
                  tuningConfig.getPartitionsSpec().getMaxRowsPerSegment(),
                  tuningConfig.getPartitionsSpec().getMaxTotalRowsOr(DynamicPartitionsSpec.DEFAULT_MAX_TOTAL_ROWS)
              );
              if (isPushRequired) {
                publishSegments(driver, publisher, committerSupplier, sequenceName);
                sequenceNumber++;
                sequenceName = makeSequenceName(getId(), sequenceNumber);
              }
            } else {
              // Failure to allocate segment puts determinism at risk, bail out to be safe.
              // May want configurable behavior here at some point.
              // If we allow continuing, then consider blacklisting the interval for a while to avoid constant checks.
              throw new ISE("Could not allocate segment for row with timestamp[%s]", inputRow.getTimestamp());
            }

            if (addResult.getParseException() != null) {
              handleParseException(addResult.getParseException());
            } else {
              rowIngestionMeters.incrementProcessed();
            }
          }
        }
        catch (ParseException e) {
          handleParseException(e);
        }
      }

      ingestionState = IngestionState.COMPLETED;

      if (!gracefullyStopped) {
        synchronized (this) {
          if (gracefullyStopped) {
            // Someone called stopGracefully after we checked the flag. That's okay, just stop now.
            log.info("Gracefully stopping.");
          } else {
            finishingJob = true;
          }
        }

        if (finishingJob) {
          log.info("Finishing job...");
          // Publish any remaining segments
          publishSegments(driver, publisher, committerSupplier, sequenceName);

          waitForSegmentPublishAndHandoff(tuningConfig.getPublishAndHandoffTimeout());
        }
      } else if (firehose != null) {
        log.info("Task was gracefully stopped, will persist data before exiting");

        persistAndWait(driver, committerSupplier.get());
      }
    }
    catch (Throwable e) {
      log.makeAlert(e, "Exception aborted realtime processing[%s]", dataSchema.getDataSource())
         .emit();
      errorMsg = Throwables.getStackTraceAsString(e);
      toolbox.getTaskReportFileWriter().write(getId(), getTaskCompletionReports());
      return TaskStatus.failure(
          getId(),
          errorMsg
      );
    }
    finally {
      if (chatHandlerProvider.isPresent()) {
        chatHandlerProvider.get().unregister(getId());
      }

      CloseQuietly.close(firehose);
      appenderator.close();
      CloseQuietly.close(driver);

      toolbox.getMonitorScheduler().removeMonitor(metricsMonitor);

      if (appenderatorsManager.shouldTaskMakeNodeAnnouncements()) {
        toolbox.getDataSegmentServerAnnouncer().unannounce();
        toolbox.getDruidNodeAnnouncer().unannounce(discoveryDruidNode);
      }
    }

    log.info("Job done!");
    toolbox.getTaskReportFileWriter().write(getId(), getTaskCompletionReports());
    return TaskStatus.success(getId());
  }

  @Override
  public boolean canRestore()
  {
    return true;
  }

  @Override
  public void stopGracefully(TaskConfig taskConfig)
  {
    if (taskConfig.isRestoreTasksOnRestart()) {
      try {
        synchronized (this) {
          if (!gracefullyStopped) {
            gracefullyStopped = true;
            if (firehose == null) {
              log.info("stopGracefully: Firehose not started yet, so nothing to stop.");
            } else if (finishingJob) {
              log.info("stopGracefully: Interrupting finishJob.");
              runThread.interrupt();
            } else if (isFirehoseDrainableByClosing(spec.getIOConfig().getFirehoseFactory())) {
              log.info("stopGracefully: Draining firehose.");
              firehose.close();
            } else {
              log.info("stopGracefully: Cannot drain firehose by closing, interrupting run thread.");
              runThread.interrupt();
            }
          }
        }
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    } else {
      synchronized (this) {
        if (!gracefullyStopped) {
          // If task restore is not enabled, just interrupt immediately.
          gracefullyStopped = true;
          runThread.interrupt();
        }
      }
    }
  }

  /**
   * Public for tests.
   */
  @JsonIgnore
  @VisibleForTesting
  public Firehose getFirehose()
  {
    return firehose;
  }

  /**
   * Public for tests.
   */
  @JsonIgnore
  @VisibleForTesting
  public FireDepartmentMetrics getMetrics()
  {
    return metrics;
  }

  @JsonIgnore
  @VisibleForTesting
  public RowIngestionMeters getRowIngestionMeters()
  {
    return rowIngestionMeters;
  }

  @JsonProperty("spec")
  public RealtimeAppenderatorIngestionSpec getSpec()
  {
    return spec;
  }


  @GET
  @Path("/rowStats")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getRowStats(
      @Context final HttpServletRequest req
  )
  {
    IndexTaskUtils.datasourceAuthorizationCheck(req, Action.READ, getDataSource(), authorizerMapper);
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
    IndexTaskUtils.datasourceAuthorizationCheck(req, Action.READ, getDataSource(), authorizerMapper);
    List<String> events = IndexTaskUtils.getMessagesFromSavedParseExceptions(savedParseExceptions);
    return Response.ok(events).build();
  }

  /**
   * Is a firehose from this factory drainable by closing it? If so, we should drain on stopGracefully rather than
   * abruptly stopping.
   * <p>
   * This is a hack to get around the fact that the Firehose and FirehoseFactory interfaces do not help us do this.
   * <p>
   * Protected for tests.
   */
  protected boolean isFirehoseDrainableByClosing(FirehoseFactory firehoseFactory)
  {
    return firehoseFactory instanceof EventReceiverFirehoseFactory
           || (firehoseFactory instanceof TimedShutoffFirehoseFactory
               && isFirehoseDrainableByClosing(((TimedShutoffFirehoseFactory) firehoseFactory).getDelegateFactory()))
           || (firehoseFactory instanceof ClippedFirehoseFactory
               && isFirehoseDrainableByClosing(((ClippedFirehoseFactory) firehoseFactory).getDelegate()));
  }

  private Map<String, TaskReport> getTaskCompletionReports()
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
    Map<String, Object> unparseableEventsMap = new HashMap<>();
    List<String> buildSegmentsParseExceptionMessages = IndexTaskUtils.getMessagesFromSavedParseExceptions(
        savedParseExceptions);
    if (buildSegmentsParseExceptionMessages != null) {
      unparseableEventsMap.put(RowIngestionMeters.BUILD_SEGMENTS, buildSegmentsParseExceptionMessages);
    }
    return unparseableEventsMap;
  }

  private Map<String, Object> getTaskCompletionRowStats()
  {
    Map<String, Object> metricsMap = new HashMap<>();
    metricsMap.put(
        RowIngestionMeters.BUILD_SEGMENTS,
        rowIngestionMeters.getTotals()
    );
    return metricsMap;
  }

  private void handleParseException(ParseException pe)
  {
    if (pe.isFromPartiallyValidRow()) {
      rowIngestionMeters.incrementProcessedWithError();
    } else {
      rowIngestionMeters.incrementUnparseable();
    }

    if (spec.getTuningConfig().isLogParseExceptions()) {
      log.error(pe, "Encountered parse exception: ");
    }

    if (savedParseExceptions != null) {
      savedParseExceptions.add(pe);
    }

    if (rowIngestionMeters.getUnparseable() + rowIngestionMeters.getProcessedWithError()
        > spec.getTuningConfig().getMaxParseExceptions()) {
      log.error("Max parse exceptions exceeded, terminating task...");
      throw new RuntimeException("Max parse exceptions exceeded, terminating task...");
    }
  }

  private void setupTimeoutAlert()
  {
    if (spec.getTuningConfig().getAlertTimeout() > 0) {
      Timer timer = new Timer("RealtimeIndexTask-Timer", true);
      timer.schedule(
          new TimerTask()
          {
            @Override
            public void run()
            {
              log.makeAlert(
                  "RealtimeIndexTask for dataSource [%s] hasn't finished in configured time [%d] ms.",
                  spec.getDataSchema().getDataSource(),
                  spec.getTuningConfig().getAlertTimeout()
              ).emit();
            }
          },
          spec.getTuningConfig().getAlertTimeout()
      );
    }
  }

  private void publishSegments(
      StreamAppenderatorDriver driver,
      TransactionalSegmentPublisher publisher,
      Supplier<Committer> committerSupplier,
      String sequenceName
  )
  {
    final ListenableFuture<SegmentsAndMetadata> publishFuture = driver.publish(
        publisher,
        committerSupplier.get(),
        Collections.singletonList(sequenceName)
    );
    pendingHandoffs.add(ListenableFutures.transformAsync(publishFuture, driver::registerHandoff));
  }

  private void waitForSegmentPublishAndHandoff(long timeout) throws InterruptedException, ExecutionException,
                                                                    TimeoutException
  {
    if (!pendingHandoffs.isEmpty()) {
      ListenableFuture<?> allHandoffs = Futures.allAsList(pendingHandoffs);
      log.info("Waiting for handoffs");


      if (timeout > 0) {
        allHandoffs.get(timeout, TimeUnit.MILLISECONDS);
      } else {
        allHandoffs.get();
      }
    }
  }

  private void persistAndWait(StreamAppenderatorDriver driver, Committer committer)
  {
    try {
      final CountDownLatch persistLatch = new CountDownLatch(1);
      driver.persist(
          new Committer()
          {
            @Override
            public Object getMetadata()
            {
              return committer.getMetadata();
            }

            @Override
            public void run()
            {
              try {
                committer.run();
              }
              finally {
                persistLatch.countDown();
              }
            }
          }
      );
      persistLatch.await();
    }
    catch (InterruptedException e) {
      log.debug(e, "Interrupted while finishing the job");
    }
    catch (Exception e) {
      log.makeAlert(e, "Failed to finish realtime task").emit();
      throw e;
    }
  }

  private DiscoveryDruidNode createDiscoveryDruidNode(TaskToolbox toolbox)
  {
    LookupNodeService lookupNodeService = getContextValue(CTX_KEY_LOOKUP_TIER) == null ?
                                          toolbox.getLookupNodeService() :
                                          new LookupNodeService(getContextValue(CTX_KEY_LOOKUP_TIER));
    return new DiscoveryDruidNode(
        toolbox.getDruidNode(),
        NodeType.PEON,
        ImmutableMap.of(
            toolbox.getDataNodeService().getName(), toolbox.getDataNodeService(),
            lookupNodeService.getName(), lookupNodeService
        )
    );
  }

  private Appenderator newAppenderator(
      final DataSchema dataSchema,
      final RealtimeAppenderatorTuningConfig tuningConfig,
      final FireDepartmentMetrics metrics,
      final TaskToolbox toolbox
  )
  {
    return appenderatorsManager.createRealtimeAppenderatorForTask(
        getId(),
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

  private static StreamAppenderatorDriver newDriver(
      final DataSchema dataSchema,
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
                skipSegmentLineageCheck,
                NumberedShardSpecFactory.instance(),
                LockGranularity.TIME_CHUNK
            )
        ),
        toolbox.getSegmentHandoffNotifierFactory(),
        new ActionBasedUsedSegmentChecker(toolbox.getTaskActionClient()),
        toolbox.getDataSegmentKiller(),
        toolbox.getObjectMapper(),
        metrics
    );
  }

  private static String makeSequenceName(String taskId, int sequenceNumber)
  {
    return taskId + "_" + sequenceNumber;
  }
}

