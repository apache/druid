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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.data.input.Committer;
import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.FirehoseFactory;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.LookupNodeService;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.indexer.IngestionState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexing.appenderator.ActionBasedSegmentAllocator;
import org.apache.druid.indexing.appenderator.ActionBasedUsedSegmentChecker;
import org.apache.druid.indexing.common.IngestionStatsAndErrorsTaskReport;
import org.apache.druid.indexing.common.IngestionStatsAndErrorsTaskReportData;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TaskRealtimeMetricsMonitorBuilder;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.SegmentAllocateAction;
import org.apache.druid.indexing.common.actions.SegmentLockAcquireAction;
import org.apache.druid.indexing.common.actions.SegmentTransactionalInsertAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TaskLocks;
import org.apache.druid.indexing.common.actions.TimeChunkLockAcquireAction;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.index.RealtimeAppenderatorIngestionSpec;
import org.apache.druid.indexing.common.index.RealtimeAppenderatorTuningConfig;
import org.apache.druid.indexing.common.stats.TaskRealtimeMetricsMonitor;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.NoopQueryRunner;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.segment.incremental.ParseExceptionHandler;
import org.apache.druid.segment.incremental.ParseExceptionReport;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.RealtimeIOConfig;
import org.apache.druid.segment.realtime.FireDepartment;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;
import org.apache.druid.segment.realtime.appenderator.Appenderator;
import org.apache.druid.segment.realtime.appenderator.AppenderatorDriverAddResult;
import org.apache.druid.segment.realtime.appenderator.SegmentsAndCommitMetadata;
import org.apache.druid.segment.realtime.appenderator.StreamAppenderatorDriver;
import org.apache.druid.segment.realtime.appenderator.TransactionalSegmentPublisher;
import org.apache.druid.segment.realtime.firehose.ChatHandler;
import org.apache.druid.segment.realtime.firehose.ClippedFirehoseFactory;
import org.apache.druid.segment.realtime.firehose.EventReceiverFirehoseFactory;
import org.apache.druid.segment.realtime.firehose.TimedShutoffFirehoseFactory;
import org.apache.druid.segment.realtime.plumber.Committers;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.timeline.partition.NumberedPartialShardSpec;
import org.apache.druid.utils.CloseableUtils;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

import javax.annotation.Nonnull;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Deprecated
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
  private final Queue<ListenableFuture<SegmentsAndCommitMetadata>> pendingHandoffs;

  @JsonIgnore
  private volatile Appenderator appenderator = null;

  @JsonIgnore
  private volatile Firehose firehose = null;

  @JsonIgnore
  private volatile FireDepartmentMetrics metrics = null;

  @JsonIgnore
  private volatile boolean gracefullyStopped = false;

  @JsonIgnore
  private volatile boolean finishingJob = false;

  @JsonIgnore
  private volatile Thread runThread = null;

  @JsonIgnore
  private final LockGranularity lockGranularity;

  @JsonIgnore
  @MonotonicNonNull
  private ParseExceptionHandler parseExceptionHandler;

  @JsonIgnore
  @MonotonicNonNull
  private IngestionState ingestionState;

  @JsonIgnore
  @MonotonicNonNull
  private AuthorizerMapper authorizerMapper;

  @JsonIgnore
  @MonotonicNonNull
  private RowIngestionMeters rowIngestionMeters;

  @JsonIgnore
  @MonotonicNonNull
  private String errorMsg;

  @JsonCreator
  public AppenderatorDriverRealtimeIndexTask(
      @JsonProperty("id") String id,
      @JsonProperty("resource") TaskResource taskResource,
      @JsonProperty("spec") RealtimeAppenderatorIngestionSpec spec,
      @JsonProperty("context") Map<String, Object> context
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

    this.ingestionState = IngestionState.NOT_STARTED;
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
  @JsonIgnore
  @Nonnull
  public Set<ResourceAction> getInputSourceResources() throws UOE
  {
    throw new UOE(StringUtils.format(
        "Task type [%s], does not support input source based security",
        getType()
    ));
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
  public boolean supportsQueries()
  {
    return true;
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient)
  {
    return true;
  }

  @Override
  public TaskStatus runTask(final TaskToolbox toolbox)
  {
    runThread = Thread.currentThread();
    authorizerMapper = toolbox.getAuthorizerMapper();
    rowIngestionMeters = toolbox.getRowIngestionMetersFactory().createRowIngestionMeters();
    parseExceptionHandler = new ParseExceptionHandler(
        rowIngestionMeters,
        spec.getTuningConfig().isLogParseExceptions(),
        spec.getTuningConfig().getMaxParseExceptions(),
        spec.getTuningConfig().getMaxSavedParseExceptions()
    );

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

    DiscoveryDruidNode discoveryDruidNode = createDiscoveryDruidNode(toolbox);

    appenderator = newAppenderator(dataSchema, tuningConfig, metrics, toolbox);
    final TaskLockType lockType = TaskLocks.determineLockTypeForAppend(getContext());
    StreamAppenderatorDriver driver = newDriver(dataSchema, appenderator, toolbox, metrics, lockType);

    try {
      log.debug("Found chat handler of class[%s]", toolbox.getChatHandlerProvider().getClass().getName());
      toolbox.getChatHandlerProvider().register(getId(), this, false);

      if (toolbox.getAppenderatorsManager().shouldTaskMakeNodeAnnouncements()) {
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
                final TaskLock lock = toolbox.getTaskActionClient().submit(
                    new TimeChunkLockAcquireAction(
                        TaskLockType.EXCLUSIVE,
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

      // Set up metrics emission
      toolbox.addMonitor(metricsMonitor);

      // Delay firehose connection to avoid claiming input resources while the plumber is starting up.
      final FirehoseFactory firehoseFactory = spec.getIOConfig().getFirehoseFactory();
      final boolean firehoseDrainableByClosing = isFirehoseDrainableByClosing(firehoseFactory);

      int sequenceNumber = 0;
      String sequenceName = makeSequenceName(getId(), sequenceNumber);

      final TransactionalSegmentPublisher publisher = (mustBeNullOrEmptyOverwriteSegments, segments, commitMetadata) -> {
        if (mustBeNullOrEmptyOverwriteSegments != null && !mustBeNullOrEmptyOverwriteSegments.isEmpty()) {
          throw new ISE(
              "Stream ingestion task unexpectedly attempted to overwrite segments: %s",
              SegmentUtils.commaSeparatedIdentifiers(mustBeNullOrEmptyOverwriteSegments)
          );
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
          firehose = firehoseFactory.connect(
              Preconditions.checkNotNull(spec.getDataSchema().getParser(), "inputRowParser"),
              toolbox.getIndexingTmpDir()
          );
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
      toolbox.getChatHandlerProvider().unregister(getId());

      CloseableUtils.closeAndSuppressExceptions(firehose, e -> log.warn("Failed to close Firehose"));
      appenderator.close();
      CloseableUtils.closeAndSuppressExceptions(driver, e -> log.warn("Failed to close AppenderatorDriver"));

      toolbox.removeMonitor(metricsMonitor);

      if (toolbox.getAppenderatorsManager().shouldTaskMakeNodeAnnouncements()) {
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
    List<ParseExceptionReport> events = IndexTaskUtils.getReportListFromSavedParseExceptions(
        parseExceptionHandler.getSavedParseExceptionReports()
    );
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

  /**
   * Return a map of reports for the task.
   *
   * A successfull task should always have a null errorMsg. A falied task should always have a non-null
   * errorMsg.
   *
   * @return Map of reports for the task.
   */
  private Map<String, TaskReport> getTaskCompletionReports()
  {
    return TaskReport.buildTaskReports(
        new IngestionStatsAndErrorsTaskReport(
            getId(),
            new IngestionStatsAndErrorsTaskReportData(
                ingestionState,
                getTaskCompletionUnparseableEvents(),
                getTaskCompletionRowStats(),
                errorMsg,
                errorMsg == null,
                0L,
                Collections.emptyMap()
            )
        )
    );
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
    Map<String, Object> metricsMap = new HashMap<>();
    metricsMap.put(
        RowIngestionMeters.BUILD_SEGMENTS,
        rowIngestionMeters.getTotals()
    );
    return metricsMap;
  }

  private void handleParseException(ParseException pe)
  {
    parseExceptionHandler.handle(pe);

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
    final ListenableFuture<SegmentsAndCommitMetadata> publishFuture = driver.publish(
        publisher,
        committerSupplier.get(),
        Collections.singletonList(sequenceName)
    );
    pendingHandoffs.add(Futures.transformAsync(
        publishFuture,
        driver::registerHandoff,
        MoreExecutors.directExecutor()
    ));
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
        NodeRole.PEON,
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
    return toolbox.getAppenderatorsManager().createRealtimeAppenderatorForTask(
        null,
        getId(),
        dataSchema,
        tuningConfig.withBasePersistDirectory(toolbox.getPersistDir()),
        metrics,
        toolbox.getSegmentPusher(),
        toolbox.getJsonMapper(),
        toolbox.getIndexIO(),
        toolbox.getIndexMergerV9(),
        toolbox.getQueryRunnerFactoryConglomerate(),
        toolbox.getSegmentAnnouncer(),
        toolbox.getEmitter(),
        toolbox.getQueryProcessingPool(),
        toolbox.getJoinableFactory(),
        toolbox.getCache(),
        toolbox.getCacheConfig(),
        toolbox.getCachePopulatorStats(),
        rowIngestionMeters,
        parseExceptionHandler,
        isUseMaxMemoryEstimates(),
        toolbox.getCentralizedTableSchemaConfig()
    );
  }

  private static StreamAppenderatorDriver newDriver(
      final DataSchema dataSchema,
      final Appenderator appenderator,
      final TaskToolbox toolbox,
      final FireDepartmentMetrics metrics,
      final TaskLockType lockType
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
                NumberedPartialShardSpec.instance(),
                LockGranularity.TIME_CHUNK,
                lockType
            )
        ),
        toolbox.getSegmentHandoffNotifierFactory(),
        new ActionBasedUsedSegmentChecker(toolbox.getTaskActionClient()),
        toolbox.getDataSegmentKiller(),
        toolbox.getJsonMapper(),
        metrics
    );
  }

  private static String makeSequenceName(String taskId, int sequenceNumber)
  {
    return taskId + "_" + sequenceNumber;
  }
}

