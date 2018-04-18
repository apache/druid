/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.druid.data.input.Committer;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.discovery.DiscoveryDruidNode;
import io.druid.discovery.DruidNodeDiscoveryProvider;
import io.druid.discovery.LookupNodeService;
import io.druid.indexer.IngestionState;
import io.druid.indexer.TaskMetricsGetter;
import io.druid.indexing.appenderator.ActionBasedSegmentAllocator;
import io.druid.indexing.appenderator.ActionBasedUsedSegmentChecker;
import io.druid.indexing.common.IngestionStatsAndErrorsTaskReport;
import io.druid.indexing.common.IngestionStatsAndErrorsTaskReportData;
import io.druid.indexing.common.TaskRealtimeMetricsMonitorBuilder;
import io.druid.indexing.common.TaskReport;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.SegmentTransactionalInsertAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.common.index.RealtimeAppenderatorIngestionSpec;
import io.druid.indexing.common.index.RealtimeAppenderatorTuningConfig;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.concurrent.ListenableFutures;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.java.util.common.parsers.ParseException;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.query.NoopQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeIOConfig;
import io.druid.segment.realtime.FireDepartment;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.FireDepartmentMetricsTaskMetricsGetter;
import io.druid.segment.realtime.RealtimeMetricsMonitor;
import io.druid.segment.realtime.appenderator.Appenderator;
import io.druid.segment.realtime.appenderator.AppenderatorDriverAddResult;
import io.druid.segment.realtime.appenderator.Appenderators;
import io.druid.segment.realtime.appenderator.SegmentsAndMetadata;
import io.druid.segment.realtime.appenderator.StreamAppenderatorDriver;
import io.druid.segment.realtime.appenderator.TransactionalSegmentPublisher;
import io.druid.segment.realtime.firehose.ChatHandler;
import io.druid.segment.realtime.firehose.ChatHandlerProvider;
import io.druid.segment.realtime.firehose.ClippedFirehoseFactory;
import io.druid.segment.realtime.firehose.EventReceiverFirehoseFactory;
import io.druid.segment.realtime.firehose.TimedShutoffFirehoseFactory;
import io.druid.segment.realtime.plumber.Committers;
import io.druid.server.security.Action;
import io.druid.server.security.AuthorizerMapper;
import io.druid.utils.CircularBuffer;
import org.apache.commons.io.FileUtils;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
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
  private static final Random random = new Random();

  private static String makeTaskId(RealtimeAppenderatorIngestionSpec spec)
  {
    final StringBuilder suffix = new StringBuilder(8);
    for (int i = 0; i < Integer.BYTES * 2; ++i) {
      suffix.append((char) ('a' + ((random.nextInt() >>> (i * 4)) & 0x0F)));
    }
    return StringUtils.format(
        "index_realtime_%s_%d_%s_%s",
        spec.getDataSchema().getDataSource(),
        spec.getTuningConfig().getShardSpec().getPartitionNum(),
        DateTimes.nowUtc(),
        suffix
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
  private TaskMetricsGetter metricsGetter;

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
  private IngestionState ingestionState;

  @JsonIgnore
  private String errorMsg;

  @JsonCreator
  public AppenderatorDriverRealtimeIndexTask(
      @JsonProperty("id") String id,
      @JsonProperty("resource") TaskResource taskResource,
      @JsonProperty("spec") RealtimeAppenderatorIngestionSpec spec,
      @JsonProperty("context") Map<String, Object> context,
      @JacksonInject ChatHandlerProvider chatHandlerProvider,
      @JacksonInject AuthorizerMapper authorizerMapper
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
  public TaskStatus run(final TaskToolbox toolbox) throws Exception
  {
    runThread = Thread.currentThread();

    setupTimeoutAlert();

    DataSchema dataSchema = spec.getDataSchema();
    RealtimeAppenderatorTuningConfig tuningConfig = spec.getTuningConfig()
                                                        .withBasePersistDirectory(toolbox.getPersistDir());

    final FireDepartment fireDepartmentForMetrics = new FireDepartment(
        dataSchema, new RealtimeIOConfig(null, null, null), null
    );

    final RealtimeMetricsMonitor metricsMonitor = TaskRealtimeMetricsMonitorBuilder.build(this, fireDepartmentForMetrics);

    this.metrics = fireDepartmentForMetrics.getMetrics();
    metricsGetter = new FireDepartmentMetricsTaskMetricsGetter(metrics);

    Supplier<Committer> committerSupplier = null;
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

      toolbox.getDataSegmentServerAnnouncer().announce();
      toolbox.getDruidNodeAnnouncer().announce(discoveryDruidNode);

      driver.startJob();

      // Set up metrics emission
      toolbox.getMonitorScheduler().addMonitor(metricsMonitor);

      // Firehose temporary directory is automatically removed when this RealtimeIndexTask completes.
      FileUtils.forceMkdir(firehoseTempDir);

      // Delay firehose connection to avoid claiming input resources while the plumber is starting up.
      final FirehoseFactory firehoseFactory = spec.getIOConfig().getFirehoseFactory();
      final boolean firehoseDrainableByClosing = isFirehoseDrainableByClosing(firehoseFactory);

      int sequenceNumber = 0;
      String sequenceName = makeSequenceName(getId(), sequenceNumber);

      final TransactionalSegmentPublisher publisher = (segments, commitMetadata) -> {
        final SegmentTransactionalInsertAction action = new SegmentTransactionalInsertAction(segments);
        return toolbox.getTaskActionClient().submit(action).isSuccess();
      };

      // Skip connecting firehose if we've been stopped before we got started.
      synchronized (this) {
        if (!gracefullyStopped) {
          firehose = firehoseFactory.connect(spec.getDataSchema().getParser(), firehoseTempDir);
          committerSupplier = Committers.supplierFromFirehose(firehose);
        }
      }

      ingestionState = IngestionState.BUILD_SEGMENTS;

      // Time to read data!
      while (!gracefullyStopped && firehoseDrainableByClosing && firehose.hasMore()) {
        try {
          InputRow inputRow = firehose.nextRow();

          if (inputRow == null) {
            log.debug("Discarded null row, considering thrownAway.");
            metrics.incrementThrownAway();
          } else {
            AppenderatorDriverAddResult addResult = driver.add(inputRow, sequenceName, committerSupplier);

            if (addResult.isOk()) {
              if (addResult.getNumRowsInSegment() > tuningConfig.getMaxRowsPerSegment()) {
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
              metrics.incrementProcessed();
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
      toolbox.getTaskReportFileWriter().write(getTaskCompletionReports());
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
      CloseQuietly.close(appenderator);
      CloseQuietly.close(driver);

      toolbox.getMonitorScheduler().removeMonitor(metricsMonitor);

      toolbox.getDataSegmentServerAnnouncer().unannounce();
      toolbox.getDruidNodeAnnouncer().unannounce(discoveryDruidNode);
    }

    log.info("Job done!");
    toolbox.getTaskReportFileWriter().write(getTaskCompletionReports());
    return TaskStatus.success(getId());
  }

  @Override
  public boolean canRestore()
  {
    return true;
  }

  @Override
  public void stopGracefully()
  {
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
      throw Throwables.propagate(e);
    }
  }

  /**
   * Public for tests.
   */
  @JsonIgnore
  public Firehose getFirehose()
  {
    return firehose;
  }

  /**
   * Public for tests.
   */
  @JsonIgnore
  public FireDepartmentMetrics getMetrics()
  {
    return metrics;
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
    Map<String, Object> returnMap = Maps.newHashMap();
    Map<String, Object> totalsMap = Maps.newHashMap();

    if (metricsGetter != null) {
      totalsMap.put(
          "buildSegments",
          metricsGetter.getTotalMetrics()
      );
    }

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
    Map<String, Object> unparseableEventsMap = Maps.newHashMap();
    List<String> buildSegmentsParseExceptionMessages = IndexTaskUtils.getMessagesFromSavedParseExceptions(savedParseExceptions);
    if (buildSegmentsParseExceptionMessages != null) {
      unparseableEventsMap.put("buildSegments", buildSegmentsParseExceptionMessages);
    }
    return unparseableEventsMap;
  }

  private Map<String, Object> getTaskCompletionRowStats()
  {
    Map<String, Object> metricsMap = Maps.newHashMap();
    if (metricsGetter != null) {
      metricsMap.put(
          "buildSegments",
          metricsGetter.getTotalMetrics()
      );
    }
    return metricsMap;
  }

  private void handleParseException(ParseException pe)
  {
    if (pe.isFromPartiallyValidRow()) {
      metrics.incrementProcessedWithErrors();
    } else {
      metrics.incrementUnparseable();
    }

    if (spec.getTuningConfig().isLogParseExceptions()) {
      log.error(pe, "Encountered parse exception: ");
    }

    if (savedParseExceptions != null) {
      savedParseExceptions.add(pe);
    }

    if (metrics.unparseable() + metrics.processedWithErrors()
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
        DruidNodeDiscoveryProvider.NODE_TYPE_PEON,
        ImmutableMap.of(
            toolbox.getDataNodeService().getName(), toolbox.getDataNodeService(),
            lookupNodeService.getName(), lookupNodeService
        )
    );
  }

  private static Appenderator newAppenderator(
      final DataSchema dataSchema,
      final RealtimeAppenderatorTuningConfig tuningConfig,
      final FireDepartmentMetrics metrics,
      final TaskToolbox toolbox
  )
  {
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
        toolbox.getCacheConfig()
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
        new ActionBasedSegmentAllocator(toolbox.getTaskActionClient(), dataSchema),
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

