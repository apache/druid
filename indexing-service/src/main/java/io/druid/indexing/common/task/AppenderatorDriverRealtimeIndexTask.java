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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.druid.data.input.Committer;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.discovery.DiscoveryDruidNode;
import io.druid.discovery.DruidNodeDiscoveryProvider;
import io.druid.discovery.LookupNodeService;
import io.druid.indexing.appenderator.ActionBasedSegmentAllocator;
import io.druid.indexing.appenderator.ActionBasedUsedSegmentChecker;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.SegmentTransactionalInsertAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.common.index.RealtimeAppenderatorIngestionSpec;
import io.druid.indexing.common.index.RealtimeAppenderatorTuningConfig;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.java.util.common.parsers.ParseException;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.query.DruidMetrics;
import io.druid.query.NoopQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeIOConfig;
import io.druid.segment.realtime.FireDepartment;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.RealtimeMetricsMonitor;
import io.druid.segment.realtime.appenderator.Appenderator;
import io.druid.segment.realtime.appenderator.AppenderatorDriver;
import io.druid.segment.realtime.appenderator.AppenderatorDriverAddResult;
import io.druid.segment.realtime.appenderator.Appenderators;
import io.druid.segment.realtime.appenderator.SegmentsAndMetadata;
import io.druid.segment.realtime.appenderator.TransactionalSegmentPublisher;
import io.druid.segment.realtime.firehose.ClippedFirehoseFactory;
import io.druid.segment.realtime.firehose.EventReceiverFirehoseFactory;
import io.druid.segment.realtime.firehose.TimedShutoffFirehoseFactory;
import io.druid.segment.realtime.plumber.Committers;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.Collections;
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

public class AppenderatorDriverRealtimeIndexTask extends AbstractTask
{
  private static final String CTX_KEY_LOOKUP_TIER = "lookupTier";

  private static final EmittingLogger log = new EmittingLogger(RealtimeIndexTask.class);
  private static final Random random = new Random();

  private static String makeTaskId(RealtimeAppenderatorIngestionSpec spec)
  {
    final StringBuilder suffix = new StringBuilder(8);
    for (int i = 0; i < Ints.BYTES * 2; ++i) {
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
  private volatile boolean gracefullyStopped = false;

  @JsonIgnore
  private volatile boolean finishingJob = false;

  @JsonIgnore
  private volatile Thread runThread = null;

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

    final RealtimeMetricsMonitor metricsMonitor = new RealtimeMetricsMonitor(
        ImmutableList.of(fireDepartmentForMetrics),
        ImmutableMap.of(
            DruidMetrics.TASK_ID, new String[]{getId()}
        )
    );

    this.metrics = fireDepartmentForMetrics.getMetrics();

    Supplier<Committer> committerSupplier = null;
    final File firehoseTempDir = toolbox.getFirehoseTemporaryDir();

    DiscoveryDruidNode discoveryDruidNode = createDiscoveryDruidNode(toolbox);

    appenderator = newAppenderator(dataSchema, tuningConfig, metrics, toolbox);
    AppenderatorDriver driver = newDriver(dataSchema, appenderator, toolbox, metrics);

    try {
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

            metrics.incrementProcessed();
          }
        }
        catch (ParseException e) {
          if (tuningConfig.isReportParseExceptions()) {
            throw e;
          } else {
            log.debug(e, "Discarded row due to exception, considering unparseable.");
            metrics.incrementUnparseable();
          }
        }
      }

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
      throw e;
    }
    finally {
      CloseQuietly.close(firehose);
      CloseQuietly.close(appenderator);
      CloseQuietly.close(driver);

      toolbox.getMonitorScheduler().removeMonitor(metricsMonitor);

      toolbox.getDataSegmentServerAnnouncer().unannounce();
      toolbox.getDruidNodeAnnouncer().unannounce(discoveryDruidNode);
    }

    log.info("Job done!");
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

  /**
   * Is a firehose from this factory drainable by closing it? If so, we should drain on stopGracefully rather than
   * abruptly stopping.
   *
   * This is a hack to get around the fact that the Firehose and FirehoseFactory interfaces do not help us do this.
   *
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
      AppenderatorDriver driver,
      TransactionalSegmentPublisher publisher,
      Supplier<Committer> committerSupplier,
      String sequenceName
  )
  {
    ListenableFuture<SegmentsAndMetadata> publishFuture = driver.publish(
        publisher,
        committerSupplier.get(),
        Collections.singletonList(sequenceName)
    );

    ListenableFuture<SegmentsAndMetadata> handoffFuture = Futures.transform(publishFuture, driver::registerHandoff);

    pendingHandoffs.add(handoffFuture);
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

  private void persistAndWait(AppenderatorDriver driver, Committer committer)
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

  private static AppenderatorDriver newDriver(
      final DataSchema dataSchema,
      final Appenderator appenderator,
      final TaskToolbox toolbox,
      final FireDepartmentMetrics metrics
  )
  {
    return new AppenderatorDriver(
        appenderator,
        new ActionBasedSegmentAllocator(toolbox.getTaskActionClient(), dataSchema),
        toolbox.getSegmentHandoffNotifierFactory(),
        new ActionBasedUsedSegmentChecker(toolbox.getTaskActionClient()),
        toolbox.getObjectMapper(),
        metrics
    );
  }

  private static String makeSequenceName(String taskId, int sequenceNumber)
  {
    return taskId + "_" + sequenceNumber;
  }
}

