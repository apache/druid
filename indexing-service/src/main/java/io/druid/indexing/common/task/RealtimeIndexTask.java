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
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metamx.emitter.EmittingLogger;
import io.druid.data.input.Committer;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.discovery.DiscoveryDruidNode;
import io.druid.discovery.DruidNodeDiscoveryProvider;
import io.druid.discovery.LookupNodeService;
import io.druid.indexing.appenderator.ActionBasedSegmentAllocator;
import io.druid.indexing.appenderator.ActionBasedUsedSegmentChecker;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskLockType;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.LockAcquireAction;
import io.druid.indexing.common.actions.LockReleaseAction;
import io.druid.indexing.common.actions.SegmentTransactionalInsertAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.java.util.common.parsers.ParseException;
import io.druid.query.DruidMetrics;
import io.druid.query.NoopQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeIOConfig;
import io.druid.segment.indexing.RealtimeTuningConfig;
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
import io.druid.segment.realtime.plumber.VersioningPolicy;
import io.druid.server.coordination.DataSegmentAnnouncer;
import io.druid.timeline.DataSegment;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class RealtimeIndexTask extends AbstractTask
{
  public static final String CTX_KEY_LOOKUP_TIER = "lookupTier";

  private static final EmittingLogger log = new EmittingLogger(RealtimeIndexTask.class);
  private static final Random random = new Random();

  private static String makeTaskId(FireDepartment fireDepartment)
  {
    return makeTaskId(
        fireDepartment.getDataSchema().getDataSource(),
        fireDepartment.getTuningConfig().getShardSpec().getPartitionNum(),
        DateTimes.nowUtc(),
        random.nextInt()
    );
  }

  static String makeTaskId(String dataSource, int partitionNumber, DateTime timestamp, int randomBits)
  {
    final StringBuilder suffix = new StringBuilder(8);
    for (int i = 0; i < Ints.BYTES * 2; ++i) {
      suffix.append((char) ('a' + ((randomBits >>> (i * 4)) & 0x0F)));
    }
    return StringUtils.format(
        "index_realtime_%s_%d_%s_%s",
        dataSource,
        partitionNumber,
        timestamp,
        suffix
    );
  }

  private static String makeDatasource(FireDepartment fireDepartment)
  {
    return fireDepartment.getDataSchema().getDataSource();
  }

  @JsonIgnore
  private final FireDepartment spec;

  @JsonIgnore
  private final Queue<ListenableFuture<SegmentsAndMetadata>> pendingHandoffs;

  @JsonIgnore
  private volatile AppenderatorDriver driver = null;

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
  public RealtimeIndexTask(
      @JsonProperty("id") String id,
      @JsonProperty("resource") TaskResource taskResource,
      @JsonProperty("spec") FireDepartment fireDepartment,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(
        id == null ? makeTaskId(fireDepartment) : id,
        StringUtils.format("index_realtime_%s", makeDatasource(fireDepartment)),
        taskResource,
        makeDatasource(fireDepartment),
        context
    );
    this.spec = fireDepartment;
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
    return "index_realtime";
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
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
    return true;
  }

  @Override
  public TaskStatus run(final TaskToolbox toolbox) throws Exception
  {
    runThread = Thread.currentThread();

    setupTimeoutAlert();

    boolean normalExit = true;

    // Wrap default DataSegmentAnnouncer such that we unlock intervals as we unannounce segments
    final long lockTimeoutMs = getContextValue(Tasks.LOCK_TIMEOUT_KEY, Tasks.DEFAULT_LOCK_TIMEOUT);
    // Note: if lockTimeoutMs is larger than ServerConfig.maxIdleTime, http timeout error can occur while waiting for a
    // lock to be acquired.
    final DataSegmentAnnouncer lockingSegmentAnnouncer = new DataSegmentAnnouncer()
    {
      @Override
      public void announceSegment(final DataSegment segment) throws IOException
      {
        // Side effect: Calling announceSegment causes a lock to be acquired
        Preconditions.checkNotNull(
            toolbox.getTaskActionClient().submit(
                new LockAcquireAction(TaskLockType.EXCLUSIVE, segment.getInterval(), lockTimeoutMs)
            ),
            "Cannot acquire a lock for interval[%s]",
            segment.getInterval()
        );
        toolbox.getSegmentAnnouncer().announceSegment(segment);
      }

      @Override
      public void unannounceSegment(final DataSegment segment) throws IOException
      {
        try {
          toolbox.getSegmentAnnouncer().unannounceSegment(segment);
        }
        finally {
          toolbox.getTaskActionClient().submit(new LockReleaseAction(segment.getInterval()));
        }
      }

      @Override
      public void announceSegments(Iterable<DataSegment> segments) throws IOException
      {
        // Side effect: Calling announceSegments causes locks to be acquired
        for (DataSegment segment : segments) {
          Preconditions.checkNotNull(
              toolbox.getTaskActionClient().submit(
                  new LockAcquireAction(TaskLockType.EXCLUSIVE, segment.getInterval(), lockTimeoutMs)
              ),
              "Cannot acquire a lock for interval[%s]",
              segment.getInterval()
          );
        }
        toolbox.getSegmentAnnouncer().announceSegments(segments);
      }

      @Override
      public void unannounceSegments(Iterable<DataSegment> segments) throws IOException
      {
        try {
          toolbox.getSegmentAnnouncer().unannounceSegments(segments);
        }
        finally {
          for (DataSegment segment : segments) {
            toolbox.getTaskActionClient().submit(new LockReleaseAction(segment.getInterval()));
          }
        }
      }
    };

    // NOTE: getVersion will block (and thus block the firehose) if there is lock contention

    // Shouldn't usually happen, since we don't expect people to submit tasks that intersect with the
    // realtime window, but if they do it can be problematic. If we decide to care, we can use more threads in
    // the plumber such that waiting for the coordinator doesn't block data processing.
    final VersioningPolicy versioningPolicy = interval -> {
      try {
        // Side effect: Calling getVersion causes a lock to be acquired
        final LockAcquireAction action = new LockAcquireAction(TaskLockType.EXCLUSIVE, interval, lockTimeoutMs);
        final TaskLock lock = Preconditions.checkNotNull(
            toolbox.getTaskActionClient().submit(action),
            "Cannot acquire a lock for interval[%s]",
            interval
        );

        return lock.getVersion();
      }
      catch (IOException e) {
        throw Throwables.propagate(e);
      }
    };

    DataSchema dataSchema = spec.getDataSchema();
    RealtimeIOConfig realtimeIOConfig = spec.getIOConfig();
    RealtimeTuningConfig tuningConfig = spec.getTuningConfig()
                                            .withBasePersistDirectory(toolbox.getPersistDir())
                                            .withVersioningPolicy(versioningPolicy);

    final FireDepartment fireDepartment = new FireDepartment(
        dataSchema,
        realtimeIOConfig,
        tuningConfig
    );
    this.metrics = fireDepartment.getMetrics();
    final RealtimeMetricsMonitor metricsMonitor = new RealtimeMetricsMonitor(
        ImmutableList.of(fireDepartment),
        ImmutableMap.of(
            DruidMetrics.TASK_ID, new String[]{getId()}
        )
    );

    Supplier<Committer> committerSupplier = null;
    final File firehoseTempDir = toolbox.getFirehoseTemporaryDir();

    LookupNodeService lookupNodeService = getContextValue(CTX_KEY_LOOKUP_TIER) == null ?
                                          toolbox.getLookupNodeService() :
                                          new LookupNodeService((String) getContextValue(CTX_KEY_LOOKUP_TIER));
    DiscoveryDruidNode discoveryDruidNode = new DiscoveryDruidNode(
        toolbox.getDruidNode(),
        DruidNodeDiscoveryProvider.NODE_TYPE_PEON,
        ImmutableMap.of(
            toolbox.getDataNodeService().getName(), toolbox.getDataNodeService(),
            lookupNodeService.getName(), lookupNodeService
        )
    );

    appenderator = newAppenderator(dataSchema, tuningConfig, fireDepartment.getMetrics(), toolbox, lockingSegmentAnnouncer);
    driver = newDriver(dataSchema, appenderator, toolbox, fireDepartment.getMetrics());

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

      // Skip connecting firehose if we've been stopped before we got started.
      synchronized (this) {
        if (!gracefullyStopped) {
          firehose = firehoseFactory.connect(spec.getDataSchema().getParser(), firehoseTempDir);
          committerSupplier = Committers.supplierFromFirehose(firehose);
        }
      }

      int sequenceNumber = 0;
      String sequenceName = makeSequenceName(sequenceNumber);

      final TransactionalSegmentPublisher publisher = (segments, commitMetadata) -> {
        final SegmentTransactionalInsertAction action = new SegmentTransactionalInsertAction(segments);
        return toolbox.getTaskActionClient().submit(action).isSuccess();
      };

      // Time to read data!
      while (firehose != null && (!gracefullyStopped || firehoseDrainableByClosing) && firehose.hasMore()) {
        try {
          InputRow inputRow = firehose.nextRow();

          if (inputRow == null) {
            log.debug("Discarded null row, considering thrownAway.");
            metrics.incrementThrownAway();
          } else {
            AppenderatorDriverAddResult addResult = driver.add(inputRow, sequenceName, committerSupplier);

            if (addResult.isOk()) {
              if (addResult.isPersistRequired()) {
                driver.persist(committerSupplier.get());
              }

              if (addResult.getNumRowsInSegment() > tuningConfig.getMaxRowsPerSegment()) {
                publishSegments(publisher, committerSupplier, sequenceName);

                sequenceNumber++;
                sequenceName = makeSequenceName(sequenceNumber);
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
          publishSegments(publisher, committerSupplier, sequenceName);

          if (!pendingHandoffs.isEmpty()) {
            ListenableFuture<?> allHandoffs = Futures.allAsList(pendingHandoffs);
            log.info("Waiting for handoffs");

            long handoffTimeout = tuningConfig.getHandoffConditionTimeout();

            if (handoffTimeout > 0) {
              allHandoffs.get(handoffTimeout, TimeUnit.MILLISECONDS);
            } else {
              allHandoffs.get();
            }
          }
        }
      }

    }
    catch (Throwable e) {
      normalExit = false;
      log.makeAlert(e, "Exception aborted realtime processing[%s]", dataSchema.getDataSource())
         .emit();
      throw e;
    }
    finally {
      if (normalExit) {
        try {
          // Persist if we had actually started.
          if (firehose != null) {
            log.info("Persisting remaining data.");

            final Committer committer = committerSupplier.get();
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
        }
        catch (InterruptedException e) {
          log.debug(e, "Interrupted while finishing the job");
        }
        catch (Exception e) {
          log.makeAlert(e, "Failed to finish realtime task").emit();
          throw e;
        }
        finally {
          if (firehose != null) {
            CloseQuietly.close(firehose);
          }
          toolbox.getMonitorScheduler().removeMonitor(metricsMonitor);
        }
      }

      if (appenderator != null) {
        appenderator.close();
      }

      if (driver != null) {
        driver.close();
      }

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
  public FireDepartment getRealtimeIngestionSchema()
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

  private Appenderator newAppenderator(
      final DataSchema dataSchema,
      final RealtimeTuningConfig tuningConfig,
      final FireDepartmentMetrics metrics,
      final TaskToolbox toolbox,
      final DataSegmentAnnouncer segmentAnnouncer
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
        segmentAnnouncer,
        toolbox.getEmitter(),
        toolbox.getQueryExecutorService(),
        toolbox.getCache(),
        toolbox.getCacheConfig()
    );
  }

  private AppenderatorDriver newDriver(
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

  private void publishSegments(
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

  private String makeSequenceName(int sequenceNumber)
  {
    return getId() + "_" + sequenceNumber;
  }
}
