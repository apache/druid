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
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.Committer;
import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.FirehoseFactory;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.LookupNodeService;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TaskRealtimeMetricsMonitorBuilder;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.LockReleaseAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TimeChunkLockAcquireAction;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.NoopQueryRunner;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.RealtimeIOConfig;
import org.apache.druid.segment.indexing.RealtimeTuningConfig;
import org.apache.druid.segment.realtime.FireDepartment;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;
import org.apache.druid.segment.realtime.RealtimeMetricsMonitor;
import org.apache.druid.segment.realtime.SegmentPublisher;
import org.apache.druid.segment.realtime.appenderator.SegmentSchemas;
import org.apache.druid.segment.realtime.firehose.ClippedFirehoseFactory;
import org.apache.druid.segment.realtime.firehose.EventReceiverFirehoseFactory;
import org.apache.druid.segment.realtime.firehose.TimedShutoffFirehoseFactory;
import org.apache.druid.segment.realtime.plumber.Committers;
import org.apache.druid.segment.realtime.plumber.Plumber;
import org.apache.druid.segment.realtime.plumber.PlumberSchool;
import org.apache.druid.segment.realtime.plumber.Plumbers;
import org.apache.druid.segment.realtime.plumber.RealtimePlumberSchool;
import org.apache.druid.segment.realtime.plumber.VersioningPolicy;
import org.apache.druid.server.coordination.DataSegmentAnnouncer;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.utils.CloseableUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

@Deprecated
public class RealtimeIndexTask extends AbstractTask
{
  public static final String CTX_KEY_LOOKUP_TIER = "lookupTier";

  private static final EmittingLogger log = new EmittingLogger(RealtimeIndexTask.class);

  private static final int TASK_ID_BITS_PER_SYMBOL = 4;
  private static final int TASK_ID_SYMBOL_MASK = (1 << TASK_ID_BITS_PER_SYMBOL) - 1;
  private static final int TASK_ID_LENGTH = Integer.SIZE / TASK_ID_BITS_PER_SYMBOL;

  public static String makeRandomId()
  {
    final StringBuilder suffix = new StringBuilder(TASK_ID_LENGTH);
    int randomBits = ThreadLocalRandom.current().nextInt();
    for (int i = 0; i < TASK_ID_LENGTH; i++) {
      suffix.append((char) ('a' + ((randomBits >>> (i * TASK_ID_BITS_PER_SYMBOL)) & TASK_ID_SYMBOL_MASK)));
    }
    return suffix.toString();
  }

  private static String makeTaskId(FireDepartment fireDepartment)
  {
    return makeTaskId(
        fireDepartment.getDataSchema().getDataSource(),
        fireDepartment.getTuningConfig().getShardSpec().getPartitionNum(),
        DateTimes.nowUtc(),
        makeRandomId()
    );
  }

  static String makeTaskId(String dataSource, int partitionNumber, DateTime timestamp, String suffix)
  {
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
  private volatile Plumber plumber = null;

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
  private volatile QueryRunnerFactoryConglomerate queryRunnerFactoryConglomerate = null;

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
    if (plumber == null) {
      // Not yet initialized, no data yet, just return a noop runner.
      return new NoopQueryRunner<>();
    }

    return plumber.getQueryRunner(query);
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
  public TaskStatus runTask(final TaskToolbox toolbox) throws Exception
  {
    runThread = Thread.currentThread();

    if (this.plumber != null) {
      throw new IllegalStateException("Plumber must be null");
    }

    setupTimeoutAlert();

    boolean normalExit = true;

    // It would be nice to get the PlumberSchool in the constructor.  Although that will need jackson injectables for
    // stuff like the ServerView, which seems kind of odd?  Perhaps revisit this when Guice has been introduced.

    final SegmentPublisher segmentPublisher = new TaskActionSegmentPublisher(toolbox);

    // NOTE: We talk to the coordinator in various places in the plumber and we could be more robust to issues
    // with the coordinator.  Right now, we'll block/throw in whatever thread triggered the coordinator behavior,
    // which will typically be either the main data processing loop or the persist thread.

    // Wrap default DataSegmentAnnouncer such that we unlock intervals as we unannounce segments
    final long lockTimeoutMs = getContextValue(Tasks.LOCK_TIMEOUT_KEY, Tasks.DEFAULT_LOCK_TIMEOUT_MILLIS);
    // Note: if lockTimeoutMs is larger than ServerConfig.maxIdleTime, http timeout error can occur while waiting for a
    // lock to be acquired.
    final DataSegmentAnnouncer lockingSegmentAnnouncer = new DataSegmentAnnouncer()
    {
      @Override
      public void announceSegment(final DataSegment segment) throws IOException
      {
        // Side effect: Calling announceSegment causes a lock to be acquired
        final TaskLock lock = Preconditions.checkNotNull(
            toolbox.getTaskActionClient().submit(
                new TimeChunkLockAcquireAction(TaskLockType.EXCLUSIVE, segment.getInterval(), lockTimeoutMs)
            ),
            "Cannot acquire a lock for interval[%s]",
            segment.getInterval()
        );
        if (lock.isRevoked()) {
          throw new ISE(StringUtils.format("Lock for interval [%s] was revoked.", segment.getInterval()));
        }
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
          final TaskLock lock = Preconditions.checkNotNull(
              toolbox.getTaskActionClient().submit(
                  new TimeChunkLockAcquireAction(TaskLockType.EXCLUSIVE, segment.getInterval(), lockTimeoutMs)
              ),
              "Cannot acquire a lock for interval[%s]",
              segment.getInterval()
          );
          if (lock.isRevoked()) {
            throw new ISE(StringUtils.format("Lock for interval [%s] was revoked.", segment.getInterval()));
          }
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

      @Override
      public void announceSegmentSchemas(String taskId, SegmentSchemas sinksSchema, SegmentSchemas sinksSchemaChange)
      {
      }

      @Override
      public void removeSegmentSchemasForTask(String taskId)
      {
      }
    };

    // NOTE: getVersion will block if there is lock contention, which will block plumber.getSink
    // NOTE: (and thus the firehose)

    // Shouldn't usually happen, since we don't expect people to submit tasks that intersect with the
    // realtime window, but if they do it can be problematic. If we decide to care, we can use more threads in
    // the plumber such that waiting for the coordinator doesn't block data processing.
    final VersioningPolicy versioningPolicy = new VersioningPolicy()
    {
      @Override
      public String getVersion(final Interval interval)
      {
        try {
          // Side effect: Calling getVersion causes a lock to be acquired
          final TimeChunkLockAcquireAction action = new TimeChunkLockAcquireAction(
              TaskLockType.EXCLUSIVE,
              interval,
              lockTimeoutMs
          );
          final TaskLock lock = Preconditions.checkNotNull(
              toolbox.getTaskActionClient().submit(action),
              "Cannot acquire a lock for interval[%s]",
              interval
          );
          if (lock.isRevoked()) {
            throw new ISE(StringUtils.format("Lock for interval [%s] was revoked.", interval));
          }
          return lock.getVersion();
        }
        catch (IOException e) {
          throw new RuntimeException(e);
        }
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
    final RealtimeMetricsMonitor metricsMonitor = TaskRealtimeMetricsMonitorBuilder.build(this, fireDepartment);

    this.queryRunnerFactoryConglomerate = toolbox.getQueryRunnerFactoryConglomerate();

    // NOTE: This pusher selects path based purely on global configuration and the DataSegment, which means
    // NOTE: that redundant realtime tasks will upload to the same location. This can cause index.zip
    // NOTE: (partitionNum_index.zip for HDFS data storage) to mismatch, or it can cause historical nodes to load
    // NOTE: different instances of the "same" segment.
    final PlumberSchool plumberSchool = new RealtimePlumberSchool(
        toolbox.getEmitter(),
        toolbox.getQueryRunnerFactoryConglomerate(),
        toolbox.getSegmentPusher(),
        lockingSegmentAnnouncer,
        segmentPublisher,
        toolbox.getSegmentHandoffNotifierFactory(),
        toolbox.getQueryProcessingPool(),
        toolbox.getJoinableFactory(),
        toolbox.getIndexMergerV9(),
        toolbox.getIndexIO(),
        toolbox.getCache(),
        toolbox.getCacheConfig(),
        toolbox.getCachePopulatorStats(),
        toolbox.getJsonMapper()
    );

    this.plumber = plumberSchool.findPlumber(dataSchema, tuningConfig, metrics);

    final Supplier<Committer> committerSupplier = Committers.nilSupplier();

    LookupNodeService lookupNodeService = getContextValue(CTX_KEY_LOOKUP_TIER) == null ?
                                          toolbox.getLookupNodeService() :
                                          new LookupNodeService((String) getContextValue(CTX_KEY_LOOKUP_TIER));
    DiscoveryDruidNode discoveryDruidNode = new DiscoveryDruidNode(
        toolbox.getDruidNode(),
        NodeRole.PEON,
        ImmutableMap.of(
            toolbox.getDataNodeService().getName(), toolbox.getDataNodeService(),
            lookupNodeService.getName(), lookupNodeService
        )
    );

    try {
      toolbox.getDataSegmentServerAnnouncer().announce();
      toolbox.getDruidNodeAnnouncer().announce(discoveryDruidNode);


      plumber.startJob();

      // Set up metrics emission
      toolbox.addMonitor(metricsMonitor);

      // Delay firehose connection to avoid claiming input resources while the plumber is starting up.
      final FirehoseFactory firehoseFactory = spec.getIOConfig().getFirehoseFactory();
      final boolean firehoseDrainableByClosing = isFirehoseDrainableByClosing(firehoseFactory);

      // Skip connecting firehose if we've been stopped before we got started.
      synchronized (this) {
        if (!gracefullyStopped) {
          firehose = firehoseFactory.connect(
              Preconditions.checkNotNull(spec.getDataSchema().getParser(), "inputRowParser"),
              toolbox.getIndexingTmpDir()
          );
        }
      }

      // Time to read data!
      while (firehose != null && (!gracefullyStopped || firehoseDrainableByClosing) && firehose.hasMore()) {
        Plumbers.addNextRow(
            committerSupplier,
            firehose,
            plumber,
            tuningConfig.isReportParseExceptions(),
            metrics
        );
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
            plumber.persist(
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

          if (gracefullyStopped) {
            log.info("Gracefully stopping.");
          } else {
            log.info("Finishing the job.");
            synchronized (this) {
              if (gracefullyStopped) {
                // Someone called stopGracefully after we checked the flag. That's okay, just stop now.
                log.info("Gracefully stopping.");
              } else {
                finishingJob = true;
              }
            }

            if (finishingJob) {
              plumber.finishJob();
            }
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
            CloseableUtils.closeAndSuppressExceptions(firehose, e -> log.warn("Failed to close Firehose"));
          }
          toolbox.removeMonitor(metricsMonitor);
        }
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

  public static class TaskActionSegmentPublisher implements SegmentPublisher
  {
    final TaskToolbox taskToolbox;

    public TaskActionSegmentPublisher(TaskToolbox taskToolbox)
    {
      this.taskToolbox = taskToolbox;
    }

    @Override
    public void publishSegment(DataSegment segment) throws IOException
    {
      taskToolbox.publishSegments(ImmutableList.of(segment));
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
}
