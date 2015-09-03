/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.metamx.common.guava.CloseQuietly;
import com.metamx.common.parsers.ParseException;
import com.metamx.emitter.EmittingLogger;
import io.druid.data.input.Firehose;
import io.druid.data.input.InputRow;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.LockAcquireAction;
import io.druid.indexing.common.actions.LockReleaseAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.query.FinalizeResultsQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.query.QueryToolChest;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeIOConfig;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.realtime.FireDepartment;
import io.druid.segment.realtime.RealtimeMetricsMonitor;
import io.druid.segment.realtime.SegmentPublisher;
import io.druid.segment.realtime.plumber.Plumber;
import io.druid.segment.realtime.plumber.RealtimePlumberSchool;
import io.druid.segment.realtime.plumber.Sink;
import io.druid.segment.realtime.plumber.VersioningPolicy;
import io.druid.server.coordination.DataSegmentAnnouncer;
import io.druid.timeline.DataSegment;
import java.util.Map;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

import java.io.File;
import java.io.IOException;
import java.util.Random;

public class RealtimeIndexTask extends AbstractTask
{
  private static final EmittingLogger log = new EmittingLogger(RealtimeIndexTask.class);
  private final static Random random = new Random();

  private static String makeTaskId(FireDepartment fireDepartment)
  {
    return makeTaskId(
        fireDepartment.getDataSchema().getDataSource(),
        fireDepartment.getTuningConfig().getShardSpec().getPartitionNum(),
        new DateTime(),
        random.nextInt()
    );
  }

  static String makeTaskId(String dataSource, int partitionNumber, DateTime timestamp, int randomBits)
  {
    final StringBuilder suffix = new StringBuilder(8);
    for(int i = 0; i < Ints.BYTES * 2; ++i) {
      suffix.append((char)('a' + ((randomBits >>> (i * 4)) & 0x0F)));
    }
    return String.format(
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
        String.format("index_realtime_%s", makeDatasource(fireDepartment)),
        taskResource == null ? new TaskResource(makeTaskId(fireDepartment), 1) : taskResource,
        makeDatasource(fireDepartment),
        context
    );
    this.spec = fireDepartment;
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
    if (plumber != null) {
      QueryRunnerFactory<T, Query<T>> factory = queryRunnerFactoryConglomerate.findFactory(query);
      QueryToolChest<T, Query<T>> toolChest = factory.getToolchest();

      return new FinalizeResultsQueryRunner<T>(plumber.getQueryRunner(query), toolChest);
    } else {
      return null;
    }
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
    return true;
  }

  @Override
  public TaskStatus run(final TaskToolbox toolbox) throws Exception
  {
    if (this.plumber != null) {
      throw new IllegalStateException("WTF?!? run with non-null plumber??!");
    }

    // Shed any locks we might have (e.g. if we were uncleanly killed and restarted) since we'll reacquire
    // them if we actually need them
    for (final TaskLock taskLock : getTaskLocks(toolbox)) {
      toolbox.getTaskActionClient().submit(new LockReleaseAction(taskLock.getInterval()));
    }

    boolean normalExit = true;

    // It would be nice to get the PlumberSchool in the constructor.  Although that will need jackson injectables for
    // stuff like the ServerView, which seems kind of odd?  Perhaps revisit this when Guice has been introduced.

    final SegmentPublisher segmentPublisher = new TaskActionSegmentPublisher(this, toolbox);

    // NOTE: We talk to the coordinator in various places in the plumber and we could be more robust to issues
    // with the coordinator.  Right now, we'll block/throw in whatever thread triggered the coordinator behavior,
    // which will typically be either the main data processing loop or the persist thread.

    // Wrap default DataSegmentAnnouncer such that we unlock intervals as we unannounce segments
    final DataSegmentAnnouncer lockingSegmentAnnouncer = new DataSegmentAnnouncer()
    {
      @Override
      public void announceSegment(final DataSegment segment) throws IOException
      {
        // Side effect: Calling announceSegment causes a lock to be acquired
        toolbox.getTaskActionClient().submit(new LockAcquireAction(segment.getInterval()));
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
          toolbox.getTaskActionClient().submit(new LockAcquireAction(segment.getInterval()));
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
          final TaskLock myLock = toolbox.getTaskActionClient()
                                         .submit(new LockAcquireAction(interval));

          return myLock.getVersion();
        }
        catch (IOException e) {
          throw Throwables.propagate(e);
        }
      }
    };

    DataSchema dataSchema = spec.getDataSchema();
    RealtimeIOConfig realtimeIOConfig = spec.getIOConfig();
    RealtimeTuningConfig tuningConfig = spec.getTuningConfig()
                                              .withBasePersistDirectory(new File(toolbox.getTaskWorkDir(), "persist"))
                                              .withVersioningPolicy(versioningPolicy);

    final FireDepartment fireDepartment = new FireDepartment(
        dataSchema,
        realtimeIOConfig,
        tuningConfig
    );
    final RealtimeMetricsMonitor metricsMonitor = new RealtimeMetricsMonitor(ImmutableList.of(fireDepartment));
    this.queryRunnerFactoryConglomerate = toolbox.getQueryRunnerFactoryConglomerate();

    // NOTE: This pusher selects path based purely on global configuration and the DataSegment, which means
    // NOTE: that redundant realtime tasks will upload to the same location. This can cause index.zip and
    // NOTE: descriptor.json to mismatch, or it can cause historical nodes to load different instances of the
    // NOTE: "same" segment.
    final RealtimePlumberSchool plumberSchool = new RealtimePlumberSchool(
        toolbox.getEmitter(),
        toolbox.getQueryRunnerFactoryConglomerate(),
        toolbox.getSegmentPusher(),
        lockingSegmentAnnouncer,
        segmentPublisher,
        toolbox.getNewSegmentServerView(),
        toolbox.getQueryExecutorService()
    );

    this.plumber = plumberSchool.findPlumber(dataSchema, tuningConfig, fireDepartment.getMetrics());

    // Delay firehose connection to avoid claiming input resources while the plumber is starting up.
    Firehose firehose = null;

    try {
      plumber.startJob();

      // Set up metrics emission
      toolbox.getMonitorScheduler().addMonitor(metricsMonitor);

      // Set up firehose
      final Period intermediatePersistPeriod = spec.getTuningConfig().getIntermediatePersistPeriod();
      firehose = spec.getIOConfig().getFirehoseFactory().connect(spec.getDataSchema().getParser());

      // Time to read data!
      long nextFlush = new DateTime().plus(intermediatePersistPeriod).getMillis();
      while (firehose.hasMore()) {
        final InputRow inputRow;

        try {
          inputRow = firehose.nextRow();

          if (inputRow == null) {
            log.debug("thrown away null input row, considering unparseable");
            fireDepartment.getMetrics().incrementUnparseable();
            continue;
          }
        }
        catch (ParseException e) {
          log.debug(e, "thrown away line due to exception, considering unparseable");
          fireDepartment.getMetrics().incrementUnparseable();
          continue;
        }

        int currCount = plumber.add(inputRow);
        if (currCount == -1) {
          fireDepartment.getMetrics().incrementThrownAway();
          log.debug("Throwing away event[%s]", inputRow);

          if (System.currentTimeMillis() > nextFlush) {
            plumber.persist(firehose.commit());
            nextFlush = new DateTime().plus(intermediatePersistPeriod).getMillis();
          }

          continue;
        }

        fireDepartment.getMetrics().incrementProcessed();
        final Sink sink = plumber.getSink(inputRow.getTimestampFromEpoch());
        if ((sink != null && !sink.canAppendRow()) || System.currentTimeMillis() > nextFlush) {
          plumber.persist(firehose.commit());
          nextFlush = new DateTime().plus(intermediatePersistPeriod).getMillis();
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
          plumber.persist(firehose.commit());
          plumber.finishJob();
        }
        catch (Exception e) {
          log.makeAlert(e, "Failed to finish realtime task").emit();
        }
        finally {
          // firehose will be non-null since normalExit is true
          CloseQuietly.close(firehose);
          toolbox.getMonitorScheduler().removeMonitor(metricsMonitor);
        }
      }
    }

    return TaskStatus.success(getId());
  }

  @JsonProperty("spec")
  public FireDepartment getRealtimeIngestionSchema()
  {
    return spec;
  }

  public static class TaskActionSegmentPublisher implements SegmentPublisher
  {
    final Task task;
    final TaskToolbox taskToolbox;

    public TaskActionSegmentPublisher(Task task, TaskToolbox taskToolbox)
    {
      this.task = task;
      this.taskToolbox = taskToolbox;
    }

    @Override
    public void publishSegment(DataSegment segment) throws IOException
    {
      taskToolbox.pushSegments(ImmutableList.of(segment));
    }
  }
}
