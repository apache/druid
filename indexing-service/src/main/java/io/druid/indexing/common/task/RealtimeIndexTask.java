/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.metamx.common.Granularity;
import com.metamx.common.guava.CloseQuietly;
import com.metamx.common.parsers.ParseException;
import com.metamx.emitter.EmittingLogger;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
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
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.segment.realtime.FireDepartment;
import io.druid.segment.realtime.FireDepartmentConfig;
import io.druid.segment.realtime.RealtimeMetricsMonitor;
import io.druid.segment.realtime.Schema;
import io.druid.segment.realtime.SegmentPublisher;
import io.druid.segment.realtime.plumber.Plumber;
import io.druid.segment.realtime.plumber.RealtimePlumberSchool;
import io.druid.segment.realtime.plumber.RejectionPolicyFactory;
import io.druid.segment.realtime.plumber.VersioningPolicy;
import io.druid.server.coordination.DataSegmentAnnouncer;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

import java.io.File;
import java.io.IOException;

public class RealtimeIndexTask extends AbstractTask
{
  private static final EmittingLogger log = new EmittingLogger(RealtimeIndexTask.class);

  private static String makeTaskId(FireDepartment fireDepartment, Schema schema)
  {
    // Backwards compatible
    if (fireDepartment == null) {
      return String.format(
          "index_realtime_%s_%d_%s",
          schema.getDataSource(), schema.getShardSpec().getPartitionNum(), new DateTime().toString()
      );
    } else {
      return String.format(
          "index_realtime_%s_%d_%s",
          fireDepartment.getDataSchema().getDataSource(),
          fireDepartment.getTuningConfig().getShardSpec().getPartitionNum(),
          new DateTime().toString()
      );
    }
  }

  private static String makeDatasource(FireDepartment fireDepartment, Schema schema)
  {
    return (fireDepartment != null) ? fireDepartment.getDataSchema().getDataSource() : schema.getDataSource();
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
      // Backwards compatible, to be deprecated
      @JsonProperty("schema") Schema spec,
      @JsonProperty("firehose") FirehoseFactory firehoseFactory,
      @JsonProperty("fireDepartmentConfig") FireDepartmentConfig fireDepartmentConfig,
      @JsonProperty("windowPeriod") Period windowPeriod,
      @JsonProperty("maxPendingPersists") int maxPendingPersists,
      @JsonProperty("segmentGranularity") Granularity segmentGranularity,
      @JsonProperty("rejectionPolicy") RejectionPolicyFactory rejectionPolicy,
      @JsonProperty("rejectionPolicyFactory") RejectionPolicyFactory rejectionPolicyFactory
  )
  {
    super(
        id == null ? makeTaskId(fireDepartment, spec) : id,
        String.format("index_realtime_%s", makeDatasource(fireDepartment, spec)),
        taskResource == null ? new TaskResource(makeTaskId(fireDepartment, spec), 1) : taskResource,
        makeDatasource(fireDepartment, spec)
    );

    if (fireDepartment != null) {
      this.spec = fireDepartment;
    } else {
      this.spec = new FireDepartment(
          new DataSchema(
              spec.getDataSource(),
              firehoseFactory == null ? null : firehoseFactory.getParser(),
              spec.getAggregators(),
              new UniformGranularitySpec(segmentGranularity, spec.getIndexGranularity(), null, segmentGranularity)
          ),
          new RealtimeIOConfig(firehoseFactory, null),
          new RealtimeTuningConfig(
              fireDepartmentConfig == null ? null : fireDepartmentConfig.getMaxRowsInMemory(),
              fireDepartmentConfig == null ? null : fireDepartmentConfig.getIntermediatePersistPeriod(),
              windowPeriod,
              null,
              null,
              rejectionPolicy == null ? rejectionPolicyFactory : rejectionPolicy,
              maxPendingPersists,
              spec.getShardSpec()
          ),
          null, null, null, null
      );
    }
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

    // Set up firehose
    final Period intermediatePersistPeriod = spec.getTuningConfig().getIntermediatePersistPeriod();
    final Firehose firehose = spec.getIOConfig().getFirehoseFactory().connect(spec.getDataSchema().getParser());

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
        tuningConfig,
        null,
        null,
        null,
        null
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
        toolbox.getQueryExecutorService(),
        null,
        null,
        null,
        null,
        null,
        null,
        0
    );

    this.plumber = plumberSchool.findPlumber(dataSchema, tuningConfig, fireDepartment.getMetrics());

    try {
      plumber.startJob();

      // Set up metrics emission
      toolbox.getMonitorScheduler().addMonitor(metricsMonitor);

      // Time to read data!
      long nextFlush = new DateTime().plus(intermediatePersistPeriod).getMillis();
      while (firehose.hasMore()) {
        final InputRow inputRow;
        try {
          inputRow = firehose.nextRow();
          if (inputRow == null) {
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
          if (currCount >= tuningConfig.getMaxRowsInMemory() || System.currentTimeMillis() > nextFlush) {
            plumber.persist(firehose.commit());
            nextFlush = new DateTime().plus(intermediatePersistPeriod).getMillis();
          }
        }
        catch (ParseException e) {
          log.warn(e, "unparseable line");
          fireDepartment.getMetrics().incrementUnparseable();
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
