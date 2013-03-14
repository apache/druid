package com.metamx.druid.merger.common.task;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closeables;
import com.metamx.common.exception.FormattedException;
import com.metamx.druid.Query;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.index.v1.IndexGranularity;
import com.metamx.druid.input.InputRow;
import com.metamx.druid.merger.common.TaskLock;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.TaskToolbox;
import com.metamx.druid.merger.common.actions.LockAcquireAction;
import com.metamx.druid.merger.common.actions.LockListAction;
import com.metamx.druid.merger.common.actions.LockReleaseAction;
import com.metamx.druid.merger.common.actions.SegmentInsertAction;
import com.metamx.druid.query.QueryRunner;
import com.metamx.druid.realtime.FireDepartmentConfig;
import com.metamx.druid.realtime.FireDepartmentMetrics;
import com.metamx.druid.realtime.Firehose;
import com.metamx.druid.realtime.FirehoseFactory;
import com.metamx.druid.realtime.plumber.Plumber;
import com.metamx.druid.realtime.plumber.RealtimePlumberSchool;
import com.metamx.druid.realtime.Schema;
import com.metamx.druid.realtime.SegmentAnnouncer;
import com.metamx.druid.realtime.SegmentPublisher;
import com.metamx.druid.realtime.plumber.Sink;
import com.metamx.druid.realtime.plumber.VersioningPolicy;
import com.metamx.emitter.EmittingLogger;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

import java.io.File;
import java.io.IOException;

public class RealtimeIndexTask extends AbstractTask
{
  @JsonIgnore
  final Schema schema;

  @JsonIgnore
  final FirehoseFactory firehoseFactory;

  @JsonIgnore
  final FireDepartmentConfig fireDepartmentConfig;

  @JsonIgnore
  final Period windowPeriod;

  @JsonIgnore
  final IndexGranularity segmentGranularity;

  @JsonIgnore
  private volatile Plumber plumber = null;

  private static final EmittingLogger log = new EmittingLogger(RealtimeIndexTask.class);

  @JsonCreator
  public RealtimeIndexTask(
      @JsonProperty("id") String id,
      @JsonProperty("schema") Schema schema,
      @JsonProperty("firehose") FirehoseFactory firehoseFactory,
      @JsonProperty("fireDepartmentConfig") FireDepartmentConfig fireDepartmentConfig, // TODO rename?
      @JsonProperty("windowPeriod") Period windowPeriod,
      @JsonProperty("segmentGranularity") IndexGranularity segmentGranularity
  )
  {
    super(
        id != null ? id : String.format(
            "index_realtime_%s_%d_%s",
            schema.getDataSource(), schema.getShardSpec().getPartitionNum(), new DateTime()
        ),
        String.format(
            "index_realtime_%s",
            schema.getDataSource()
        ),
        schema.getDataSource(),
        null
    );

    this.schema = schema;
    this.firehoseFactory = firehoseFactory;
    this.fireDepartmentConfig = fireDepartmentConfig;
    this.windowPeriod = windowPeriod;
    this.segmentGranularity = segmentGranularity;
  }

  @Override
  public String getType()
  {
    return "index_realtime";
  }

  @Override
  public <T> QueryRunner<T> getQueryRunner(Query<T> query)
  {
    if (plumber != null) {
      return plumber.getQueryRunner(query);
    } else {
      return null;
    }
  }

  @Override
  public TaskStatus run(final TaskToolbox toolbox) throws Exception
  {
    if (this.plumber != null) {
      throw new IllegalStateException("WTF?!? run with non-null plumber??!");
    }

    // Shed any locks we might have (e.g. if we were uncleanly killed and restarted) since we'll reacquire
    // them if we actually need them
    for (final TaskLock taskLock : toolbox.getTaskActionClient().submit(new LockListAction())) {
      toolbox.getTaskActionClient().submit(new LockReleaseAction(taskLock.getInterval()));
    }

    boolean normalExit = true;

    final FireDepartmentMetrics metrics = new FireDepartmentMetrics();
    final Period intermediatePersistPeriod = fireDepartmentConfig.getIntermediatePersistPeriod();
    final Firehose firehose = firehoseFactory.connect();

    // TODO -- Take PlumberSchool in constructor (although that will need jackson injectables for stuff like
    // TODO -- the ServerView, which seems kind of odd?)
    final RealtimePlumberSchool realtimePlumberSchool = new RealtimePlumberSchool(
        windowPeriod,
        new File(toolbox.getTaskDir(), "persist"),
        segmentGranularity
    );

    final SegmentPublisher segmentPublisher = new TaskActionSegmentPublisher(this, toolbox);

    // TODO -- We're adding stuff to talk to the coordinator in various places in the plumber, and may
    // TODO -- want to be more robust to coordinator downtime (currently we'll block/throw in whatever
    // TODO -- thread triggered the coordinator behavior, which will typically be either the main
    // TODO -- data processing loop or the persist thread)

    // Wrap default SegmentAnnouncer such that we unlock intervals as we unannounce segments
    final SegmentAnnouncer lockingSegmentAnnouncer = new SegmentAnnouncer()
    {
      @Override
      public void announceSegment(final DataSegment segment) throws IOException
      {
        // NOTE: Side effect: Calling announceSegment causes a lock to be acquired
        toolbox.getTaskActionClient().submit(new LockAcquireAction(segment.getInterval()));
        toolbox.getSegmentAnnouncer().announceSegment(segment);
      }

      @Override
      public void unannounceSegment(final DataSegment segment) throws IOException
      {
        try {
          toolbox.getSegmentAnnouncer().unannounceSegment(segment);
        } finally {
          toolbox.getTaskActionClient().submit(new LockReleaseAction(segment.getInterval()));
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
          // NOTE: Side effect: Calling getVersion causes a lock to be acquired
          final TaskLock myLock = toolbox.getTaskActionClient()
                                         .submit(new LockAcquireAction(interval));

          return myLock.getVersion();
        } catch (IOException e) {
          throw Throwables.propagate(e);
        }
      }
    };

    // NOTE: This pusher selects path based purely on global configuration and the DataSegment, which means
    // NOTE: that redundant realtime tasks will upload to the same location. This can cause index.zip and
    // NOTE: descriptor.json to mismatch, or it can cause compute nodes to load different instances of the
    // NOTE: "same" segment.
    realtimePlumberSchool.setDataSegmentPusher(toolbox.getSegmentPusher());
    realtimePlumberSchool.setConglomerate(toolbox.getQueryRunnerFactoryConglomerate());
    realtimePlumberSchool.setVersioningPolicy(versioningPolicy);
    realtimePlumberSchool.setSegmentAnnouncer(lockingSegmentAnnouncer);
    realtimePlumberSchool.setSegmentPublisher(segmentPublisher);
    realtimePlumberSchool.setServerView(toolbox.getNewSegmentServerView());
    realtimePlumberSchool.setServiceEmitter(toolbox.getEmitter());

    this.plumber = realtimePlumberSchool.findPlumber(schema, metrics);

    try {
      plumber.startJob();

      long nextFlush = new DateTime().plus(intermediatePersistPeriod).getMillis();
      while (firehose.hasMore()) {
        final InputRow inputRow;
        try {
          inputRow = firehose.nextRow();

          final Sink sink = plumber.getSink(inputRow.getTimestampFromEpoch());
          if (sink == null) {
            metrics.incrementThrownAway();
            log.debug("Throwing away event[%s]", inputRow);

            if (System.currentTimeMillis() > nextFlush) {
              plumber.persist(firehose.commit());
              nextFlush = new DateTime().plus(intermediatePersistPeriod).getMillis();
            }

            continue;
          }

          if (sink.isEmpty()) {
            log.info("Task %s: New sink: %s", getId(), sink);
          }

          int currCount = sink.add(inputRow);
          metrics.incrementProcessed();
          if (currCount >= fireDepartmentConfig.getMaxRowsInMemory() || System.currentTimeMillis() > nextFlush) {
            plumber.persist(firehose.commit());
            nextFlush = new DateTime().plus(intermediatePersistPeriod).getMillis();
          }
        }
        catch (FormattedException e) {
          log.warn(e, "unparseable line");
          metrics.incrementUnparseable();
        }
      }
    }
    catch (Exception e) {
      log.makeAlert(e, "Exception aborted realtime processing[%s]", schema.getDataSource())
         .emit();
      normalExit = false;
      throw Throwables.propagate(e);
    }
    finally {
      Closeables.closeQuietly(firehose);

      if (normalExit) {
        try {
          plumber.persist(firehose.commit());
          plumber.finishJob();
        } catch(Exception e) {
          log.makeAlert(e, "Failed to finish realtime task").emit();
        }
      }
    }

    return TaskStatus.success(getId());
  }

  @JsonProperty
  public Schema getSchema()
  {
    return schema;
  }

  @JsonProperty("firehose")
  public FirehoseFactory getFirehoseFactory()
  {
    return firehoseFactory;
  }

  @JsonProperty
  public FireDepartmentConfig getFireDepartmentConfig()
  {
    return fireDepartmentConfig;
  }

  @JsonProperty
  public Period getWindowPeriod()
  {
    return windowPeriod;
  }

  @JsonProperty
  public IndexGranularity getSegmentGranularity()
  {
    return segmentGranularity;
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
      taskToolbox.getTaskActionClient().submit(new SegmentInsertAction(ImmutableSet.of(segment)));
    }
  }
}
