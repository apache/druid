package io.druid.segment.realtime.plumber;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.metamx.common.Pair;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.client.DruidServer;
import io.druid.client.ServerView;
import io.druid.common.guava.ThreadRenamingCallable;
import io.druid.common.guava.ThreadRenamingRunnable;
import io.druid.concurrent.Execs;
import io.druid.query.MetricsEmittingQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.query.QueryToolChest;
import io.druid.query.SegmentDescriptor;
import io.druid.query.spec.SpecificSegmentQueryRunner;
import io.druid.query.spec.SpecificSegmentSpec;
import io.druid.segment.IndexGranularity;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMerger;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.Segment;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.FireHydrant;
import io.druid.segment.realtime.Schema;
import io.druid.segment.realtime.SegmentPublisher;
import io.druid.server.coordination.DataSegmentAnnouncer;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.SingleElementPartitionChunk;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 */
public class RealtimePlumber implements Plumber
{
  private static final EmittingLogger log = new EmittingLogger(RealtimePlumber.class);

  private final Period windowPeriod;
  private final File basePersistDirectory;
  private final IndexGranularity segmentGranularity;
  private final Schema schema;
  private final FireDepartmentMetrics metrics;
  private final RejectionPolicy rejectionPolicy;
  private final ServiceEmitter emitter;
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final DataSegmentAnnouncer segmentAnnouncer;
  private final ExecutorService queryExecutorService;
  private final VersioningPolicy versioningPolicy;
  private final DataSegmentPusher dataSegmentPusher;
  private final SegmentPublisher segmentPublisher;
  private final ServerView serverView;
  private final int maxPendingPersists;

  private final Object handoffCondition = new Object();
  private final Map<Long, Sink> sinks = Maps.newConcurrentMap();
  private final VersionedIntervalTimeline<String, Sink> sinkTimeline = new VersionedIntervalTimeline<String, Sink>(
      String.CASE_INSENSITIVE_ORDER
  );

  private volatile boolean shuttingDown = false;
  private volatile boolean stopped = false;
  private volatile ExecutorService persistExecutor = null;
  private volatile ScheduledExecutorService scheduledExecutor = null;

  public RealtimePlumber(
      Period windowPeriod,
      File basePersistDirectory,
      IndexGranularity segmentGranularity,
      Schema schema,
      FireDepartmentMetrics metrics,
      RejectionPolicy rejectionPolicy,
      ServiceEmitter emitter,
      QueryRunnerFactoryConglomerate conglomerate,
      DataSegmentAnnouncer segmentAnnouncer,
      ExecutorService queryExecutorService,
      VersioningPolicy versioningPolicy,
      DataSegmentPusher dataSegmentPusher,
      SegmentPublisher segmentPublisher,
      ServerView serverView,
      int maxPendingPersists
  )
  {
    this.windowPeriod = windowPeriod;
    this.basePersistDirectory = basePersistDirectory;
    this.segmentGranularity = segmentGranularity;
    this.schema = schema;
    this.metrics = metrics;
    this.rejectionPolicy = rejectionPolicy;
    this.emitter = emitter;
    this.conglomerate = conglomerate;
    this.segmentAnnouncer = segmentAnnouncer;
    this.queryExecutorService = queryExecutorService;
    this.versioningPolicy = versioningPolicy;
    this.dataSegmentPusher = dataSegmentPusher;
    this.segmentPublisher = segmentPublisher;
    this.serverView = serverView;
    this.maxPendingPersists = maxPendingPersists;
  }

  public Schema getSchema()
  {
    return schema;
  }

  public Period getWindowPeriod()
  {
    return windowPeriod;
  }

  public IndexGranularity getSegmentGranularity()
  {
    return segmentGranularity;
  }

  public VersioningPolicy getVersioningPolicy()
  {
    return versioningPolicy;
  }

  public RejectionPolicy getRejectionPolicy()
  {
    return rejectionPolicy;
  }

  public Map<Long, Sink> getSinks()
  {
    return sinks;
  }

  @Override
  public void startJob()
  {
    computeBaseDir(schema).mkdirs();
    initializeExecutors();
    bootstrapSinksFromDisk();
    registerServerViewCallback();
    startPersistThread();
  }

  @Override
  public Sink getSink(long timestamp)
  {
    if (!rejectionPolicy.accept(timestamp)) {
      return null;
    }

    final long truncatedTime = segmentGranularity.truncate(timestamp);

    Sink retVal = sinks.get(truncatedTime);

    if (retVal == null) {
      final Interval sinkInterval = new Interval(
          new DateTime(truncatedTime),
          segmentGranularity.increment(new DateTime(truncatedTime))
      );

      retVal = new Sink(sinkInterval, schema, versioningPolicy.getVersion(sinkInterval));

      try {
        segmentAnnouncer.announceSegment(retVal.getSegment());
        sinks.put(truncatedTime, retVal);
        sinkTimeline.add(retVal.getInterval(), retVal.getVersion(), new SingleElementPartitionChunk<Sink>(retVal));
      }
      catch (IOException e) {
        log.makeAlert(e, "Failed to announce new segment[%s]", schema.getDataSource())
           .addData("interval", retVal.getInterval())
           .emit();
      }
    }

    return retVal;
  }

  @Override
  public <T> QueryRunner<T> getQueryRunner(final Query<T> query)
  {
    final QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(query);
    final QueryToolChest<T, Query<T>> toolchest = factory.getToolchest();

    final Function<Query<T>, ServiceMetricEvent.Builder> builderFn =
        new Function<Query<T>, ServiceMetricEvent.Builder>()
        {

          @Override
          public ServiceMetricEvent.Builder apply(@Nullable Query<T> input)
          {
            return toolchest.makeMetricBuilder(query);
          }
        };

    List<TimelineObjectHolder<String, Sink>> querySinks = Lists.newArrayList();
    for (Interval interval : query.getIntervals()) {
      querySinks.addAll(sinkTimeline.lookup(interval));
    }

    return toolchest.mergeResults(
        factory.mergeRunners(
            queryExecutorService,
            FunctionalIterable
                .create(querySinks)
                .transform(
                    new Function<TimelineObjectHolder<String, Sink>, QueryRunner<T>>()
                    {
                      @Override
                      public QueryRunner<T> apply(TimelineObjectHolder<String, Sink> holder)
                      {
                        final Sink theSink = holder.getObject().getChunk(0).getObject();
                        return new SpecificSegmentQueryRunner<T>(
                            new MetricsEmittingQueryRunner<T>(
                                emitter,
                                builderFn,
                                factory.mergeRunners(
                                    MoreExecutors.sameThreadExecutor(),
                                    Iterables.transform(
                                        theSink,
                                        new Function<FireHydrant, QueryRunner<T>>()
                                        {
                                          @Override
                                          public QueryRunner<T> apply(FireHydrant input)
                                          {
                                            return factory.createRunner(input.getSegment());
                                          }
                                        }
                                    )
                                )
                            ),
                            new SpecificSegmentSpec(
                                new SegmentDescriptor(
                                    holder.getInterval(),
                                    theSink.getSegment().getVersion(),
                                    theSink.getSegment().getShardSpec().getPartitionNum()
                                )
                            )
                        );
                      }
                    }
                )
        )
    );
  }

  @Override
  public void persist(final Runnable commitRunnable)
  {
    final List<Pair<FireHydrant, Interval>> indexesToPersist = Lists.newArrayList();
    for (Sink sink : sinks.values()) {
      if (sink.swappable()) {
        indexesToPersist.add(Pair.of(sink.swap(), sink.getInterval()));
      }
    }

    log.info("Submitting persist runnable for dataSource[%s]", schema.getDataSource());

    persistExecutor.execute(
        new ThreadRenamingRunnable(String.format("%s-incremental-persist", schema.getDataSource()))
        {
          @Override
          public void doRun()
          {
            for (Pair<FireHydrant, Interval> pair : indexesToPersist) {
              metrics.incrementRowOutputCount(persistHydrant(pair.lhs, schema, pair.rhs));
            }
            commitRunnable.run();
          }
        }
    );
  }

  // Submits persist-n-merge task for a Sink to the persistExecutor
  private void persistAndMerge(final long truncatedTime, final Sink sink)
  {
    final String threadName = String.format(
        "%s-%s-persist-n-merge", schema.getDataSource(), new DateTime(truncatedTime)
    );
    persistExecutor.execute(
        new ThreadRenamingRunnable(threadName)
        {
          @Override
          public void doRun()
          {
            final Interval interval = sink.getInterval();

            for (FireHydrant hydrant : sink) {
              if (!hydrant.hasSwapped()) {
                log.info("Hydrant[%s] hasn't swapped yet, swapping. Sink[%s]", hydrant, sink);
                final int rowCount = persistHydrant(hydrant, schema, interval);
                metrics.incrementRowOutputCount(rowCount);
              }
            }

            final File mergedTarget = new File(computePersistDir(schema, interval), "merged");
            if (mergedTarget.exists()) {
              log.info("Skipping already-merged sink: %s", sink);
              return;
            }

            File mergedFile = null;
            try {
              List<QueryableIndex> indexes = Lists.newArrayList();
              for (FireHydrant fireHydrant : sink) {
                Segment segment = fireHydrant.getSegment();
                final QueryableIndex queryableIndex = segment.asQueryableIndex();
                log.info("Adding hydrant[%s]", fireHydrant);
                indexes.add(queryableIndex);
              }

              mergedFile = IndexMerger.mergeQueryableIndex(
                  indexes,
                  schema.getAggregators(),
                  mergedTarget
              );

              QueryableIndex index = IndexIO.loadIndex(mergedFile);

              DataSegment segment = dataSegmentPusher.push(
                  mergedFile,
                  sink.getSegment().withDimensions(Lists.newArrayList(index.getAvailableDimensions()))
              );

              segmentPublisher.publishSegment(segment);
            }
            catch (IOException e) {
              log.makeAlert(e, "Failed to persist merged index[%s]", schema.getDataSource())
                 .addData("interval", interval)
                 .emit();
              if (shuttingDown) {
                // We're trying to shut down, and this segment failed to push. Let's just get rid of it.
                abandonSegment(truncatedTime, sink);
              }
            }

            if (mergedFile != null) {
              try {
                log.info("Deleting Index File[%s]", mergedFile);
                FileUtils.deleteDirectory(mergedFile);
              }
              catch (IOException e) {
                log.warn(e, "Error deleting directory[%s]", mergedFile);
              }
            }
          }
        }
    );
  }

  @Override
  public void finishJob()
  {
    log.info("Shutting down...");

    shuttingDown = true;

    for (final Map.Entry<Long, Sink> entry : sinks.entrySet()) {
      persistAndMerge(entry.getKey(), entry.getValue());
    }

    while (!sinks.isEmpty()) {
      try {
        log.info(
            "Cannot shut down yet! Sinks remaining: %s",
            Joiner.on(", ").join(
                Iterables.transform(
                    sinks.values(),
                    new Function<Sink, String>()
                    {
                      @Override
                      public String apply(Sink input)
                      {
                        return input.getSegment().getIdentifier();
                      }
                    }
                )
            )
        );

        synchronized (handoffCondition) {
          while (!sinks.isEmpty()) {
            handoffCondition.wait();
          }
        }
      }
      catch (InterruptedException e) {
        throw Throwables.propagate(e);
      }
    }

    shutdownExecutors();

    stopped = true;
  }

  protected void initializeExecutors()
  {
    if (persistExecutor == null) {
      // use a blocking single threaded executor to throttle the firehose when write to disk is slow
      persistExecutor = Execs.newBlockingSingleThreaded(
          "plumber_persist_%d", maxPendingPersists
      );
    }
    if (scheduledExecutor == null) {
      scheduledExecutor = Executors.newScheduledThreadPool(
          1,
          new ThreadFactoryBuilder()
              .setDaemon(true)
              .setNameFormat("plumber_scheduled_%d")
              .build()
      );
    }
  }

  protected void shutdownExecutors()
  {
    // scheduledExecutor is shutdown here, but persistExecutor is shutdown when the
    // ServerView sends it a new segment callback
    if (scheduledExecutor != null) {
      scheduledExecutor.shutdown();
    }
  }

  protected void bootstrapSinksFromDisk()
  {
    File baseDir = computeBaseDir(schema);
    if (baseDir == null || !baseDir.exists()) {
      return;
    }

    File[] files = baseDir.listFiles();
    if (files == null) {
      return;
    }

    for (File sinkDir : files) {
      Interval sinkInterval = new Interval(sinkDir.getName().replace("_", "/"));

      //final File[] sinkFiles = sinkDir.listFiles();
      // To avoid reading and listing of "merged" dir
      final File[] sinkFiles = sinkDir.listFiles(
          new FilenameFilter()
          {
            @Override
            public boolean accept(File dir, String fileName)
            {
              return !(Ints.tryParse(fileName) == null);
            }
          }
      );
      Arrays.sort(
          sinkFiles,
          new Comparator<File>()
          {
            @Override
            public int compare(File o1, File o2)
            {
              try {
                return Ints.compare(Integer.parseInt(o1.getName()), Integer.parseInt(o2.getName()));
              }
              catch (NumberFormatException e) {
                log.error(e, "Couldn't compare as numbers? [%s][%s]", o1, o2);
                return o1.compareTo(o2);
              }
            }
          }
      );

      try {
        List<FireHydrant> hydrants = Lists.newArrayList();
        for (File segmentDir : sinkFiles) {
          log.info("Loading previously persisted segment at [%s]", segmentDir);

          // Although this has been tackled at start of this method.
          // Just a doubly-check added to skip "merged" dir. from being added to hydrants
          // If 100% sure that this is not needed, this check can be removed.
          if (Ints.tryParse(segmentDir.getName()) == null) {
            continue;
          }

          hydrants.add(
              new FireHydrant(
                  new QueryableIndexSegment(
                      DataSegment.makeDataSegmentIdentifier(
                          schema.getDataSource(),
                          sinkInterval.getStart(),
                          sinkInterval.getEnd(),
                          versioningPolicy.getVersion(sinkInterval),
                          schema.getShardSpec()
                      ),
                      IndexIO.loadIndex(segmentDir)
                  ),
                  Integer.parseInt(segmentDir.getName())
              )
          );
        }

        Sink currSink = new Sink(sinkInterval, schema, versioningPolicy.getVersion(sinkInterval), hydrants);
        sinks.put(sinkInterval.getStartMillis(), currSink);
        sinkTimeline.add(
            currSink.getInterval(),
            currSink.getVersion(),
            new SingleElementPartitionChunk<Sink>(currSink)
        );

        segmentAnnouncer.announceSegment(currSink.getSegment());
      }
      catch (IOException e) {
        log.makeAlert(e, "Problem loading sink[%s] from disk.", schema.getDataSource())
           .addData("interval", sinkInterval)
           .emit();
      }
    }
  }

  protected void startPersistThread()
  {
    final long truncatedNow = segmentGranularity.truncate(new DateTime()).getMillis();
    final long windowMillis = windowPeriod.toStandardDuration().getMillis();

    log.info(
        "Expect to run at [%s]",
        new DateTime().plus(
            new Duration(System.currentTimeMillis(), segmentGranularity.increment(truncatedNow) + windowMillis)
        )
    );

    ScheduledExecutors
        .scheduleAtFixedRate(
            scheduledExecutor,
            new Duration(System.currentTimeMillis(), segmentGranularity.increment(truncatedNow) + windowMillis),
            new Duration(truncatedNow, segmentGranularity.increment(truncatedNow)),
            new ThreadRenamingCallable<ScheduledExecutors.Signal>(
                String.format(
                    "%s-overseer-%d",
                    schema.getDataSource(),
                    schema.getShardSpec().getPartitionNum()
                )
            )
            {
              @Override
              public ScheduledExecutors.Signal doCall()
              {
                if (stopped) {
                  log.info("Stopping merge-n-push overseer thread");
                  return ScheduledExecutors.Signal.STOP;
                }

                log.info("Starting merge and push.");

                long minTimestamp = segmentGranularity.truncate(
                    rejectionPolicy.getCurrMaxTime().minus(windowMillis)
                ).getMillis();

                List<Map.Entry<Long, Sink>> sinksToPush = Lists.newArrayList();
                for (Map.Entry<Long, Sink> entry : sinks.entrySet()) {
                  final Long intervalStart = entry.getKey();
                  if (intervalStart < minTimestamp) {
                    log.info("Adding entry[%s] for merge and push.", entry);
                    sinksToPush.add(entry);
                  }
                }

                for (final Map.Entry<Long, Sink> entry : sinksToPush) {
                  persistAndMerge(entry.getKey(), entry.getValue());
                }

                if (stopped) {
                  log.info("Stopping merge-n-push overseer thread");
                  return ScheduledExecutors.Signal.STOP;
                } else {
                  return ScheduledExecutors.Signal.REPEAT;
                }
              }
            }
        );
  }

  /**
   * Unannounces a given sink and removes all local references to it.
   */
  protected void abandonSegment(final long truncatedTime, final Sink sink)
  {
    try {
      segmentAnnouncer.unannounceSegment(sink.getSegment());
      FileUtils.deleteDirectory(computePersistDir(schema, sink.getInterval()));
      log.info("Removing sinkKey %d for segment %s", truncatedTime, sink.getSegment().getIdentifier());
      sinks.remove(truncatedTime);
      sinkTimeline.remove(
          sink.getInterval(),
          sink.getVersion(),
          new SingleElementPartitionChunk<>(sink)
      );
      synchronized (handoffCondition) {
        handoffCondition.notifyAll();
      }
    }
    catch (IOException e) {
      log.makeAlert(e, "Unable to abandon old segment for dataSource[%s]", schema.getDataSource())
         .addData("interval", sink.getInterval())
         .emit();
    }
  }

  protected File computeBaseDir(Schema schema)
  {
    return new File(basePersistDirectory, schema.getDataSource());
  }

  protected File computePersistDir(Schema schema, Interval interval)
  {
    return new File(computeBaseDir(schema), interval.toString().replace("/", "_"));
  }

  /**
   * Persists the given hydrant and returns the number of rows persisted
   *
   * @param indexToPersist
   * @param schema
   * @param interval
   *
   * @return the number of rows persisted
   */
  protected int persistHydrant(FireHydrant indexToPersist, Schema schema, Interval interval)
  {
    if (indexToPersist.hasSwapped()) {
      log.info(
          "DataSource[%s], Interval[%s], Hydrant[%s] already swapped. Ignoring request to persist.",
          schema.getDataSource(), interval, indexToPersist
      );
      return 0;
    }

    log.info("DataSource[%s], Interval[%s], persisting Hydrant[%s]", schema.getDataSource(), interval, indexToPersist);
    try {
      int numRows = indexToPersist.getIndex().size();

      File persistedFile = IndexMerger.persist(
          indexToPersist.getIndex(),
          new File(computePersistDir(schema, interval), String.valueOf(indexToPersist.getCount()))
      );

      indexToPersist.swapSegment(
          new QueryableIndexSegment(
              indexToPersist.getSegment().getIdentifier(),
              IndexIO.loadIndex(persistedFile)
          )
      );

      return numRows;
    }
    catch (IOException e) {
      log.makeAlert("dataSource[%s] -- incremental persist failed", schema.getDataSource())
         .addData("interval", interval)
         .addData("count", indexToPersist.getCount())
         .emit();

      throw Throwables.propagate(e);
    }
  }

  private void registerServerViewCallback()
  {
    serverView.registerSegmentCallback(
        persistExecutor,
        new ServerView.BaseSegmentCallback()
        {
          @Override
          public ServerView.CallbackAction segmentAdded(DruidServer server, DataSegment segment)
          {
            if (stopped) {
              log.info("Unregistering ServerViewCallback");
              persistExecutor.shutdown();
              return ServerView.CallbackAction.UNREGISTER;
            }

            if (!server.isAssignable()) {
              return ServerView.CallbackAction.CONTINUE;
            }

            log.debug("Checking segment[%s] on server[%s]", segment, server);
            if (schema.getDataSource().equals(segment.getDataSource())) {
              final Interval interval = segment.getInterval();
              for (Map.Entry<Long, Sink> entry : sinks.entrySet()) {
                final Long sinkKey = entry.getKey();
                if (interval.contains(sinkKey)) {
                  final Sink sink = entry.getValue();
                  log.info("Segment[%s] matches sink[%s] on server[%s]", segment, sink, server);

                  final String segmentVersion = segment.getVersion();
                  final String sinkVersion = sink.getSegment().getVersion();
                  if (segmentVersion.compareTo(sinkVersion) >= 0) {
                    log.info("Segment version[%s] >= sink version[%s]", segmentVersion, sinkVersion);
                    abandonSegment(sinkKey, sink);
                  }
                }
              }
            }

            return ServerView.CallbackAction.CONTINUE;
          }
        }
    );
  }
}
