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

package io.druid.segment.realtime.plumber;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.metamx.common.Granularity;
import com.metamx.common.ISE;
import com.metamx.common.Pair;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.client.FilteredServerView;
import io.druid.client.ServerView;
import io.druid.common.guava.ThreadRenamingCallable;
import io.druid.common.guava.ThreadRenamingRunnable;
import io.druid.concurrent.Execs;
import io.druid.data.input.InputRow;
import io.druid.query.MetricsEmittingQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.query.QueryToolChest;
import io.druid.query.ReportTimelineMissingSegmentQueryRunner;
import io.druid.query.SegmentDescriptor;
import io.druid.query.spec.SpecificSegmentQueryRunner;
import io.druid.query.spec.SpecificSegmentSpec;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMaker;
import io.druid.segment.IndexMerger;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.Segment;
import io.druid.segment.incremental.IndexSizeExceededException;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.FireHydrant;
import io.druid.segment.realtime.SegmentPublisher;
import io.druid.server.coordination.DataSegmentAnnouncer;
import io.druid.server.coordination.DruidServerMetadata;
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
import java.io.Closeable;
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
import java.util.concurrent.TimeUnit;

/**
 */
public class RealtimePlumber implements Plumber
{
  private static final EmittingLogger log = new EmittingLogger(RealtimePlumber.class);
  private static final int WARN_DELAY = 1000;

  private final DataSchema schema;
  private final RealtimeTuningConfig config;
  private final RejectionPolicy rejectionPolicy;
  private final FireDepartmentMetrics metrics;
  private final ServiceEmitter emitter;
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final DataSegmentAnnouncer segmentAnnouncer;
  private final ExecutorService queryExecutorService;
  private final DataSegmentPusher dataSegmentPusher;
  private final SegmentPublisher segmentPublisher;
  private final FilteredServerView serverView;
  private final Object handoffCondition = new Object();
  private final Map<Long, Sink> sinks = Maps.newConcurrentMap();
  private final VersionedIntervalTimeline<String, Sink> sinkTimeline = new VersionedIntervalTimeline<String, Sink>(
      String.CASE_INSENSITIVE_ORDER
  );

  private volatile boolean shuttingDown = false;
  private volatile boolean stopped = false;
  private volatile boolean cleanShutdown = true;
  private volatile ExecutorService persistExecutor = null;
  private volatile ExecutorService mergeExecutor = null;
  private volatile ScheduledExecutorService scheduledExecutor = null;


  public RealtimePlumber(
      DataSchema schema,
      RealtimeTuningConfig config,
      FireDepartmentMetrics metrics,
      ServiceEmitter emitter,
      QueryRunnerFactoryConglomerate conglomerate,
      DataSegmentAnnouncer segmentAnnouncer,
      ExecutorService queryExecutorService,
      DataSegmentPusher dataSegmentPusher,
      SegmentPublisher segmentPublisher,
      FilteredServerView serverView
  )
  {
    this.schema = schema;
    this.config = config;
    this.rejectionPolicy = config.getRejectionPolicyFactory().create(config.getWindowPeriod());
    this.metrics = metrics;
    this.emitter = emitter;
    this.conglomerate = conglomerate;
    this.segmentAnnouncer = segmentAnnouncer;
    this.queryExecutorService = queryExecutorService;
    this.dataSegmentPusher = dataSegmentPusher;
    this.segmentPublisher = segmentPublisher;
    this.serverView = serverView;

    log.info("Creating plumber using rejectionPolicy[%s]", getRejectionPolicy());
  }

  public DataSchema getSchema()
  {
    return schema;
  }

  public RealtimeTuningConfig getConfig()
  {
    return config;
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
    // Push pending sinks bootstrapped from previous run
    mergeAndPush();
  }

  @Override
  public int add(InputRow row) throws IndexSizeExceededException
  {
    final Sink sink = getSink(row.getTimestampFromEpoch());
    if (sink == null) {
      return -1;
    }

    return sink.add(row);
  }

  public Sink getSink(long timestamp)
  {
    if (!rejectionPolicy.accept(timestamp)) {
      return null;
    }

    final Granularity segmentGranularity = schema.getGranularitySpec().getSegmentGranularity();
    final VersioningPolicy versioningPolicy = config.getVersioningPolicy();

    final long truncatedTime = segmentGranularity.truncate(new DateTime(timestamp)).getMillis();

    Sink retVal = sinks.get(truncatedTime);

    if (retVal == null) {
      final Interval sinkInterval = new Interval(
          new DateTime(truncatedTime),
          segmentGranularity.increment(new DateTime(truncatedTime))
      );

      retVal = new Sink(sinkInterval, schema, config, versioningPolicy.getVersion(sinkInterval));

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
                        if (holder == null) {
                          throw new ISE("No timeline entry at all!");
                        }

                        // The realtime plumber always uses SingleElementPartitionChunk
                        final Sink theSink = holder.getObject().getChunk(0).getObject();

                        if (theSink == null) {
                          throw new ISE("Missing sink for timeline entry[%s]!", holder);
                        }

                        final SegmentDescriptor descriptor = new SegmentDescriptor(
                            holder.getInterval(),
                            theSink.getSegment().getVersion(),
                            theSink.getSegment().getShardSpec().getPartitionNum()
                        );

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
                                            // It is possible that we got a query for a segment, and while that query
                                            // is in the jetty queue, the segment is abandoned. Here, we need to retry
                                            // the query for the segment.
                                            if (input == null || input.getSegment() == null) {
                                              return new ReportTimelineMissingSegmentQueryRunner<T>(descriptor);
                                            }

                                            // Prevent the underlying segment from closing when its being iterated
                                            final Closeable closeable = input.getSegment().increment();
                                            try {
                                              return factory.createRunner(input.getSegment());
                                            }
                                            finally {
                                              try {
                                                if (closeable != null) {
                                                  closeable.close();
                                                }
                                              }
                                              catch (IOException e) {
                                                throw Throwables.propagate(e);
                                              }
                                            }
                                          }
                                        }
                                    )
                                )
                            ).withWaitMeasuredFromNow(),
                            new SpecificSegmentSpec(
                                descriptor
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

    final Stopwatch runExecStopwatch = Stopwatch.createStarted();
    final Stopwatch persistStopwatch = Stopwatch.createStarted();
    persistExecutor.execute(
        new ThreadRenamingRunnable(String.format("%s-incremental-persist", schema.getDataSource()))
        {
          @Override
          public void doRun()
          {
            try {
              for (Pair<FireHydrant, Interval> pair : indexesToPersist) {
                metrics.incrementRowOutputCount(persistHydrant(pair.lhs, schema, pair.rhs));
              }
              commitRunnable.run();
            }
            catch (Exception e) {
              metrics.incrementFailedPersists();
              throw e;
            }
            finally {
              metrics.incrementNumPersists();
              metrics.incrementPersistTimeMillis(persistStopwatch.elapsed(TimeUnit.MILLISECONDS));
              persistStopwatch.stop();
            }
          }
        }
    );

    final long startDelay = runExecStopwatch.elapsed(TimeUnit.MILLISECONDS);
    metrics.incrementPersistBackPressureMillis(startDelay);
    if (startDelay > WARN_DELAY) {
      log.warn("Ingestion was throttled for [%,d] millis because persists were pending.", startDelay);
    }
    runExecStopwatch.stop();
  }

  // Submits persist-n-merge task for a Sink to the mergeExecutor
  private void persistAndMerge(final long truncatedTime, final Sink sink)
  {
    final String threadName = String.format(
        "%s-%s-persist-n-merge", schema.getDataSource(), new DateTime(truncatedTime)
    );
    mergeExecutor.execute(
        new ThreadRenamingRunnable(threadName)
        {
          @Override
          public void doRun()
          {
            final Interval interval = sink.getInterval();

            // Bail out if this sink has been abandoned by a previously-executed task.
            if (sinks.get(truncatedTime) != sink) {
              log.info("Sink[%s] was abandoned, bailing out of persist-n-merge.", sink);
              return;
            }

            // Use a file to indicate that pushing has completed.
            final File persistDir = computePersistDir(schema, interval);
            final File mergedTarget = new File(persistDir, "merged");
            final File isPushedMarker = new File(persistDir, "isPushedMarker");

            if (!isPushedMarker.exists()) {
              removeSegment(sink, mergedTarget);
              if (mergedTarget.exists()) {
                log.wtf("Merged target[%s] exists?!", mergedTarget);
                return;
              }
            } else {
              log.info("Already pushed sink[%s]", sink);
              return;
            }

            for (FireHydrant hydrant : sink) {
              synchronized (hydrant) {
                if (!hydrant.hasSwapped()) {
                  log.info("Hydrant[%s] hasn't swapped yet, swapping. Sink[%s]", hydrant, sink);
                  final int rowCount = persistHydrant(hydrant, schema, interval);
                  metrics.incrementRowOutputCount(rowCount);
                }
              }
            }

            try {
              List<QueryableIndex> indexes = Lists.newArrayList();
              for (FireHydrant fireHydrant : sink) {
                Segment segment = fireHydrant.getSegment();
                final QueryableIndex queryableIndex = segment.asQueryableIndex();
                log.info("Adding hydrant[%s]", fireHydrant);
                indexes.add(queryableIndex);
              }

              final File mergedFile;
              if (config.isPersistInHeap()) {
                mergedFile = IndexMaker.mergeQueryableIndex(
                    indexes,
                    schema.getAggregators(),
                    mergedTarget
                );
              } else {
                mergedFile = IndexMerger.mergeQueryableIndex(
                    indexes,
                    schema.getAggregators(),
                    mergedTarget
                );
              }

              QueryableIndex index = IndexIO.loadIndex(mergedFile);

              DataSegment segment = dataSegmentPusher.push(
                  mergedFile,
                  sink.getSegment().withDimensions(Lists.newArrayList(index.getAvailableDimensions()))
              );

              segmentPublisher.publishSegment(segment);

              if (!isPushedMarker.createNewFile()) {
                log.makeAlert("Failed to create marker file for [%s]", schema.getDataSource())
                   .addData("interval", sink.getInterval())
                   .addData("partitionNum", segment.getShardSpec().getPartitionNum())
                   .addData("marker", isPushedMarker)
                   .emit();
              }
            }
            catch (Exception e) {
              metrics.incrementFailedHandoffs();
              log.makeAlert(e, "Failed to persist merged index[%s]", schema.getDataSource())
                 .addData("interval", interval)
                 .emit();
              if (shuttingDown) {
                // We're trying to shut down, and this segment failed to push. Let's just get rid of it.
                // This call will also delete possibly-partially-written files, so we don't need to do it explicitly.
                cleanShutdown = false;
                abandonSegment(truncatedTime, sink);
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

    if (!cleanShutdown) {
      throw new ISE("Exception occurred during persist and merge.");
    }
  }

  protected void initializeExecutors()
  {
    final int maxPendingPersists = config.getMaxPendingPersists();

    if (persistExecutor == null) {
      // use a blocking single threaded executor to throttle the firehose when write to disk is slow
      persistExecutor = Execs.newBlockingSingleThreaded(
          "plumber_persist_%d", maxPendingPersists
      );
    }
    if (mergeExecutor == null) {
      // use a blocking single threaded executor to throttle the firehose when write to disk is slow
      mergeExecutor = Execs.newBlockingSingleThreaded(
          "plumber_merge_%d", 1
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
    // scheduledExecutor is shutdown here, but mergeExecutor is shutdown when the
    // ServerView sends it a new segment callback
    if (scheduledExecutor != null) {
      scheduledExecutor.shutdown();
      persistExecutor.shutdown();
    }
  }

  protected void bootstrapSinksFromDisk()
  {
    final VersioningPolicy versioningPolicy = config.getVersioningPolicy();

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
                          config.getShardSpec()
                      ),
                      IndexIO.loadIndex(segmentDir)
                  ),
                  Integer.parseInt(segmentDir.getName())
              )
          );
        }

        Sink currSink = new Sink(sinkInterval, schema, config, versioningPolicy.getVersion(sinkInterval), hydrants);
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
    final Granularity segmentGranularity = schema.getGranularitySpec().getSegmentGranularity();
    final Period windowPeriod = config.getWindowPeriod();

    final DateTime truncatedNow = segmentGranularity.truncate(new DateTime());
    final long windowMillis = windowPeriod.toStandardDuration().getMillis();

    log.info(
        "Expect to run at [%s]",
        new DateTime().plus(
            new Duration(
                System.currentTimeMillis(),
                segmentGranularity.increment(truncatedNow).getMillis() + windowMillis
            )
        )
    );

    ScheduledExecutors
        .scheduleAtFixedRate(
            scheduledExecutor,
            new Duration(
                System.currentTimeMillis(),
                segmentGranularity.increment(truncatedNow).getMillis() + windowMillis
            ),
            new Duration(truncatedNow, segmentGranularity.increment(truncatedNow)),
            new ThreadRenamingCallable<ScheduledExecutors.Signal>(
                String.format(
                    "%s-overseer-%d",
                    schema.getDataSource(),
                    config.getShardSpec().getPartitionNum()
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

                mergeAndPush();

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

  private void mergeAndPush()
  {
    final Granularity segmentGranularity = schema.getGranularitySpec().getSegmentGranularity();
    final Period windowPeriod = config.getWindowPeriod();

    final long windowMillis = windowPeriod.toStandardDuration().getMillis();
    log.info("Starting merge and push.");
    DateTime minTimestampAsDate = segmentGranularity.truncate(
        new DateTime(
            Math.max(
                windowMillis,
                rejectionPolicy.getCurrMaxTime()
                               .getMillis()
            )
            - windowMillis
        )
    );
    long minTimestamp = minTimestampAsDate.getMillis();

    log.info("Found [%,d] sinks. minTimestamp [%s]", sinks.size(), minTimestampAsDate);

    List<Map.Entry<Long, Sink>> sinksToPush = Lists.newArrayList();
    for (Map.Entry<Long, Sink> entry : sinks.entrySet()) {
      final Long intervalStart = entry.getKey();
      if (intervalStart < minTimestamp) {
        log.info("Adding entry[%s] for merge and push.", entry);
        sinksToPush.add(entry);
      } else {
        log.warn(
            "[%s] < [%s] Skipping persist and merge.",
            new DateTime(intervalStart),
            minTimestampAsDate
        );
      }
    }

    log.info("Found [%,d] sinks to persist and merge", sinksToPush.size());

    for (final Map.Entry<Long, Sink> entry : sinksToPush) {
      persistAndMerge(entry.getKey(), entry.getValue());
    }
  }

  /**
   * Unannounces a given sink and removes all local references to it. It is important that this is only called
   * from the single-threaded mergeExecutor, since otherwise chaos may ensue if merged segments are deleted while
   * being created.
   *
   * @param truncatedTime sink key
   * @param sink          sink to unannounce
   */
  protected void abandonSegment(final long truncatedTime, final Sink sink)
  {
    try {
      segmentAnnouncer.unannounceSegment(sink.getSegment());
      removeSegment(sink, computePersistDir(schema, sink.getInterval()));
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
    catch (Exception e) {
      log.makeAlert(e, "Unable to abandon old segment for dataSource[%s]", schema.getDataSource())
         .addData("interval", sink.getInterval())
         .emit();
    }
  }

  protected File computeBaseDir(DataSchema schema)
  {
    return new File(config.getBasePersistDirectory(), schema.getDataSource());
  }

  protected File computePersistDir(DataSchema schema, Interval interval)
  {
    return new File(computeBaseDir(schema), interval.toString().replace("/", "_"));
  }

  /**
   * Persists the given hydrant and returns the number of rows persisted
   *
   * @param indexToPersist hydrant to persist
   * @param schema         datasource schema
   * @param interval       interval to persist
   *
   * @return the number of rows persisted
   */
  protected int persistHydrant(FireHydrant indexToPersist, DataSchema schema, Interval interval)
  {
    synchronized (indexToPersist) {
      if (indexToPersist.hasSwapped()) {
        log.info(
            "DataSource[%s], Interval[%s], Hydrant[%s] already swapped. Ignoring request to persist.",
            schema.getDataSource(), interval, indexToPersist
        );
        return 0;
      }

      log.info(
          "DataSource[%s], Interval[%s], persisting Hydrant[%s]",
          schema.getDataSource(),
          interval,
          indexToPersist
      );
      try {
        int numRows = indexToPersist.getIndex().size();

        final File persistedFile;
        if (config.isPersistInHeap()) {
          persistedFile = IndexMaker.persist(
              indexToPersist.getIndex(),
              new File(computePersistDir(schema, interval), String.valueOf(indexToPersist.getCount()))
          );
        } else {
          persistedFile = IndexMerger.persist(
              indexToPersist.getIndex(),
              new File(computePersistDir(schema, interval), String.valueOf(indexToPersist.getCount()))
          );
        }

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
  }

  private void registerServerViewCallback()
  {
    serverView.registerSegmentCallback(
        mergeExecutor,
        new ServerView.BaseSegmentCallback()
        {
          @Override
          public ServerView.CallbackAction segmentAdded(DruidServerMetadata server, DataSegment segment)
          {
            if (stopped) {
              log.info("Unregistering ServerViewCallback");
              mergeExecutor.shutdown();
              return ServerView.CallbackAction.UNREGISTER;
            }

            if (!server.isAssignable()) {
              return ServerView.CallbackAction.CONTINUE;
            }

            log.debug("Checking segment[%s] on server[%s]", segment, server);
            if (schema.getDataSource().equals(segment.getDataSource())
                && config.getShardSpec().getPartitionNum() == segment.getShardSpec().getPartitionNum()
                ) {
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
        },
        new Predicate<DataSegment>()
        {
          @Override
          public boolean apply(final DataSegment segment)
          {
            return
                schema.getDataSource().equalsIgnoreCase(segment.getDataSource())
                && config.getShardSpec().getPartitionNum() == segment.getShardSpec().getPartitionNum()
                && Iterables.any(
                    sinks.keySet(), new Predicate<Long>()
                    {
                      @Override
                      public boolean apply(Long sinkKey)
                      {
                        return segment.getInterval().contains(sinkKey);
                      }
                    }
                );
          }
        }
    );
  }

  private void removeSegment(final Sink sink, final File target)
  {
    if (target.exists()) {
      try {
        log.info("Deleting Index File[%s]", target);
        FileUtils.deleteDirectory(target);
      }
      catch (Exception e) {
        log.makeAlert(e, "Unable to remove file for dataSource[%s]", schema.getDataSource())
           .addData("file", target)
           .addData("interval", sink.getInterval())
           .emit();
      }
    }
  }
}
