/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.realtime.plumber;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.metamx.common.Pair;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.druid.Query;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.client.DruidServer;
import com.metamx.druid.client.ServerView;
import com.metamx.druid.coordination.DataSegmentAnnouncer;
import com.metamx.druid.guava.ThreadRenamingCallable;
import com.metamx.druid.guava.ThreadRenamingRunnable;
import com.metamx.druid.index.QueryableIndex;
import com.metamx.druid.index.QueryableIndexSegment;
import com.metamx.druid.index.Segment;
import com.metamx.druid.index.v1.IndexGranularity;
import com.metamx.druid.index.v1.IndexIO;
import com.metamx.druid.index.v1.IndexMerger;
import com.metamx.druid.loading.DataSegmentPusher;
import com.metamx.druid.query.MetricsEmittingQueryRunner;
import com.metamx.druid.query.QueryRunner;
import com.metamx.druid.query.QueryRunnerFactory;
import com.metamx.druid.query.QueryRunnerFactoryConglomerate;
import com.metamx.druid.query.QueryToolChest;
import com.metamx.druid.query.segment.SegmentDescriptor;
import com.metamx.druid.query.segment.SpecificSegmentQueryRunner;
import com.metamx.druid.query.segment.SpecificSegmentSpec;
import com.metamx.druid.realtime.FireDepartmentMetrics;
import com.metamx.druid.realtime.FireHydrant;
import com.metamx.druid.realtime.Schema;
import com.metamx.druid.realtime.SegmentPublisher;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
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
public class RealtimePlumberSchool implements PlumberSchool
{
  private static final EmittingLogger log = new EmittingLogger(RealtimePlumberSchool.class);
  private static final ListeningExecutorService EXEC = MoreExecutors.sameThreadExecutor();

  private final Period windowPeriod;
  private final File basePersistDirectory;
  private final IndexGranularity segmentGranularity;
  private final Object handoffCondition = new Object();

  private ServiceEmitter emitter;

  private volatile VersioningPolicy versioningPolicy = null;
  private volatile RejectionPolicyFactory rejectionPolicyFactory = null;
  private volatile QueryRunnerFactoryConglomerate conglomerate = null;
  private volatile DataSegmentPusher dataSegmentPusher = null;
  private volatile DataSegmentAnnouncer segmentAnnouncer = null;
  private volatile SegmentPublisher segmentPublisher = null;
  private volatile ServerView serverView = null;

  @JsonCreator
  public RealtimePlumberSchool(
      @JsonProperty("windowPeriod") Period windowPeriod,
      @JsonProperty("basePersistDirectory") File basePersistDirectory,
      @JsonProperty("segmentGranularity") IndexGranularity segmentGranularity
  )
  {
    this.windowPeriod = windowPeriod;
    this.basePersistDirectory = basePersistDirectory;
    this.segmentGranularity = segmentGranularity;
    this.versioningPolicy = new IntervalStartVersioningPolicy();
    this.rejectionPolicyFactory = new ServerTimeRejectionPolicyFactory();

    Preconditions.checkNotNull(windowPeriod, "RealtimePlumberSchool requires a windowPeriod.");
    Preconditions.checkNotNull(basePersistDirectory, "RealtimePlumberSchool requires a basePersistDirectory.");
    Preconditions.checkNotNull(segmentGranularity, "RealtimePlumberSchool requires a segmentGranularity.");
  }

  @JsonProperty("versioningPolicy")
  public void setVersioningPolicy(VersioningPolicy versioningPolicy)
  {
    this.versioningPolicy = versioningPolicy;
  }

  @JsonProperty("rejectionPolicy")
  public void setRejectionPolicyFactory(RejectionPolicyFactory factory)
  {
    this.rejectionPolicyFactory = factory;
  }

  @JacksonInject("queryRunnerFactoryConglomerate")
  public void setConglomerate(QueryRunnerFactoryConglomerate conglomerate)
  {
    this.conglomerate = conglomerate;
  }

  @JacksonInject("segmentPusher")
  public void setDataSegmentPusher(DataSegmentPusher dataSegmentPusher)
  {
    this.dataSegmentPusher = dataSegmentPusher;
  }

  @JacksonInject("segmentAnnouncer")
  public void setSegmentAnnouncer(DataSegmentAnnouncer segmentAnnouncer)
  {
    this.segmentAnnouncer = segmentAnnouncer;
  }

  @JacksonInject("segmentPublisher")
  public void setSegmentPublisher(SegmentPublisher segmentPublisher)
  {
    this.segmentPublisher = segmentPublisher;
  }

  @JacksonInject("serverView")
  public void setServerView(ServerView serverView)
  {
    this.serverView = serverView;
  }

  @JacksonInject("serviceEmitter")
  public void setServiceEmitter(ServiceEmitter emitter)
  {
    this.emitter = emitter;
  }

  @Override
  public Plumber findPlumber(final Schema schema, final FireDepartmentMetrics metrics)
  {
    verifyState();

    final RejectionPolicy rejectionPolicy = rejectionPolicyFactory.create(windowPeriod);
    log.info("Creating plumber using rejectionPolicy[%s]", rejectionPolicy);

    return new Plumber()
    {
      private volatile boolean stopped = false;
      private volatile ExecutorService persistExecutor = null;
      private volatile ScheduledExecutorService scheduledExecutor = null;

      private final Map<Long, Sink> sinks = Maps.newConcurrentMap();

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

        return toolchest.mergeResults(
            factory.mergeRunners(
                EXEC,
                FunctionalIterable
                    .create(sinks.values())
                    .transform(
                        new Function<Sink, QueryRunner<T>>()
                        {
                          @Override
                          public QueryRunner<T> apply(Sink input)
                          {
                            return new SpecificSegmentQueryRunner<T>(
                                new MetricsEmittingQueryRunner<T>(
                                    emitter,
                                    builderFn,
                                    factory.mergeRunners(
                                        EXEC,
                                        Iterables.transform(
                                            input,
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
                                        input.getInterval(),
                                        input.getSegment().getVersion(),
                                        input.getSegment().getShardSpec().getPartitionNum()
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

      @Override
      public void finishJob()
      {
        log.info("Shutting down...");

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

        // scheduledExecutor is shutdown here, but persistExecutor is shutdown when the
        // ServerView sends it a new segment callback
        if (scheduledExecutor != null) {
          scheduledExecutor.shutdown();
        }

        stopped = true;
      }

      private void initializeExecutors()
      {
        if (persistExecutor == null) {
          persistExecutor = Executors.newFixedThreadPool(
              1,
              new ThreadFactoryBuilder()
                  .setDaemon(true)
                  .setNameFormat("plumber_persist_%d")
                  .build()
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

      private void bootstrapSinksFromDisk()
      {
        for (File sinkDir : computeBaseDir(schema).listFiles()) {
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
                      new QueryableIndexSegment(null, IndexIO.loadIndex(segmentDir)),
                      Integer.parseInt(segmentDir.getName())
                  )
              );
            }

            Sink currSink = new Sink(sinkInterval, schema, versioningPolicy.getVersion(sinkInterval), hydrants);
            sinks.put(sinkInterval.getStartMillis(), currSink);

            segmentAnnouncer.announceSegment(currSink.getSegment());
          }
          catch (IOException e) {
            log.makeAlert(e, "Problem loading sink[%s] from disk.", schema.getDataSource())
               .addData("interval", sinkInterval)
               .emit();
          }
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

                if ("realtime".equals(server.getType())) {
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
                        try {
                          segmentAnnouncer.unannounceSegment(sink.getSegment());
                          FileUtils.deleteDirectory(computePersistDir(schema, sink.getInterval()));
                          log.info("Removing sinkKey %d for segment %s", sinkKey, sink.getSegment().getIdentifier());
                          sinks.remove(sinkKey);

                          synchronized (handoffCondition) {
                            handoffCondition.notifyAll();
                          }
                        }
                        catch (IOException e) {
                          log.makeAlert(e, "Unable to delete old segment for dataSource[%s].", schema.getDataSource())
                             .addData("interval", sink.getInterval())
                             .emit();
                        }
                      }
                    }
                  }
                }

                return ServerView.CallbackAction.CONTINUE;
              }
            }
        );
      }

      private void startPersistThread()
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
                      final Sink sink = entry.getValue();

                      final String threadName = String.format(
                          "%s-%s-persist-n-merge", schema.getDataSource(), new DateTime(entry.getKey())
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
                                    new File(computePersistDir(schema, interval), "merged")
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
                              }

                              if (mergedFile != null) {
                                try {
                                  if (mergedFile != null) {
                                    log.info("Deleting Index File[%s]", mergedFile);
                                    FileUtils.deleteDirectory(mergedFile);
                                  }
                                }
                                catch (IOException e) {
                                  log.warn(e, "Error deleting directory[%s]", mergedFile);
                                }
                              }
                            }
                          }
                      );
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
    };
  }

  private File computeBaseDir(Schema schema)
  {
    return new File(basePersistDirectory, schema.getDataSource());
  }

  private File computePersistDir(Schema schema, Interval interval)
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
  private int persistHydrant(FireHydrant indexToPersist, Schema schema, Interval interval)
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

      indexToPersist.swapSegment(new QueryableIndexSegment(null, IndexIO.loadIndex(persistedFile)));

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

  private void verifyState()
  {
    Preconditions.checkNotNull(conglomerate, "must specify a queryRunnerFactoryConglomerate to do this action.");
    Preconditions.checkNotNull(dataSegmentPusher, "must specify a segmentPusher to do this action.");
    Preconditions.checkNotNull(segmentAnnouncer, "must specify a segmentAnnouncer to do this action.");
    Preconditions.checkNotNull(segmentPublisher, "must specify a segmentPublisher to do this action.");
    Preconditions.checkNotNull(serverView, "must specify a serverView to do this action.");
    Preconditions.checkNotNull(emitter, "must specify a serviceEmitter to do this action.");
  }
}
