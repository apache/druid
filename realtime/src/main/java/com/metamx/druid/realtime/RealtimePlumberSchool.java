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

package com.metamx.druid.realtime;

import com.google.common.base.Function;
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
import com.metamx.druid.StorageAdapter;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.client.DruidServer;
import com.metamx.druid.client.ServerView;
import com.metamx.druid.guava.ThreadRenamingRunnable;
import com.metamx.druid.index.QueryableIndex;
import com.metamx.druid.index.QueryableIndexSegment;
import com.metamx.druid.index.Segment;
import com.metamx.druid.index.v1.IndexGranularity;
import com.metamx.druid.index.v1.IndexIO;
import com.metamx.druid.index.v1.IndexMerger;
import com.metamx.druid.index.v1.MMappedIndex;
import com.metamx.druid.index.v1.MMappedIndexQueryableIndex;
import com.metamx.druid.index.v1.MMappedIndexStorageAdapter;
import com.metamx.druid.loading.SegmentPusher;
import com.metamx.druid.query.MetricsEmittingQueryRunner;
import com.metamx.druid.query.QueryRunner;
import com.metamx.druid.query.QueryRunnerFactory;
import com.metamx.druid.query.QueryRunnerFactoryConglomerate;
import com.metamx.druid.query.QueryToolChest;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import org.apache.commons.io.FileUtils;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;
import org.codehaus.jackson.map.annotate.JacksonInject;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
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

  private volatile Executor persistExecutor = null;
  private volatile ScheduledExecutorService scheduledExecutor = null;

  private volatile RejectionPolicyFactory rejectionPolicyFactory = null;
  private volatile QueryRunnerFactoryConglomerate conglomerate = null;
  private volatile SegmentPusher segmentPusher = null;
  private volatile MetadataUpdater metadataUpdater = null;
  private volatile ServerView serverView = null;
  private ServiceEmitter emitter;

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
    this.rejectionPolicyFactory = new ServerTimeRejectionPolicyFactory();

    Preconditions.checkNotNull(windowPeriod, "RealtimePlumberSchool requires a windowPeriod.");
    Preconditions.checkNotNull(basePersistDirectory, "RealtimePlumberSchool requires a basePersistDirectory.");
    Preconditions.checkNotNull(segmentGranularity, "RealtimePlumberSchool requires a segmentGranularity.");
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
  public void setSegmentPusher(SegmentPusher segmentPusher)
  {
    this.segmentPusher = segmentPusher;
  }

  @JacksonInject("metadataUpdater")
  public void setMetadataUpdater(MetadataUpdater metadataUpdater)
  {
    this.metadataUpdater = metadataUpdater;
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
    initializeExecutors();

    computeBaseDir(schema).mkdirs();

    final Map<Long, Sink> sinks = Maps.newConcurrentMap();

    for (File sinkDir : computeBaseDir(schema).listFiles()) {
      Interval sinkInterval = new Interval(sinkDir.getName().replace("_", "/"));

      final File[] sinkFiles = sinkDir.listFiles();
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
          hydrants.add(
              new FireHydrant(
                  new QueryableIndexSegment(null, IndexIO.loadIndex(segmentDir)),
                  Integer.parseInt(segmentDir.getName())
              )
          );
        }

        Sink currSink = new Sink(sinkInterval, schema, hydrants);
        sinks.put(sinkInterval.getStartMillis(), currSink);

        metadataUpdater.announceSegment(currSink.getSegment());
      }
      catch (IOException e) {
        log.makeAlert(e, "Problem loading sink[%s] from disk.", schema.getDataSource())
           .addData("interval", sinkInterval)
           .emit();
      }
    }

    serverView.registerSegmentCallback(
        persistExecutor,
        new ServerView.BaseSegmentCallback()
        {
          @Override
          public ServerView.CallbackAction segmentAdded(DruidServer server, DataSegment segment)
          {
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
                  log.info("Segment matches sink[%s]", sink);

                  if (segment.getVersion().compareTo(sink.getSegment().getVersion()) >= 0) {
                    try {
                      metadataUpdater.unannounceSegment(sink.getSegment());
                      FileUtils.deleteDirectory(computePersistDir(schema, sink.getInterval()));
                      sinks.remove(sinkKey);
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

    final long truncatedNow = segmentGranularity.truncate(new DateTime()).getMillis();
    final long windowMillis = windowPeriod.toStandardDuration().getMillis();
    final RejectionPolicy rejectionPolicy = rejectionPolicyFactory.create(windowPeriod);
    log.info("Creating plumber using rejectionPolicy[%s]", rejectionPolicy);

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
            new ThreadRenamingRunnable(String.format("%s-overseer", schema.getDataSource()))
            {
              @Override
              public void doRun()
              {
                log.info("Starting merge and push.");

                long minTimestamp = segmentGranularity.truncate(rejectionPolicy.getCurrMaxTime()).getMillis() - windowMillis;

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

                          final File mergedFile;
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

                            DataSegment segment = segmentPusher.push(
                                mergedFile,
                                sink.getSegment().withDimensions(Lists.newArrayList(index.getAvailableDimensions()))
                            );

                            metadataUpdater.publishSegment(segment);
                          }
                          catch (IOException e) {
                            log.makeAlert(e, "Failed to persist merged index[%s]", schema.getDataSource())
                               .addData("interval", interval)
                               .emit();
                          }
                        }
                      }
                  );
                }
              }
            }
        );

    return new Plumber()
    {
      @Override
      public Sink getSink(long timestamp)
      {
        if (!rejectionPolicy.accept(timestamp)) {
          return null;
        }

        final long truncatedTime = segmentGranularity.truncate(timestamp);

        Sink retVal = sinks.get(truncatedTime);

        if (retVal == null) {
          retVal = new Sink(
              new Interval(new DateTime(truncatedTime), segmentGranularity.increment(new DateTime(truncatedTime))),
              schema
          );

          try {
            metadataUpdater.announceSegment(retVal.getSegment());

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
        final Function<Query<T>, ServiceMetricEvent.Builder> builderFn =
            new Function<Query<T>, ServiceMetricEvent.Builder>()
            {
              private final QueryToolChest<T,Query<T>> toolchest = factory.getToolchest();

              @Override
              public ServiceMetricEvent.Builder apply(@Nullable Query<T> input)
              {
                return toolchest.makeMetricBuilder(query);
              }
            };


        return factory.mergeRunners(
            EXEC,
            FunctionalIterable
                .create(sinks.values())
                .transform(
                    new Function<Sink, QueryRunner<T>>()
                    {
                      @Override
                      public QueryRunner<T> apply(@Nullable Sink input)
                      {
                        return new MetricsEmittingQueryRunner<T>(
                            emitter,
                            builderFn,
                            factory.mergeRunners(
                                EXEC,
                                Iterables.transform(
                                    input,
                                    new Function<FireHydrant, QueryRunner<T>>()
                                    {
                                      @Override
                                      public QueryRunner<T> apply(@Nullable FireHydrant input)
                                      {
                                        return factory.createRunner(input.getSegment());
                                      }
                                    }
                                )
                            )
                        );
                      }
                    }
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
        throw new UnsupportedOperationException();
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
    Preconditions.checkNotNull(segmentPusher, "must specify a segmentPusher to do this action.");
    Preconditions.checkNotNull(metadataUpdater, "must specify a metadataUpdater to do this action.");
    Preconditions.checkNotNull(serverView, "must specify a serverView to do this action.");
    Preconditions.checkNotNull(emitter, "must specify a serviceEmitter to do this action.");
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

  public interface RejectionPolicy
  {
    public DateTime getCurrMaxTime();
    public boolean accept(long timestamp);
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(value = {
      @JsonSubTypes.Type(name = "serverTime", value = ServerTimeRejectionPolicyFactory.class),
      @JsonSubTypes.Type(name = "messageTime", value = MessageTimeRejectionPolicyFactory.class)
  })
  public static interface RejectionPolicyFactory
  {
    public RejectionPolicy create(Period windowPeriod);
  }

  public static class ServerTimeRejectionPolicyFactory implements RejectionPolicyFactory
  {
    @Override
    public RejectionPolicy create(final Period windowPeriod)
    {
      final long windowMillis = windowPeriod.toStandardDuration().getMillis();

      return new RejectionPolicy()
      {
        @Override
        public DateTime getCurrMaxTime()
        {
          return new DateTime();
        }

        @Override
        public boolean accept(long timestamp)
        {
          return timestamp >= (System.currentTimeMillis() - windowMillis);
        }

        @Override
        public String toString()
        {
          return String.format("serverTime-%s", windowPeriod);
        }
      };
    }
  }

  public static class MessageTimeRejectionPolicyFactory implements RejectionPolicyFactory
  {
    @Override
    public RejectionPolicy create(final Period windowPeriod)
    {
      final long windowMillis = windowPeriod.toStandardDuration().getMillis();

      return new RejectionPolicy()
      {
        private volatile long maxTimestamp = Long.MIN_VALUE;

        @Override
        public DateTime getCurrMaxTime()
        {
          return new DateTime(maxTimestamp);
        }

        @Override
        public boolean accept(long timestamp)
        {
          maxTimestamp = Math.max(maxTimestamp, timestamp);

          return timestamp >= (maxTimestamp - windowMillis);
        }

        @Override
        public String toString()
        {
          return String.format("messageTime-%s", windowPeriod);
        }
      };
    }
  }
}
