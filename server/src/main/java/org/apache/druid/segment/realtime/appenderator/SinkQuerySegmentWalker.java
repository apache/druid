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

package org.apache.druid.segment.realtime.appenderator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.apache.druid.client.CachingQueryRunner;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulatorStats;
import org.apache.druid.client.cache.ForegroundCachePopulator;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.CloseQuietly;
import org.apache.druid.java.util.common.guava.FunctionalIterable;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.BySegmentQueryRunner;
import org.apache.druid.query.CPUTimeMetricQueryRunner;
import org.apache.druid.query.FinalizeResultsQueryRunner;
import org.apache.druid.query.MetricsEmittingQueryRunner;
import org.apache.druid.query.NoopQueryRunner;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QueryRunnerHelper;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.ReportTimelineMissingSegmentQueryRunner;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.SinkQueryRunners;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.query.spec.SpecificSegmentQueryRunner;
import org.apache.druid.query.spec.SpecificSegmentSpec;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.segment.join.Joinables;
import org.apache.druid.segment.realtime.FireHydrant;
import org.apache.druid.segment.realtime.plumber.Sink;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.apache.druid.timeline.partition.PartitionHolder;
import org.joda.time.Interval;

import java.io.Closeable;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Query handler for indexing tasks.
 */
public class SinkQuerySegmentWalker implements QuerySegmentWalker
{
  private static final EmittingLogger log = new EmittingLogger(SinkQuerySegmentWalker.class);
  private static final String CONTEXT_SKIP_INCREMENTAL_SEGMENT = "skipIncrementalSegment";

  private final String dataSource;

  private final VersionedIntervalTimeline<String, Sink> sinkTimeline;
  private final ObjectMapper objectMapper;
  private final ServiceEmitter emitter;
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final ExecutorService queryExecutorService;
  private final JoinableFactory joinableFactory;
  private final Cache cache;
  private final CacheConfig cacheConfig;
  private final CachePopulatorStats cachePopulatorStats;

  public SinkQuerySegmentWalker(
      String dataSource,
      VersionedIntervalTimeline<String, Sink> sinkTimeline,
      ObjectMapper objectMapper,
      ServiceEmitter emitter,
      QueryRunnerFactoryConglomerate conglomerate,
      ExecutorService queryExecutorService,
      JoinableFactory joinableFactory,
      Cache cache,
      CacheConfig cacheConfig,
      CachePopulatorStats cachePopulatorStats
  )
  {
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.sinkTimeline = Preconditions.checkNotNull(sinkTimeline, "sinkTimeline");
    this.objectMapper = Preconditions.checkNotNull(objectMapper, "objectMapper");
    this.emitter = Preconditions.checkNotNull(emitter, "emitter");
    this.conglomerate = Preconditions.checkNotNull(conglomerate, "conglomerate");
    this.queryExecutorService = Preconditions.checkNotNull(queryExecutorService, "queryExecutorService");
    this.joinableFactory = Preconditions.checkNotNull(joinableFactory, "joinableFactory");
    this.cache = Preconditions.checkNotNull(cache, "cache");
    this.cacheConfig = Preconditions.checkNotNull(cacheConfig, "cacheConfig");
    this.cachePopulatorStats = Preconditions.checkNotNull(cachePopulatorStats, "cachePopulatorStats");

    if (!cache.isLocal()) {
      log.warn("Configured cache[%s] is not local, caching will not be enabled.", cache.getClass().getName());
    }
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(final Query<T> query, final Iterable<Interval> intervals)
  {
    final Iterable<SegmentDescriptor> specs = FunctionalIterable
        .create(intervals)
        .transformCat(sinkTimeline::lookup)
        .transformCat(
            holder -> FunctionalIterable
                .create(holder.getObject())
                .transform(
                    chunk -> new SegmentDescriptor(
                        holder.getInterval(),
                        holder.getVersion(),
                        chunk.getChunkNumber()
                    )
                )
        );

    return getQueryRunnerForSegments(query, specs);
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(final Query<T> query, final Iterable<SegmentDescriptor> specs)
  {
    // We only handle one particular dataSource. Make sure that's what we have, then ignore from here on out.
    final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(query.getDataSource());
    final Optional<TableDataSource> baseTableDataSource = analysis.getBaseTableDataSource();

    if (!baseTableDataSource.isPresent() || !dataSource.equals(baseTableDataSource.get().getName())) {
      // Report error, since we somehow got a query for a datasource we can't handle.
      throw new ISE("Cannot handle datasource: %s", analysis.getDataSource());
    }

    final QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(query);
    if (factory == null) {
      throw new ISE("Unknown query type[%s].", query.getClass());
    }

    final QueryToolChest<T, Query<T>> toolChest = factory.getToolchest();
    final boolean skipIncrementalSegment = query.getContextValue(CONTEXT_SKIP_INCREMENTAL_SEGMENT, false);
    final AtomicLong cpuTimeAccumulator = new AtomicLong(0L);

    // Make sure this query type can handle the subquery, if present.
    if (analysis.isQuery() && !toolChest.canPerformSubquery(((QueryDataSource) analysis.getDataSource()).getQuery())) {
      throw new ISE("Cannot handle subquery: %s", analysis.getDataSource());
    }

    // segmentMapFn maps each base Segment into a joined Segment if necessary.
    final Function<SegmentReference, SegmentReference> segmentMapFn = Joinables.createSegmentMapFn(
        analysis.getPreJoinableClauses(),
        joinableFactory,
        cpuTimeAccumulator,
        analysis.getBaseQuery().orElse(query)
    );

    Iterable<QueryRunner<T>> perSegmentRunners = Iterables.transform(
        specs,
        descriptor -> {
          final PartitionHolder<Sink> holder = sinkTimeline.findEntry(
              descriptor.getInterval(),
              descriptor.getVersion()
          );
          if (holder == null) {
            return new ReportTimelineMissingSegmentQueryRunner<>(descriptor);
          }

          final PartitionChunk<Sink> chunk = holder.getChunk(descriptor.getPartitionNumber());
          if (chunk == null) {
            return new ReportTimelineMissingSegmentQueryRunner<>(descriptor);
          }

          final Sink theSink = chunk.getObject();
          final SegmentId sinkSegmentId = theSink.getSegment().getId();

          Iterable<QueryRunner<T>> perHydrantRunners = new SinkQueryRunners<>(
              Iterables.transform(
                  theSink,
                  hydrant -> {
                    // Hydrant might swap at any point, but if it's swapped at the start
                    // then we know it's *definitely* swapped.
                    final boolean hydrantDefinitelySwapped = hydrant.hasSwapped();

                    if (skipIncrementalSegment && !hydrantDefinitelySwapped) {
                      return new Pair<>(hydrant.getSegmentDataInterval(), new NoopQueryRunner<>());
                    }

                    // Prevent the underlying segment from swapping when its being iterated
                    final Optional<Pair<SegmentReference, Closeable>> maybeSegmentAndCloseable =
                        hydrant.getSegmentForQuery(segmentMapFn);

                    // if optional isn't present, we failed to acquire reference to the segment or any joinables
                    if (!maybeSegmentAndCloseable.isPresent()) {
                      return new Pair<>(
                          hydrant.getSegmentDataInterval(),
                          new ReportTimelineMissingSegmentQueryRunner<>(descriptor)
                      );
                    }
                    final Pair<SegmentReference, Closeable> segmentAndCloseable = maybeSegmentAndCloseable.get();
                    try {

                      QueryRunner<T> runner = factory.createRunner(segmentAndCloseable.lhs);

                      // 1) Only use caching if data is immutable
                      // 2) Hydrants are not the same between replicas, make sure cache is local
                      if (hydrantDefinitelySwapped && cache.isLocal()) {
                        runner = new CachingQueryRunner<>(
                            makeHydrantCacheIdentifier(hydrant),
                            descriptor,
                            objectMapper,
                            cache,
                            toolChest,
                            runner,
                            // Always populate in foreground regardless of config
                            new ForegroundCachePopulator(
                                objectMapper,
                                cachePopulatorStats,
                                cacheConfig.getMaxEntrySize()
                            ),
                            cacheConfig
                        );
                      }
                      // Make it always use Closeable to decrement()
                      runner = QueryRunnerHelper.makeClosingQueryRunner(
                          runner,
                          segmentAndCloseable.rhs
                      );
                      return new Pair<>(segmentAndCloseable.lhs.getDataInterval(), runner);
                    }
                    catch (RuntimeException e) {
                      CloseQuietly.close(segmentAndCloseable.rhs);
                      throw e;
                    }
                  }
              )
          );
          return new SpecificSegmentQueryRunner<>(
              withPerSinkMetrics(
                  new BySegmentQueryRunner<>(
                      sinkSegmentId,
                      descriptor.getInterval().getStart(),
                      factory.mergeRunners(
                          Execs.directExecutor(),
                          perHydrantRunners
                      )
                  ),
                  toolChest,
                  sinkSegmentId,
                  cpuTimeAccumulator
              ),
              new SpecificSegmentSpec(descriptor)
          );
        }
    );
    final QueryRunner<T> mergedRunner =
        toolChest.mergeResults(
            factory.mergeRunners(
                queryExecutorService,
                perSegmentRunners
            )
        );

    return CPUTimeMetricQueryRunner.safeBuild(
        new FinalizeResultsQueryRunner<>(mergedRunner, toolChest),
        toolChest,
        emitter,
        cpuTimeAccumulator,
        true
    );
  }

  /**
   * Decorates a Sink's query runner to emit query/segmentAndCache/time, query/segment/time, query/wait/time once
   * each for the whole Sink. Also adds CPU time to cpuTimeAccumulator.
   */
  private <T> QueryRunner<T> withPerSinkMetrics(
      final QueryRunner<T> sinkRunner,
      final QueryToolChest<T, ? extends Query<T>> queryToolChest,
      final SegmentId sinkSegmentId,
      final AtomicLong cpuTimeAccumulator
  )
  {
    // Note: reportSegmentAndCacheTime and reportSegmentTime are effectively the same here. They don't split apart
    // cache vs. non-cache due to the fact that Sinks may be partially cached and partially uncached. Making this
    // better would need to involve another accumulator like the cpuTimeAccumulator that we could share with the
    // sinkRunner.
    String sinkSegmentIdString = sinkSegmentId.toString();
    return CPUTimeMetricQueryRunner.safeBuild(
        new MetricsEmittingQueryRunner<>(
            emitter,
            queryToolChest,
            new MetricsEmittingQueryRunner<>(
                emitter,
                queryToolChest,
                sinkRunner,
                QueryMetrics::reportSegmentTime,
                queryMetrics -> queryMetrics.segment(sinkSegmentIdString)
            ),
            QueryMetrics::reportSegmentAndCacheTime,
            queryMetrics -> queryMetrics.segment(sinkSegmentIdString)
        ).withWaitMeasuredFromNow(),
        queryToolChest,
        emitter,
        cpuTimeAccumulator,
        false
    );
  }

  public VersionedIntervalTimeline<String, Sink> getSinkTimeline()
  {
    return sinkTimeline;
  }

  public static String makeHydrantCacheIdentifier(FireHydrant input)
  {
    return input.getSegmentId() + "_" + input.getCount();
  }
}
