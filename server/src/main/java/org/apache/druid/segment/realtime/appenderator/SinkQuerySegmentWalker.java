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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.client.CachingQueryRunner;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulatorStats;
import org.apache.druid.client.cache.ForegroundCachePopulator;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.guava.FunctionalIterable;
import org.apache.druid.java.util.common.guava.LazySequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.SequenceWrapper;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.BySegmentQueryRunner;
import org.apache.druid.query.CPUTimeMetricQueryRunner;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.DefaultQueryMetrics;
import org.apache.druid.query.DirectQueryProcessingPool;
import org.apache.druid.query.FinalizeResultsQueryRunner;
import org.apache.druid.query.NoopQueryRunner;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QueryRunnerHelper;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.ReportTimelineMissingSegmentQueryRunner;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.SinkQueryRunners;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.query.spec.SpecificSegmentQueryRunner;
import org.apache.druid.query.spec.SpecificSegmentSpec;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.TimeBoundaryInspector;
import org.apache.druid.segment.realtime.FireHydrant;
import org.apache.druid.segment.realtime.sink.Sink;
import org.apache.druid.segment.realtime.sink.SinkSegmentReference;
import org.apache.druid.server.ResourceIdPopulatingQueryRunner;
import org.apache.druid.timeline.Overshadowable;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.IntegerPartitionChunk;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.apache.druid.utils.CloseableUtils;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.ObjLongConsumer;
import java.util.stream.Collectors;

/**
 * Query handler for indexing tasks.
 */
public class SinkQuerySegmentWalker implements QuerySegmentWalker
{
  private static final EmittingLogger log = new EmittingLogger(SinkQuerySegmentWalker.class);
  private static final String CONTEXT_SKIP_INCREMENTAL_SEGMENT = "skipIncrementalSegment";

  private static final Set<String> SEGMENT_QUERY_METRIC = ImmutableSet.of(DefaultQueryMetrics.QUERY_SEGMENT_TIME);
  private static final Set<String> SEGMENT_CACHE_AND_WAIT_METRICS = ImmutableSet.of(
      DefaultQueryMetrics.QUERY_WAIT_TIME,
      DefaultQueryMetrics.QUERY_SEGMENT_AND_CACHE_TIME
  );

  private static final Map<String, ObjLongConsumer<? super QueryMetrics<?>>> METRICS_TO_REPORT =
      ImmutableMap.of(
          DefaultQueryMetrics.QUERY_SEGMENT_TIME, QueryMetrics::reportSegmentTime,
          DefaultQueryMetrics.QUERY_SEGMENT_AND_CACHE_TIME, QueryMetrics::reportSegmentAndCacheTime,
          DefaultQueryMetrics.QUERY_WAIT_TIME, QueryMetrics::reportWaitTime
      );

  private final String dataSource;

  // Maintain a timeline of ids and Sinks for all the segments including the base and upgraded versions
  private final VersionedIntervalTimeline<String, SinkHolder> upgradedSegmentsTimeline;
  private final ObjectMapper objectMapper;
  private final ServiceEmitter emitter;
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final QueryProcessingPool queryProcessingPool;
  private final Cache cache;
  private final CacheConfig cacheConfig;
  private final CachePopulatorStats cachePopulatorStats;

  public SinkQuerySegmentWalker(
      String dataSource,
      VersionedIntervalTimeline<String, SinkHolder> upgradedSegmentsTimeline,
      ObjectMapper objectMapper,
      ServiceEmitter emitter,
      QueryRunnerFactoryConglomerate conglomerate,
      QueryProcessingPool queryProcessingPool,
      Cache cache,
      CacheConfig cacheConfig,
      CachePopulatorStats cachePopulatorStats
  )
  {
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.upgradedSegmentsTimeline = upgradedSegmentsTimeline;
    this.objectMapper = Preconditions.checkNotNull(objectMapper, "objectMapper");
    this.emitter = Preconditions.checkNotNull(emitter, "emitter");
    this.conglomerate = Preconditions.checkNotNull(conglomerate, "conglomerate");
    this.queryProcessingPool = Preconditions.checkNotNull(queryProcessingPool, "queryProcessingPool");
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
        .transformCat(upgradedSegmentsTimeline::lookup)
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
    final DataSource dataSourceFromQuery = query.getDataSource();
    final DataSourceAnalysis analysis = dataSourceFromQuery.getAnalysis();

    // Sanity check: make sure the query is based on the table we're meant to handle.
    if (!analysis.getBaseTableDataSource().filter(ds -> dataSource.equals(ds.getName())).isPresent()) {
      throw new ISE("Cannot handle datasource: %s", dataSourceFromQuery);
    }

    final QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(query);
    if (factory == null) {
      throw new ISE("Unknown query type[%s].", query.getClass());
    }

    final QueryToolChest<T, Query<T>> toolChest = factory.getToolchest();
    final boolean skipIncrementalSegment = query.context().getBoolean(CONTEXT_SKIP_INCREMENTAL_SEGMENT, false);
    final AtomicLong cpuTimeAccumulator = new AtomicLong(0L);

    // Make sure this query type can handle the subquery, if present.
    if ((dataSourceFromQuery instanceof QueryDataSource)
        && !toolChest.canPerformSubquery(((QueryDataSource) dataSourceFromQuery).getQuery())) {
      throw new ISE("Cannot handle subquery: %s", dataSourceFromQuery);
    }

    // segmentMapFn maps each base Segment into a joined Segment if necessary.
    final Function<SegmentReference, SegmentReference> segmentMapFn =
        dataSourceFromQuery.createSegmentMapFunction(
            query,
            cpuTimeAccumulator
        );

    // We compute the join cache key here itself so it doesn't need to be re-computed for every segment
    final Optional<byte[]> cacheKeyPrefix = Optional.ofNullable(query.getDataSource().getCacheKey());

    // We need to report data for each Sink all-or-nothing, which means we need to acquire references for all
    // subsegments (FireHydrants) of a segment (Sink) at once. To ensure they are properly released even when a
    // query fails or is canceled, we acquire *all* sink reference upfront, and release them all when the main
    // QueryRunner returned by this method is closed. (We can't do the acquisition and releasing at the level of
    // each FireHydrant's runner, since then it wouldn't be properly all-or-nothing on a per-Sink basis.)
    final List<SinkSegmentReference> allSegmentReferences = new ArrayList<>();
    final Map<SegmentDescriptor, SegmentId> segmentIdMap = new HashMap<>();
    final LinkedHashMap<SegmentDescriptor, List<QueryRunner<T>>> allRunners = new LinkedHashMap<>();
    final ConcurrentHashMap<String, SinkMetricsEmittingQueryRunner.SegmentMetrics> segmentMetricsAccumulator = new ConcurrentHashMap<>();

    try {
      for (final SegmentDescriptor descriptor : specs) {
        final PartitionChunk<SinkHolder> chunk = upgradedSegmentsTimeline.findChunk(
            descriptor.getInterval(),
            descriptor.getVersion(),
            descriptor.getPartitionNumber()
        );

        if (chunk == null) {
          allRunners.put(
              descriptor,
              Collections.singletonList(new ReportTimelineMissingSegmentQueryRunner<>(descriptor))
          );
          continue;
        }

        final Sink theSink = chunk.getObject().sink;
        final SegmentId sinkSegmentId = theSink.getSegment().getId();
        segmentIdMap.put(descriptor, sinkSegmentId);
        final List<SinkSegmentReference> sinkSegmentReferences =
            theSink.acquireSegmentReferences(segmentMapFn, skipIncrementalSegment);

        if (sinkSegmentReferences == null) {
          // We failed to acquire references for all subsegments. Bail and report the entire sink missing.
          allRunners.put(
              descriptor,
              Collections.singletonList(new ReportTimelineMissingSegmentQueryRunner<>(descriptor))
          );
        } else if (sinkSegmentReferences.isEmpty()) {
          allRunners.put(descriptor, Collections.singletonList(new NoopQueryRunner<>()));
        } else {
          allSegmentReferences.addAll(sinkSegmentReferences);

          allRunners.put(
              descriptor,
              sinkSegmentReferences.stream().map(
                  segmentReference -> {
                    QueryRunner<T> runner = new SinkMetricsEmittingQueryRunner<>(
                        emitter,
                        factory.getToolchest(),
                        factory.createRunner(segmentReference.getSegment()),
                        segmentMetricsAccumulator,
                        SEGMENT_QUERY_METRIC,
                        sinkSegmentId.toString()
                    );

                    // 1) Only use caching if data is immutable
                    // 2) Hydrants are not the same between replicas, make sure cache is local
                    if (segmentReference.isImmutable() && cache.isLocal()) {
                      final SegmentReference segment = segmentReference.getSegment();
                      final TimeBoundaryInspector timeBoundaryInspector = segment.as(TimeBoundaryInspector.class);
                      final Interval cacheKeyInterval;

                      if (timeBoundaryInspector != null) {
                        cacheKeyInterval = timeBoundaryInspector.getMinMaxInterval();
                      } else {
                        cacheKeyInterval = segment.getDataInterval();
                      }

                      runner = new CachingQueryRunner<>(
                          makeHydrantCacheIdentifier(sinkSegmentId, segmentReference.getHydrantNumber()),
                          cacheKeyPrefix,
                          descriptor,
                          cacheKeyInterval,
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

                    // Regardless of whether caching is enabled, do reportSegmentAndCacheTime outside the
                    // *possible* caching.
                    runner = new SinkMetricsEmittingQueryRunner<>(
                        emitter,
                        factory.getToolchest(),
                        runner,
                        segmentMetricsAccumulator,
                        SEGMENT_CACHE_AND_WAIT_METRICS,
                        sinkSegmentId.toString()
                    );

                    // Emit CPU time metrics.
                    runner = CPUTimeMetricQueryRunner.safeBuild(
                        runner,
                        toolChest,
                        emitter,
                        cpuTimeAccumulator,
                        false
                    );

                    // Run with specific segment descriptor.
                    runner = new SpecificSegmentQueryRunner<>(
                        runner,
                        new SpecificSegmentSpec(descriptor)
                    );

                    return runner;
                  }
              ).collect(Collectors.toList())
          );
        }
      }

      final QueryRunner<T> mergedRunner;

      if (query.context().isBySegment()) {
        // bySegment: merge all hydrants for a Sink first, then merge Sinks. Necessary to keep results for the
        // same segment together, but causes additional memory usage due to the extra layer of materialization,
        // so we only do this if we need to.
        mergedRunner = factory.mergeRunners(
            queryProcessingPool,
            allRunners.entrySet().stream().map(
                entry -> new BySegmentQueryRunner<>(
                    segmentIdMap.get(entry.getKey()),
                    entry.getKey().getInterval().getStart(),
                    factory.mergeRunners(
                        DirectQueryProcessingPool.INSTANCE,
                        entry.getValue()
                    )
                )
            ).collect(Collectors.toList())
        );
      } else {
        // Not bySegment: merge all hydrants at the same level, rather than grouped by Sink (segment).
        mergedRunner = factory.mergeRunners(
            queryProcessingPool,
            new SinkQueryRunners<>(
                allRunners.entrySet().stream().flatMap(
                    entry ->
                        entry.getValue().stream().map(
                            runner ->
                                Pair.of(entry.getKey().getInterval(), runner)
                        )
                ).collect(Collectors.toList()))
        );
      }

      // 1) Populate resource id to the query
      // 2) Merge results using the toolChest, finalize if necessary.
      // 3) Measure CPU time of that operation.
      // 4) Release all sink segment references.
      return new ResourceIdPopulatingQueryRunner<>(
          QueryRunnerHelper.makeClosingQueryRunner(
              CPUTimeMetricQueryRunner.safeBuild(
                  new SinkMetricsEmittingQueryRunner<>(
                      emitter,
                      toolChest,
                      new FinalizeResultsQueryRunner<>(
                          toolChest.mergeResults(mergedRunner, true),
                          toolChest
                      ),
                      segmentMetricsAccumulator,
                      Collections.emptySet(),
                      null
                  ),
                  toolChest,
                  emitter,
                  cpuTimeAccumulator,
                  true
              ),
              () -> CloseableUtils.closeAll(allSegmentReferences)
          )
      );
    }
    catch (Throwable e) {
      throw CloseableUtils.closeAndWrapInCatch(e, () -> CloseableUtils.closeAll(allSegmentReferences));
    }
  }

  /**
   * Must be called when a segment is announced by a task.
   * Either the base segment upon allocation or any upgraded version due to a concurrent replace
   */
  public void registerUpgradedPendingSegment(SegmentIdWithShardSpec id, Sink sink)
  {
    final SegmentDescriptor upgradedDescriptor = id.asSegmentId().toDescriptor();
    upgradedSegmentsTimeline.add(
        upgradedDescriptor.getInterval(),
        upgradedDescriptor.getVersion(),
        IntegerPartitionChunk.make(
            null,
            null,
            upgradedDescriptor.getPartitionNumber(),
            new SinkHolder(upgradedDescriptor, sink)
        )
    );
  }

  /**
   * Must be called when dropping sink from the sinkTimeline
   * It is the responsibility of the caller to unregister all associated ids including the base id
   */
  public void unregisterUpgradedPendingSegment(SegmentIdWithShardSpec id, Sink sink)
  {
    final SegmentDescriptor upgradedDescriptor = id.asSegmentId().toDescriptor();
    upgradedSegmentsTimeline.remove(
        upgradedDescriptor.getInterval(),
        upgradedDescriptor.getVersion(),
        IntegerPartitionChunk.make(
            null,
            null,
            upgradedDescriptor.getPartitionNumber(),
            new SinkHolder(upgradedDescriptor, sink)
        )
    );
  }

  @VisibleForTesting
  String getDataSource()
  {
    return dataSource;
  }

  public static String makeHydrantCacheIdentifier(final FireHydrant hydrant)
  {
    return makeHydrantCacheIdentifier(hydrant.getSegmentId(), hydrant.getCount());
  }

  public static String makeHydrantCacheIdentifier(final SegmentId segmentId, final int hydrantNumber)
  {
    // Cache ID like segmentId_H0, etc. The 'H' disambiguates subsegment [foo_x_y_z partition 0 hydrant 1]
    // from full segment [foo_x_y_z partition 1], and is therefore useful if we ever want the cache to mix full segments
    // with subsegments (hydrants).
    return segmentId + "_H" + hydrantNumber;
  }
  
  /**
   * This class is responsible for emitting query/segment/time, query/wait/time and query/segmentAndCache/Time metrics for a Sink.
   * It accumulates query/segment/time and query/segmentAndCache/time metric for each FireHydrant at the level of Sink.
   * query/wait/time metric is the time taken to process the first FireHydrant for the Sink.
   * <p>
   * This class operates in two distinct modes based on whether {@link SinkMetricsEmittingQueryRunner#segmentId} is null or non-null.
   * When segmentId is non-null, it accumulates the metrics. When segmentId is null, it emits the accumulated metrics.
   * <p>
   * This class is derived from {@link org.apache.druid.query.MetricsEmittingQueryRunner}.
   */
  private static class SinkMetricsEmittingQueryRunner<T> implements QueryRunner<T>
  {
    private final ServiceEmitter emitter;
    private final QueryToolChest<T, ? extends Query<T>> queryToolChest;
    private final QueryRunner<T> queryRunner;
    private final ConcurrentHashMap<String, SegmentMetrics> segmentMetricsAccumulator;
    private final Set<String> metricsToCompute;
    @Nullable
    private final String segmentId;
    private final long creationTimeNs;

    private SinkMetricsEmittingQueryRunner(
        ServiceEmitter emitter,
        QueryToolChest<T, ? extends Query<T>> queryToolChest,
        QueryRunner<T> queryRunner,
        ConcurrentHashMap<String, SegmentMetrics> segmentMetricsAccumulator,
        Set<String> metricsToCompute,
        @Nullable String segmentId
    )
    {
      this.emitter = emitter;
      this.queryToolChest = queryToolChest;
      this.queryRunner = queryRunner;
      this.segmentMetricsAccumulator = segmentMetricsAccumulator;
      this.metricsToCompute = metricsToCompute;
      this.segmentId = segmentId;
      this.creationTimeNs = System.nanoTime();
    }

    @Override
    public Sequence<T> run(final QueryPlus<T> queryPlus, final ResponseContext responseContext)
    {
      QueryPlus<T> queryWithMetrics = queryPlus.withQueryMetrics(queryToolChest);
      return Sequences.wrap(
          // Use LazySequence because we want to account execution time of queryRunner.run() (it prepares the underlying
          // Sequence) as part of the reported query time, i.e. we want to execute queryRunner.run() after
          // `startTimeNs = System.nanoTime();`
          new LazySequence<>(() -> queryRunner.run(queryWithMetrics, responseContext)),
          new SequenceWrapper()
          {
            private long startTimeNs;

            @Override
            public void before()
            {
              startTimeNs = System.nanoTime();
            }

            @Override
            public void after(boolean isDone, Throwable thrown)
            {
              if (segmentId != null) {
                // accumulate metrics
                final SegmentMetrics metrics = segmentMetricsAccumulator.computeIfAbsent(segmentId, id -> new SegmentMetrics());
                if (metricsToCompute.contains(DefaultQueryMetrics.QUERY_WAIT_TIME)) {
                  metrics.setWaitTime(startTimeNs - creationTimeNs);
                }
                if (metricsToCompute.contains(DefaultQueryMetrics.QUERY_SEGMENT_TIME)) {
                  metrics.addSegmentTime(System.nanoTime() - startTimeNs);
                }
                if (metricsToCompute.contains(DefaultQueryMetrics.QUERY_SEGMENT_AND_CACHE_TIME)) {
                  metrics.addSegmentAndCacheTime(System.nanoTime() - startTimeNs);
                }
              } else {
                final QueryMetrics<?> queryMetrics = queryWithMetrics.getQueryMetrics();
                // report accumulated metrics
                for (Map.Entry<String, SegmentMetrics> segmentAndMetrics : segmentMetricsAccumulator.entrySet()) {
                  queryMetrics.segment(segmentAndMetrics.getKey());

                  for (Map.Entry<String, ObjLongConsumer<? super QueryMetrics<?>>> reportMetric : METRICS_TO_REPORT.entrySet()) {
                    final String metricName = reportMetric.getKey();
                    switch (metricName) {
                      case DefaultQueryMetrics.QUERY_SEGMENT_TIME:
                        reportMetric.getValue().accept(queryMetrics, segmentAndMetrics.getValue().getSegmentTime());
                      case DefaultQueryMetrics.QUERY_WAIT_TIME:
                        reportMetric.getValue().accept(queryMetrics, segmentAndMetrics.getValue().getWaitTime());
                      case DefaultQueryMetrics.QUERY_SEGMENT_AND_CACHE_TIME:
                        reportMetric.getValue().accept(queryMetrics, segmentAndMetrics.getValue().getSegmentAndCacheTime());
                    }
                  }

                  try {
                    queryMetrics.emit(emitter);
                  }
                  catch (Exception e) {
                    // Query should not fail, because of emitter failure. Swallowing the exception.
                    log.error(e, "Failed to emit metrics for segment[%s]", segmentAndMetrics.getKey());
                  }
                }
              }
            }
          }
      );
    }

    /**
     * Class to track segment related metrics during query execution.
     */
    private static class SegmentMetrics
    {
      private final AtomicLong querySegmentTime = new AtomicLong(0);
      private final AtomicLong queryWaitTime = new AtomicLong(0);
      private final AtomicLong querySegmentAndCacheTime = new AtomicLong(0);

      private void addSegmentTime(long time)
      {
        querySegmentTime.addAndGet(time);
      }

      private void setWaitTime(long time)
      {
        queryWaitTime.set(time);
      }

      private void addSegmentAndCacheTime(long time)
      {
        querySegmentAndCacheTime.addAndGet(time);
      }

      private long getSegmentTime()
      {
        return querySegmentTime.get();
      }

      private long getWaitTime()
      {
        return queryWaitTime.get();
      }

      private long getSegmentAndCacheTime()
      {
        return querySegmentAndCacheTime.get();
      }
    }
  }
  
  private static class SinkHolder implements Overshadowable<SinkHolder>
  {
    private final Sink sink;
    private final SegmentDescriptor segmentDescriptor;

    private SinkHolder(SegmentDescriptor segmentDescriptor, Sink sink)
    {
      this.segmentDescriptor = segmentDescriptor;
      this.sink = sink;
    }

    @Override
    public int getStartRootPartitionId()
    {
      return segmentDescriptor.getPartitionNumber();
    }

    @Override
    public int getEndRootPartitionId()
    {
      return segmentDescriptor.getPartitionNumber() + 1;
    }

    @Override
    public String getVersion()
    {
      return segmentDescriptor.getVersion();
    }

    @Override
    public short getMinorVersion()
    {
      return 0;
    }

    @Override
    public short getAtomicUpdateGroupSize()
    {
      return 1;
    }
  }
}
