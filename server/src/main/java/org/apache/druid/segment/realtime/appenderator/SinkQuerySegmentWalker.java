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
import org.apache.druid.client.CachingQueryRunner;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulatorStats;
import org.apache.druid.client.cache.ForegroundCachePopulator;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.guava.FunctionalIterable;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.BySegmentQueryRunner;
import org.apache.druid.query.CPUTimeMetricQueryRunner;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.DirectQueryProcessingPool;
import org.apache.druid.query.FinalizeResultsQueryRunner;
import org.apache.druid.query.MetricsEmittingQueryRunner;
import org.apache.druid.query.NoopQueryRunner;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryMetrics;
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
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.query.spec.SpecificSegmentQueryRunner;
import org.apache.druid.query.spec.SpecificSegmentSpec;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.realtime.FireHydrant;
import org.apache.druid.segment.realtime.plumber.Sink;
import org.apache.druid.segment.realtime.plumber.SinkSegmentReference;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.apache.druid.utils.CloseableUtils;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

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
  private final QueryProcessingPool queryProcessingPool;
  private final Cache cache;
  private final CacheConfig cacheConfig;
  private final CachePopulatorStats cachePopulatorStats;
  private final ConcurrentMap<SegmentDescriptor, SegmentDescriptor> newIdToBasePendingSegment
      = new ConcurrentHashMap<>();

  public SinkQuerySegmentWalker(
      String dataSource,
      VersionedIntervalTimeline<String, Sink> sinkTimeline,
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
    this.sinkTimeline = Preconditions.checkNotNull(sinkTimeline, "sinkTimeline");
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

    try {
      for (final SegmentDescriptor newDescriptor : specs) {
        final SegmentDescriptor descriptor = newIdToBasePendingSegment.getOrDefault(newDescriptor, newDescriptor);
        final PartitionChunk<Sink> chunk = sinkTimeline.findChunk(
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

        final Sink theSink = chunk.getObject();
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
                    QueryRunner<T> runner = new MetricsEmittingQueryRunner<>(
                        emitter,
                        factory.getToolchest(),
                        factory.createRunner(segmentReference.getSegment()),
                        QueryMetrics::reportSegmentTime,
                        queryMetrics -> queryMetrics.segment(sinkSegmentId.toString())
                    );

                    // 1) Only use caching if data is immutable
                    // 2) Hydrants are not the same between replicas, make sure cache is local
                    if (segmentReference.isImmutable() && cache.isLocal()) {
                      StorageAdapter storageAdapter = segmentReference.getSegment().asStorageAdapter();
                      long segmentMinTime = storageAdapter.getMinTime().getMillis();
                      long segmentMaxTime = storageAdapter.getMaxTime().getMillis();
                      Interval actualDataInterval = Intervals.utc(segmentMinTime, segmentMaxTime + 1);
                      runner = new CachingQueryRunner<>(
                          makeHydrantCacheIdentifier(sinkSegmentId, segmentReference.getHydrantNumber()),
                          cacheKeyPrefix,
                          descriptor,
                          actualDataInterval,
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
                    runner = new MetricsEmittingQueryRunner<>(
                        emitter,
                        factory.getToolchest(),
                        runner,
                        QueryMetrics::reportSegmentAndCacheTime,
                        queryMetrics -> queryMetrics.segment(sinkSegmentId.toString())
                    ).withWaitMeasuredFromNow();

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

      // 1) Merge results using the toolChest, finalize if necessary.
      // 2) Measure CPU time of that operation.
      // 3) Release all sink segment references.
      return QueryRunnerHelper.makeClosingQueryRunner(
          CPUTimeMetricQueryRunner.safeBuild(
              new FinalizeResultsQueryRunner<>(toolChest.mergeResults(mergedRunner), toolChest),
              toolChest,
              emitter,
              cpuTimeAccumulator,
              true
          ),
          () -> CloseableUtils.closeAll(allSegmentReferences)
      );
    }
    catch (Throwable e) {
      throw CloseableUtils.closeAndWrapInCatch(e, () -> CloseableUtils.closeAll(allSegmentReferences));
    }
  }

  public void registerNewVersionOfPendingSegment(
      SegmentIdWithShardSpec basePendingSegment,
      SegmentIdWithShardSpec newSegmentVersion
  )
  {
    newIdToBasePendingSegment.put(
        newSegmentVersion.asSegmentId().toDescriptor(),
        basePendingSegment.asSegmentId().toDescriptor()
    );
  }

  @VisibleForTesting
  String getDataSource()
  {
    return dataSource;
  }

  public VersionedIntervalTimeline<String, Sink> getSinkTimeline()
  {
    return sinkTimeline;
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
}
