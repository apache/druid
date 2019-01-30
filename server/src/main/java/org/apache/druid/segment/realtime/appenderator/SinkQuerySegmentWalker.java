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
import com.google.common.base.Function;
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
import org.apache.druid.query.MetricsEmittingQueryRunner;
import org.apache.druid.query.NoopQueryRunner;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QueryRunnerHelper;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.ReportTimelineMissingSegmentQueryRunner;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.spec.SpecificSegmentQueryRunner;
import org.apache.druid.query.spec.SpecificSegmentSpec;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.realtime.FireHydrant;
import org.apache.druid.segment.realtime.plumber.Sink;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.apache.druid.timeline.partition.PartitionHolder;
import org.joda.time.Interval;

import java.io.Closeable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

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
        .transformCat(
            new Function<Interval, Iterable<TimelineObjectHolder<String, Sink>>>()
            {
              @Override
              public Iterable<TimelineObjectHolder<String, Sink>> apply(final Interval interval)
              {
                return sinkTimeline.lookup(interval);
              }
            }
        )
        .transformCat(
            new Function<TimelineObjectHolder<String, Sink>, Iterable<SegmentDescriptor>>()
            {
              @Override
              public Iterable<SegmentDescriptor> apply(final TimelineObjectHolder<String, Sink> holder)
              {
                return FunctionalIterable
                    .create(holder.getObject())
                    .transform(
                        new Function<PartitionChunk<Sink>, SegmentDescriptor>()
                        {
                          @Override
                          public SegmentDescriptor apply(final PartitionChunk<Sink> chunk)
                          {
                            return new SegmentDescriptor(
                                holder.getInterval(),
                                holder.getVersion(),
                                chunk.getChunkNumber()
                            );
                          }
                        }
                    );
              }
            }
        );

    return getQueryRunnerForSegments(query, specs);
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(final Query<T> query, final Iterable<SegmentDescriptor> specs)
  {
    // We only handle one particular dataSource. Make sure that's what we have, then ignore from here on out.
    if (!(query.getDataSource() instanceof TableDataSource)
        || !dataSource.equals(((TableDataSource) query.getDataSource()).getName())) {
      log.makeAlert("Received query for unknown dataSource")
         .addData("dataSource", query.getDataSource())
         .emit();
      return new NoopQueryRunner<>();
    }

    final QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(query);
    if (factory == null) {
      throw new ISE("Unknown query type[%s].", query.getClass());
    }

    final QueryToolChest<T, Query<T>> toolChest = factory.getToolchest();
    final boolean skipIncrementalSegment = query.getContextValue(CONTEXT_SKIP_INCREMENTAL_SEGMENT, false);
    final AtomicLong cpuTimeAccumulator = new AtomicLong(0L);

    return CPUTimeMetricQueryRunner.safeBuild(
        toolChest.mergeResults(
            factory.mergeRunners(
                queryExecutorService,
                FunctionalIterable
                    .create(specs)
                    .transform(
                        new Function<SegmentDescriptor, QueryRunner<T>>()
                        {
                          @Override
                          public QueryRunner<T> apply(final SegmentDescriptor descriptor)
                          {
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

                            return new SpecificSegmentQueryRunner<>(
                                withPerSinkMetrics(
                                    new BySegmentQueryRunner<>(
                                        sinkSegmentId,
                                        descriptor.getInterval().getStart(),
                                        factory.mergeRunners(
                                            Execs.directExecutor(),
                                            Iterables.transform(
                                                theSink,
                                                new Function<FireHydrant, QueryRunner<T>>()
                                                {
                                                  @Override
                                                  public QueryRunner<T> apply(final FireHydrant hydrant)
                                                  {
                                                    // Hydrant might swap at any point, but if it's swapped at the start
                                                    // then we know it's *definitely* swapped.
                                                    final boolean hydrantDefinitelySwapped = hydrant.hasSwapped();

                                                    if (skipIncrementalSegment && !hydrantDefinitelySwapped) {
                                                      return new NoopQueryRunner<>();
                                                    }

                                                    // Prevent the underlying segment from swapping when its being iterated
                                                    final Pair<Segment, Closeable> segment = hydrant.getAndIncrementSegment();
                                                    try {
                                                      QueryRunner<T> baseRunner = QueryRunnerHelper.makeClosingQueryRunner(
                                                          factory.createRunner(segment.lhs),
                                                          segment.rhs
                                                      );

                                                      // 1) Only use caching if data is immutable
                                                      // 2) Hydrants are not the same between replicas, make sure cache is local
                                                      if (hydrantDefinitelySwapped && cache.isLocal()) {
                                                        return new CachingQueryRunner<>(
                                                            makeHydrantCacheIdentifier(hydrant),
                                                            descriptor,
                                                            objectMapper,
                                                            cache,
                                                            toolChest,
                                                            baseRunner,
                                                            // Always populate in foreground regardless of config
                                                            new ForegroundCachePopulator(
                                                                objectMapper,
                                                                cachePopulatorStats,
                                                                cacheConfig.getMaxEntrySize()
                                                            ),
                                                            cacheConfig
                                                        );
                                                      } else {
                                                        return baseRunner;
                                                      }
                                                    }
                                                    catch (RuntimeException e) {
                                                      CloseQuietly.close(segment.rhs);
                                                      throw e;
                                                    }
                                                  }
                                                }
                                            )
                                        )
                                    ),
                                    toolChest,
                                    sinkSegmentId,
                                    cpuTimeAccumulator
                                ),
                                new SpecificSegmentSpec(descriptor)
                            );
                          }
                        }
                    )
            )
        ),
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

  public static String makeHydrantCacheIdentifier(FireHydrant input)
  {
    return input.getSegmentId() + "_" + input.getCount();
  }
}
