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

package org.apache.druid.server.coordination;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import org.apache.druid.client.CachingQueryRunner;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulator;
import org.apache.druid.guice.annotations.Processing;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.FunctionalIterable;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.BySegmentQueryRunner;
import org.apache.druid.query.CPUTimeMetricQueryRunner;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.FinalizeResultsQueryRunner;
import org.apache.druid.query.MetricsEmittingQueryRunner;
import org.apache.druid.query.NoopQueryRunner;
import org.apache.druid.query.PerSegmentOptimizingQueryRunner;
import org.apache.druid.query.PerSegmentQueryOptimizationContext;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.ReferenceCountingSegmentQueryRunner;
import org.apache.druid.query.ReportTimelineMissingSegmentQueryRunner;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.spec.SpecificSegmentQueryRunner;
import org.apache.druid.query.spec.SpecificSegmentSpec;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.SetAndVerifyContextQueryRunner;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.apache.druid.timeline.partition.PartitionHolder;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class ServerManager implements QuerySegmentWalker
{
  private static final EmittingLogger log = new EmittingLogger(ServerManager.class);
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final ServiceEmitter emitter;
  private final ExecutorService exec;
  private final CachePopulator cachePopulator;
  private final Cache cache;
  private final ObjectMapper objectMapper;
  private final CacheConfig cacheConfig;
  private final SegmentManager segmentManager;
  private final ServerConfig serverConfig;

  @Inject
  public ServerManager(
      QueryRunnerFactoryConglomerate conglomerate,
      ServiceEmitter emitter,
      @Processing ExecutorService exec,
      CachePopulator cachePopulator,
      @Smile ObjectMapper objectMapper,
      Cache cache,
      CacheConfig cacheConfig,
      SegmentManager segmentManager,
      ServerConfig serverConfig
  )
  {
    this.conglomerate = conglomerate;
    this.emitter = emitter;

    this.exec = exec;
    this.cachePopulator = cachePopulator;
    this.cache = cache;
    this.objectMapper = objectMapper;

    this.cacheConfig = cacheConfig;
    this.segmentManager = segmentManager;
    this.serverConfig = serverConfig;
  }

  private DataSource getInnerMostDataSource(DataSource dataSource)
  {
    if (dataSource instanceof QueryDataSource) {
      return getInnerMostDataSource(((QueryDataSource) dataSource).getQuery().getDataSource());
    }
    return dataSource;
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals)
  {
    final QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(query);
    if (factory == null) {
      throw new ISE("Unknown query type[%s].", query.getClass());
    }

    final QueryToolChest<T, Query<T>> toolChest = factory.getToolchest();
    final AtomicLong cpuTimeAccumulator = new AtomicLong(0L);

    DataSource dataSource = getInnerMostDataSource(query.getDataSource());
    if (!(dataSource instanceof TableDataSource)) {
      throw new UnsupportedOperationException("data source type '" + dataSource.getClass().getName() + "' unsupported");
    }
    String dataSourceName = getDataSourceName(dataSource);

    final VersionedIntervalTimeline<String, ReferenceCountingSegment> timeline = segmentManager.getTimeline(
        dataSourceName
    );

    if (timeline == null) {
      return new NoopQueryRunner<T>();
    }

    FunctionalIterable<QueryRunner<T>> queryRunners = FunctionalIterable
        .create(intervals)
        .transformCat(
            new Function<Interval, Iterable<TimelineObjectHolder<String, ReferenceCountingSegment>>>()
            {
              @Override
              public Iterable<TimelineObjectHolder<String, ReferenceCountingSegment>> apply(Interval input)
              {
                return timeline.lookup(input);
              }
            }
        )
        .transformCat(
            new Function<TimelineObjectHolder<String, ReferenceCountingSegment>, Iterable<QueryRunner<T>>>()
            {
              @Override
              public Iterable<QueryRunner<T>> apply(
                  @Nullable
                  final TimelineObjectHolder<String, ReferenceCountingSegment> holder
              )
              {
                if (holder == null) {
                  return null;
                }

                return FunctionalIterable
                    .create(holder.getObject())
                    .transform(
                        new Function<PartitionChunk<ReferenceCountingSegment>, QueryRunner<T>>()
                        {
                          @Override
                          public QueryRunner<T> apply(PartitionChunk<ReferenceCountingSegment> input)
                          {
                            return buildAndDecorateQueryRunner(
                                factory,
                                toolChest,
                                input.getObject(),
                                new SegmentDescriptor(
                                    holder.getInterval(),
                                    holder.getVersion(),
                                    input.getChunkNumber()
                                ),
                                cpuTimeAccumulator
                            );
                          }
                        }
                    );
              }
            }
        );

    return CPUTimeMetricQueryRunner.safeBuild(
        new FinalizeResultsQueryRunner<>(
            toolChest.mergeResults(factory.mergeRunners(exec, queryRunners)),
            toolChest
        ),
        toolChest,
        emitter,
        cpuTimeAccumulator,
        true
    );
  }

  private String getDataSourceName(DataSource dataSource)
  {
    return Iterables.getOnlyElement(dataSource.getNames());
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs)
  {
    final QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(query);
    if (factory == null) {
      log.makeAlert("Unknown query type, [%s]", query.getClass())
         .addData("dataSource", query.getDataSource())
         .emit();
      return new NoopQueryRunner<T>();
    }

    final QueryToolChest<T, Query<T>> toolChest = factory.getToolchest();

    String dataSourceName = getDataSourceName(query.getDataSource());

    final VersionedIntervalTimeline<String, ReferenceCountingSegment> timeline = segmentManager.getTimeline(
        dataSourceName
    );

    if (timeline == null) {
      return new NoopQueryRunner<T>();
    }

    final AtomicLong cpuTimeAccumulator = new AtomicLong(0L);

    FunctionalIterable<QueryRunner<T>> queryRunners = FunctionalIterable
        .create(specs)
        .transformCat(
            new Function<SegmentDescriptor, Iterable<QueryRunner<T>>>()
            {
              @Override
              @SuppressWarnings("unchecked")
              public Iterable<QueryRunner<T>> apply(SegmentDescriptor input)
              {

                final PartitionHolder<ReferenceCountingSegment> entry = timeline.findEntry(
                    input.getInterval(), input.getVersion()
                );

                if (entry == null) {
                  return Collections.singletonList(
                      new ReportTimelineMissingSegmentQueryRunner<T>(input));
                }

                final PartitionChunk<ReferenceCountingSegment> chunk = entry.getChunk(input.getPartitionNumber());
                if (chunk == null) {
                  return Collections.singletonList(new ReportTimelineMissingSegmentQueryRunner<T>(input));
                }

                final ReferenceCountingSegment adapter = chunk.getObject();
                return Collections.singletonList(
                    buildAndDecorateQueryRunner(factory, toolChest, adapter, input, cpuTimeAccumulator)
                );
              }
            }
        );

    return CPUTimeMetricQueryRunner.safeBuild(
        new FinalizeResultsQueryRunner<>(
            toolChest.mergeResults(factory.mergeRunners(exec, queryRunners)),
            toolChest
        ),
        toolChest,
        emitter,
        cpuTimeAccumulator,
        true
    );
  }

  private <T> QueryRunner<T> buildAndDecorateQueryRunner(
      final QueryRunnerFactory<T, Query<T>> factory,
      final QueryToolChest<T, Query<T>> toolChest,
      final ReferenceCountingSegment adapter,
      final SegmentDescriptor segmentDescriptor,
      final AtomicLong cpuTimeAccumulator
  )
  {
    SpecificSegmentSpec segmentSpec = new SpecificSegmentSpec(segmentDescriptor);
    SegmentId segmentId = adapter.getId();
    String segmentIdString = segmentId.toString();

    MetricsEmittingQueryRunner<T> metricsEmittingQueryRunnerInner = new MetricsEmittingQueryRunner<>(
        emitter,
        toolChest,
        new ReferenceCountingSegmentQueryRunner<>(factory, adapter, segmentDescriptor),
        QueryMetrics::reportSegmentTime,
        queryMetrics -> queryMetrics.segment(segmentIdString)
    );

    CachingQueryRunner<T> cachingQueryRunner = new CachingQueryRunner<>(
        segmentIdString,
        segmentDescriptor,
        objectMapper,
        cache,
        toolChest,
        metricsEmittingQueryRunnerInner,
        cachePopulator,
        cacheConfig
    );

    BySegmentQueryRunner<T> bySegmentQueryRunner = new BySegmentQueryRunner<>(
        segmentId,
        adapter.getDataInterval().getStart(),
        cachingQueryRunner
    );

    MetricsEmittingQueryRunner<T> metricsEmittingQueryRunnerOuter = new MetricsEmittingQueryRunner<>(
        emitter,
        toolChest,
        bySegmentQueryRunner,
        QueryMetrics::reportSegmentAndCacheTime,
        queryMetrics -> queryMetrics.segment(segmentIdString)
    ).withWaitMeasuredFromNow();

    SpecificSegmentQueryRunner<T> specificSegmentQueryRunner = new SpecificSegmentQueryRunner<>(
        metricsEmittingQueryRunnerOuter,
        segmentSpec
    );

    PerSegmentOptimizingQueryRunner<T> perSegmentOptimizingQueryRunner = new PerSegmentOptimizingQueryRunner<>(
        specificSegmentQueryRunner,
        new PerSegmentQueryOptimizationContext(segmentDescriptor)
    );

    return new SetAndVerifyContextQueryRunner<>(
        serverConfig,
        CPUTimeMetricQueryRunner.safeBuild(
            perSegmentOptimizingQueryRunner,
            toolChest,
            emitter,
            cpuTimeAccumulator,
            false
        )
    );
  }
}
