/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.coordination;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.client.CachingQueryRunner;
import io.druid.client.cache.Cache;
import io.druid.client.cache.CacheConfig;
import io.druid.guice.annotations.BackgroundCaching;
import io.druid.guice.annotations.Processing;
import io.druid.guice.annotations.Smile;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.FunctionalIterable;
import io.druid.query.BySegmentQueryRunner;
import io.druid.query.CPUTimeMetricQueryRunner;
import io.druid.query.DataSource;
import io.druid.query.FinalizeResultsQueryRunner;
import io.druid.query.MetricsEmittingQueryRunner;
import io.druid.query.NoopQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryMetrics;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.ReferenceCountingSegmentQueryRunner;
import io.druid.query.ReportTimelineMissingSegmentQueryRunner;
import io.druid.query.SegmentDescriptor;
import io.druid.query.TableDataSource;
import io.druid.query.spec.SpecificSegmentQueryRunner;
import io.druid.query.spec.SpecificSegmentSpec;
import io.druid.segment.ReferenceCountingSegment;
import io.druid.server.SegmentManager;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.PartitionChunk;
import io.druid.timeline.partition.PartitionHolder;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Arrays;
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
  private final ExecutorService cachingExec;
  private final Cache cache;
  private final ObjectMapper objectMapper;
  private final CacheConfig cacheConfig;
  private final SegmentManager segmentManager;

  @Inject
  public ServerManager(
      QueryRunnerFactoryConglomerate conglomerate,
      ServiceEmitter emitter,
      @Processing ExecutorService exec,
      @BackgroundCaching ExecutorService cachingExec,
      @Smile ObjectMapper objectMapper,
      Cache cache,
      CacheConfig cacheConfig,
      SegmentManager segmentManager
  )
  {
    this.conglomerate = conglomerate;
    this.emitter = emitter;

    this.exec = exec;
    this.cachingExec = cachingExec;
    this.cache = cache;
    this.objectMapper = objectMapper;

    this.cacheConfig = cacheConfig;
    this.segmentManager = segmentManager;
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

    DataSource dataSource = query.getDataSource();
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
        new FinalizeResultsQueryRunner<T>(
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
                  return Arrays.<QueryRunner<T>>asList(new ReportTimelineMissingSegmentQueryRunner<T>(input));
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
    String segmentId = adapter.getIdentifier();
    return CPUTimeMetricQueryRunner.safeBuild(
        new SpecificSegmentQueryRunner<T>(
            new MetricsEmittingQueryRunner<T>(
                emitter,
                toolChest,
                new BySegmentQueryRunner<T>(
                    segmentId,
                    adapter.getDataInterval().getStart(),
                    new CachingQueryRunner<T>(
                        segmentId,
                        segmentDescriptor,
                        objectMapper,
                        cache,
                        toolChest,
                        new MetricsEmittingQueryRunner<T>(
                            emitter,
                            toolChest,
                            new ReferenceCountingSegmentQueryRunner<T>(factory, adapter, segmentDescriptor),
                            QueryMetrics::reportSegmentTime,
                            queryMetrics -> queryMetrics.segment(segmentId)
                        ),
                        cachingExec,
                        cacheConfig
                    )
                ),
                QueryMetrics::reportSegmentAndCacheTime,
                queryMetrics -> queryMetrics.segment(segmentId)
            ).withWaitMeasuredFromNow(),
            segmentSpec
        ),
        toolChest,
        emitter,
        cpuTimeAccumulator,
        false
    );
  }
}
