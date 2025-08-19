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

package org.apache.druid.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.apache.druid.client.CachingQueryRunner;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulator;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.FunctionalIterable;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
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
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryUnsupportedException;
import org.apache.druid.query.ReportTimelineMissingSegmentQueryRunner;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.druid.query.planning.ExecutionVertex;
import org.apache.druid.query.policy.PolicyEnforcer;
import org.apache.druid.query.spec.SpecificSegmentQueryRunner;
import org.apache.druid.query.spec.SpecificSegmentSpec;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentMapFunction;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.TimeBoundaryInspector;
import org.apache.druid.segment.loading.AcquireSegmentAction;
import org.apache.druid.segment.loading.VirtualPlaceholderSegment;
import org.apache.druid.server.coordination.DataSegmentAndDescriptor;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.apache.druid.utils.CloseableUtils;
import org.apache.druid.utils.JvmUtils;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Query handler for Historical processes (see CliHistorical).
 * <p>
 * In tests, this class's behavior is partially mimicked by TestClusterQuerySegmentWalker.
 */
public class ServerManager implements QuerySegmentWalker
{
  private static final EmittingLogger log = new EmittingLogger(ServerManager.class);
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final ServiceEmitter emitter;
  private final QueryProcessingPool queryProcessingPool;
  private final CachePopulator cachePopulator;
  private final Cache cache;
  private final ObjectMapper objectMapper;
  private final CacheConfig cacheConfig;
  private final SegmentManager segmentManager;
  private final ServerConfig serverConfig;
  private final PolicyEnforcer policyEnforcer;

  @Inject
  public ServerManager(
      QueryRunnerFactoryConglomerate conglomerate,
      ServiceEmitter emitter,
      QueryProcessingPool queryProcessingPool,
      CachePopulator cachePopulator,
      @Smile ObjectMapper objectMapper,
      Cache cache,
      CacheConfig cacheConfig,
      SegmentManager segmentManager,
      ServerConfig serverConfig,
      PolicyEnforcer policyEnforcer
  )
  {
    this.conglomerate = conglomerate;
    this.emitter = emitter;

    this.queryProcessingPool = queryProcessingPool;
    this.cachePopulator = cachePopulator;
    this.cache = cache;
    this.objectMapper = objectMapper;

    this.cacheConfig = cacheConfig;
    this.segmentManager = segmentManager;
    this.serverConfig = serverConfig;
    this.policyEnforcer = policyEnforcer;
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals)
  {
    final VersionedIntervalTimeline<String, DataSegment> timeline;
    final Optional<VersionedIntervalTimeline<String, DataSegment>> maybeTimeline =
        segmentManager.getTimeline(ExecutionVertex.of(query).getBaseTableDataSource());

    if (maybeTimeline.isPresent()) {
      timeline = maybeTimeline.get();
    } else {
      // Even though we didn't find a timeline for the query datasource, we simply return a NoopQueryRunner
      // instead of reporting missing intervals because the query intervals are a filter rather than something
      // we must find.
      return new NoopQueryRunner<>();
    }

    FunctionalIterable<SegmentDescriptor> segmentDescriptors = FunctionalIterable
        .create(intervals)
        .transformCat(timeline::lookup)
        .transformCat(
            holder -> {
              if (holder == null) {
                return null;
              }

              return FunctionalIterable
                  .create(holder.getObject())
                  .transform(
                      partitionChunk ->
                          new SegmentDescriptor(
                              holder.getInterval(),
                              holder.getVersion(),
                              partitionChunk.getChunkNumber()
                          )
                  );
            }
        );

    return getQueryRunnerForSegments(query, segmentDescriptors);
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs)
  {
    final ExecutionVertex ev = ExecutionVertex.of(query);
    final Optional<VersionedIntervalTimeline<String, DataSegment>> maybeTimeline =
        segmentManager.getTimeline(ev.getBaseTableDataSource());
    if (maybeTimeline.isEmpty()) {
      return new ReportTimelineMissingSegmentQueryRunner<>(Lists.newArrayList(specs));
    }

    final QueryRunnerFactory<T, Query<T>> factory = getQueryRunnerFactory(query);
    final QueryToolChest<T, Query<T>> toolChest = getQueryToolChest(query, factory);
    final VersionedIntervalTimeline<String, DataSegment> timeline = maybeTimeline.get();

    return new ResourceManagingQueryRunner<>(timeline, factory, toolChest, ev, specs);
  }

  /**
   * For each {@link SegmentDescriptor}, we try to fetch a {@link DataSegment} from the supplied
   * {@link VersionedIntervalTimeline}, then use
   * {@link SegmentManager#acquireSegments(Iterable)} to apply {@link SegmentMapFunction}
   * to get the segments from the cache (or loaded from deep storage if necessary) and transform the segments as
   * appropriate for query processing into {@link SegmentReference} wrappers. The wrappers contain the
   * {@link SegmentDescriptor} and an {@link Optional<Segment>}. If present, the {@link Segment} will be registered to
   * the {@link Closer}. If empty, the segment was not in the timeline, not in the cache, or the map function transform
   * was unable to be applied.
   */
  protected List<SegmentReference> getAndLoadAllSegmentReferences(
      VersionedIntervalTimeline<String, DataSegment> timeline,
      Iterable<SegmentDescriptor> segments,
      SegmentMapFunction segmentMapFunction,
      final long timeout
  )
  {
    final Iterable<DataSegmentAndDescriptor> segmentsToMap = FunctionalIterable
        .create(segments)
        .transform(
            descriptor -> {
              final PartitionChunk<DataSegment> chunk = timeline.findChunk(
                  descriptor.getInterval(),
                  descriptor.getVersion(),
                  descriptor.getPartitionNumber()
              );

              if (chunk != null) {
                final DataSegment segment = chunk.getObject();
                if (segment != null) {
                  return new DataSegmentAndDescriptor(segment, descriptor);
                }
              }
              return new DataSegmentAndDescriptor(null, descriptor);
            }
        );
    final List<AcquireSegmentAction> loaders = segmentManager.acquireSegments(segmentsToMap);
    long timeoutAt = System.currentTimeMillis() + timeout;
    return AcquireSegmentAction.mapAllSegments(loaders, segmentMapFunction, timeoutAt);
  }


  /**
   * Special handling for {@link SegmentMetadataQuery}, to force it to only use segments that are already stored in the
   * cache as strong references, using empty placeholder segments with a __time column and 0 rows for all segments that
   * are in the timeline but are missing from the cache (e.g. weak references)
   */
  private List<SegmentReference> getSegmentMetadataSegmentReferences(
      VersionedIntervalTimeline<String, DataSegment> timeline,
      Iterable<SegmentDescriptor> segments,
      SegmentMapFunction segmentMapFn
  )
  {
    // materialize to list to acquire all of the references
    List<SegmentReference> segmentReferences = new ArrayList<>();
    for (SegmentDescriptor descriptor : segments) {
      final PartitionChunk<DataSegment> chunk = timeline.findChunk(
          descriptor.getInterval(),
          descriptor.getVersion(),
          descriptor.getPartitionNumber()
      );
      if (chunk == null) {
        segmentReferences.add(SegmentReference.missing(descriptor));
      } else {
        final DataSegment dataSegment = chunk.getObject();
        final Optional<Segment> theSegment = segmentMapFn.apply(segmentManager.acquireSegment(dataSegment));
        if (theSegment.isPresent()) {
          segmentReferences.add(
              new SegmentReference(
                  descriptor,
                  theSegment,
                  AcquireSegmentAction.NOOP_CLEANUP
              )
          );
        } else {
          // make a stub to run the segment metadata query instead of being a 'missing' segment
          segmentReferences.add(
              new SegmentReference(
                  descriptor,
                  Optional.of(new VirtualPlaceholderSegment(dataSegment)),
                  AcquireSegmentAction.NOOP_CLEANUP
              )
          );
        }
      }
    }
    return segmentReferences;
  }

  protected <T> FunctionalIterable<QueryRunner<T>> getQueryRunnersForSegments(
      final VersionedIntervalTimeline<String, DataSegment> timeline,
      final Iterable<SegmentDescriptor> specs,
      final Query<T> query,
      final QueryRunnerFactory<T, Query<T>> factory,
      final QueryToolChest<T, Query<T>> toolChest,
      final SegmentMapFunction segmentMapFn,
      final AtomicLong cpuTimeAccumulator,
      final Optional<byte[]> cacheKeyPrefix,
      final Closer closer
  )
  {
    final List<SegmentReference> segmentReferences;
    // this is kind of gross, but otherwise we're going to be loading weak assignments more or less as soon as they
    // are assigned instead of on demand at query time
    if (query instanceof SegmentMetadataQuery) {
      segmentReferences = getSegmentMetadataSegmentReferences(timeline, specs, segmentMapFn);
    } else {
      segmentReferences = getAndLoadAllSegmentReferences(
          timeline,
          specs,
          segmentMapFn,
          query.context().getTimeout()
      );
    }
    return FunctionalIterable
        .create(segmentReferences)
        .transform(
            ref ->
                closer.register(ref)
                      .getSegmentReference()
                      .map(segment ->
                               buildQueryRunnerForSegment(
                                   ref.getSegmentDescriptor(),
                                   segment,
                                   factory,
                                   toolChest,
                                   cpuTimeAccumulator,
                                   cacheKeyPrefix
                               )
                      ).orElse(
                          new ReportTimelineMissingSegmentQueryRunner<>(ref.getSegmentDescriptor())
                      )
        );
  }

  protected <T> QueryRunner<T> buildQueryRunnerForSegment(
      final SegmentDescriptor segmentDescriptor,
      final Segment segment,
      final QueryRunnerFactory<T, Query<T>> factory,
      final QueryToolChest<T, Query<T>> toolChest,
      final AtomicLong cpuTimeAccumulator,
      Optional<byte[]> cacheKeyPrefix
  )
  {
    if (segment.isTombstone()) {
      return new NoopQueryRunner<>();
    }

    final SegmentId segmentId = segment.getId();
    final Interval segmentInterval = segment.getDataInterval();
    final String segmentIdString = segmentId.toString();

    final SpecificSegmentSpec segmentSpec = new SpecificSegmentSpec(segmentDescriptor);
    final MetricsEmittingQueryRunner<T> metricsEmittingQueryRunnerInner = new MetricsEmittingQueryRunner<>(
        emitter,
        toolChest,
        factory.createRunner(segment),
        QueryMetrics::reportSegmentTime,
        queryMetrics -> queryMetrics.segment(segmentIdString)
    );

    final TimeBoundaryInspector timeBoundaryInspector = segment.as(TimeBoundaryInspector.class);
    final Interval cacheKeyInterval = timeBoundaryInspector != null
                                      ? timeBoundaryInspector.getMinMaxInterval()
                                      : segmentInterval;
    final CachingQueryRunner<T> cachingQueryRunner = new CachingQueryRunner<>(
        segmentIdString,
        cacheKeyPrefix,
        segmentDescriptor,
        cacheKeyInterval,
        objectMapper,
        cache,
        toolChest,
        metricsEmittingQueryRunnerInner,
        cachePopulator,
        cacheConfig
    );

    final BySegmentQueryRunner<T> bySegmentQueryRunner = new BySegmentQueryRunner<>(
        segmentId,
        segmentInterval.getStart(),
        cachingQueryRunner
    );

    final MetricsEmittingQueryRunner<T> metricsEmittingQueryRunnerOuter = new MetricsEmittingQueryRunner<>(
        emitter,
        toolChest,
        bySegmentQueryRunner,
        QueryMetrics::reportSegmentAndCacheTime,
        queryMetrics -> queryMetrics.segment(segmentIdString)
    ).withWaitMeasuredFromNow();

    final SpecificSegmentQueryRunner<T> specificSegmentQueryRunner = new SpecificSegmentQueryRunner<>(
        metricsEmittingQueryRunnerOuter,
        segmentSpec
    );

    final PerSegmentOptimizingQueryRunner<T> perSegmentOptimizingQueryRunner = new PerSegmentOptimizingQueryRunner<>(
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


  private <T> QueryRunnerFactory<T, Query<T>> getQueryRunnerFactory(Query<T> query)
  {
    final DataSource dataSourceFromQuery = query.getDataSource();
    final QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(query);
    if (factory == null) {
      final QueryUnsupportedException e = new QueryUnsupportedException(
          StringUtils.format("Unknown query type, [%s]", query.getClass())
      );
      log.makeAlert(e, "Error while executing a query[%s]", query.getId())
         .addData("dataSource", dataSourceFromQuery)
         .emit();
      throw e;
    }
    return factory;
  }

  private static <T> QueryToolChest<T, Query<T>> getQueryToolChest(Query<T> query, QueryRunnerFactory<T, Query<T>> factory)
  {
    final DataSource dataSourceFromQuery = query.getDataSource();
    final QueryToolChest<T, Query<T>> toolChest = factory.getToolchest();
    // Make sure this query type can handle the subquery, if present.
    if ((dataSourceFromQuery instanceof QueryDataSource)
        && !toolChest.canPerformSubquery(((QueryDataSource) dataSourceFromQuery).getQuery())) {
      throw new QueryUnsupportedException(StringUtils.format("Cannot handle subquery: %s", dataSourceFromQuery));
    }
    return toolChest;
  }

  /**
   * {@link QueryRunner} that on run builds a set of {@link QueryRunner} for a set of {@link SegmentDescriptor} and
   * merges them using the {@link QueryToolChest}. The {@link VersionedIntervalTimeline} provides segment references,
   * which are registered with a closer as they are acquired, and then released in the baggage of merged result
   * {@link Sequence}
   */
  public final class ResourceManagingQueryRunner<T> implements QueryRunner<T>
  {
    private final VersionedIntervalTimeline<String, DataSegment> timeline;
    private final QueryRunnerFactory<T, Query<T>> factory;
    private final QueryToolChest<T, Query<T>> toolChest;
    private final ExecutionVertex ev;
    private final Iterable<SegmentDescriptor> specs;

    public ResourceManagingQueryRunner(
        VersionedIntervalTimeline<String, DataSegment> timeline,
        QueryRunnerFactory<T, Query<T>> factory,
        QueryToolChest<T, Query<T>> toolChest,
        ExecutionVertex ev,
        Iterable<SegmentDescriptor> specs
    )
    {
      this.timeline = timeline;
      this.factory = factory;
      this.toolChest = toolChest;
      this.ev = ev;
      this.specs = specs;
    }


    @Override
    public Sequence<T> run(QueryPlus<T> queryPlus, ResponseContext responseContext)
    {
      queryPlus = queryPlus.withQuery(
          ResourceIdPopulatingQueryRunner.populateResourceId(queryPlus.getQuery())
      );
      final Query<T> query = queryPlus.getQuery();
      final AtomicLong cpuTimeAccumulator = new AtomicLong(0L);
      final SegmentMapFunction segmentMapFn = JvmUtils.safeAccumulateThreadCpuTime(
          cpuTimeAccumulator,
          () -> ev.createSegmentMapFunction(policyEnforcer)
      );

      // We compute the datasource's cache key here itself so it doesn't need to be re-computed for every segment
      final Optional<byte[]> cacheKeyPrefix = Optional.ofNullable(query.getDataSource().getCacheKey());

      // closer to track all used resources
      final Closer closer = Closer.create();
      try {
        final FunctionalIterable<QueryRunner<T>> queryRunners = getQueryRunnersForSegments(
            timeline,
            specs,
            query,
            factory,
            toolChest,
            segmentMapFn,
            cpuTimeAccumulator,
            cacheKeyPrefix,
            closer
        );
        final QueryRunner<T> queryRunner = CPUTimeMetricQueryRunner.safeBuild(
            new FinalizeResultsQueryRunner<>(
                toolChest.mergeResults(factory.mergeRunners(queryProcessingPool, queryRunners), true),
                toolChest
            ),
            toolChest,
            emitter,
            cpuTimeAccumulator,
            true
        );
        return queryRunner.run(queryPlus, responseContext).withBaggage(closer);
      }
      catch (Throwable t) {
        throw CloseableUtils.closeAndWrapInCatch(t, closer);
      }
    }
  }
}
