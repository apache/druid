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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import org.apache.druid.client.CachingQueryRunner;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulator;
import org.apache.druid.error.DruidException;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.FunctionalIterable;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.BySegmentQueryRunner;
import org.apache.druid.query.CPUTimeMetricQueryRunner;
import org.apache.druid.query.DataSegmentAndDescriptor;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.FinalizeResultsQueryRunner;
import org.apache.druid.query.LeafSegmentsBundle;
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
import org.apache.druid.segment.loading.AcquireSegmentResult;
import org.apache.druid.segment.loading.VirtualPlaceholderSegment;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.apache.druid.utils.CloseableUtils;
import org.apache.druid.utils.JvmUtils;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
  protected final SegmentManager segmentManager;
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
      return (queryPlus, responseContext) -> {
        responseContext.addMissingSegments(Lists.newArrayList(specs));
        return Sequences.empty();
      };
    }

    final QueryRunnerFactory<T, Query<T>> factory = getQueryRunnerFactory(query);
    final QueryToolChest<T, Query<T>> toolChest = getQueryToolChest(query, factory);
    final VersionedIntervalTimeline<String, DataSegment> timeline = maybeTimeline.get();

    return new ResourceManagingQueryRunner<>(timeline, factory, toolChest, ev, specs);
  }

  /**
   * For each {@link SegmentDescriptor}, we try to obtain a {@link DataSegment} from the supplied
   * {@link VersionedIntervalTimeline} to create a {@link LeafSegmentsBundle} which contains the partitioned segments
   * into 3 groups: those which are already present in the cache, those which can be loaded on demand, and those which
   * were missing from the {@link VersionedIntervalTimeline}.
   */
  protected LeafSegmentsBundle getSegmentsBundle(
      VersionedIntervalTimeline<String, DataSegment> timeline,
      Iterable<SegmentDescriptor> segments,
      SegmentMapFunction segmentMapFunction
  )
  {
    // first we build a list of DataSegment we need to get references for, paired with the descriptor from the query
    final List<DataSegmentAndDescriptor> segmentsToMap = new ArrayList<>();
    for (SegmentDescriptor descriptor : segments) {
      final PartitionChunk<DataSegment> chunk = timeline.findChunk(
          descriptor.getInterval(),
          descriptor.getVersion(),
          descriptor.getPartitionNumber()
      );

      if (chunk != null) {
        segmentsToMap.add(new DataSegmentAndDescriptor(chunk.getObject(), descriptor));
      } else {
        segmentsToMap.add(DataSegmentAndDescriptor.missing(descriptor));
      }
    }

    return segmentManager.getSegmentsBundle(segmentsToMap, segmentMapFunction);
  }

  /**
   * Combines {@link LeafSegmentsBundle#cachedSegments} with {@link LeafSegmentsBundle#loadableSegments}, loading the
   * latter set into the cache
   */
  protected ArrayList<SegmentReference> getOrLoadBundleSegments(
      QueryPlus<?> queryPlus,
      LeafSegmentsBundle segmentsBundle,
      SegmentMapFunction segmentMapFunction
  )
  {
    final ArrayList<SegmentReference> segmentReferences = new ArrayList<>(
        segmentsBundle.getCachedSegments().size() + segmentsBundle.getLoadableSegments().size()
    );
    segmentReferences.addAll(segmentsBundle.getCachedSegments());

    // loadable segments are skipped for segment metadata queries to not cause disk churn from pulling them all down,
    // so we use placeholders instead. this is kind of gross, but otherwise we're going to be loading weak assignments
    // more or less as soon as they are assigned instead of on demand at query time due to the broker issuing metadata
    // queries to build the SQL schema
    if (queryPlus.getQuery() instanceof SegmentMetadataQuery) {
      for (DataSegmentAndDescriptor segment : segmentsBundle.getLoadableSegments()) {
        segmentReferences.add(
            new SegmentReference(
                segment.getDescriptor(),
                Optional.of(new VirtualPlaceholderSegment(segment.getDataSegment())),
                null
            )
        );
      }
    } else {
      // load the remaining segments
      try {
        final LoadSegmentsResult result = getOrLoadSegmentReferences(
            segmentsBundle.getLoadableSegments(),
            segmentMapFunction,
            queryPlus.getQuery().context().getTimeout()
        );
        segmentReferences.addAll(result.getSegmentReferences());
        if (segmentManager.canLoadSegmentsOnDemand()) {
          result.reportMetrics(queryPlus.getQueryMetrics());
        }
      }
      catch (Throwable t) {
        throw CloseableUtils.closeAndWrapInCatch(t, segmentsBundle::closeCachedReferences);
      }
    }

    return segmentReferences;
  }

  /**
   * Given a list of {@link DataSegmentAndDescriptor}, uses {@link SegmentManager#acquireSegment(DataSegment)} for each
   * to obtain a 'reference' to segments in the cache (or load from deep storage if necessary/supported by the
   * storage layer).
   * <p>
   * For each of these segments, we then apply a {@link SegmentMapFunction} to prepare for processing. The returned
   * {@link SegmentReference} MUST BE CLOSED to release the reference.
   */
  protected LoadSegmentsResult getOrLoadSegmentReferences(
      List<DataSegmentAndDescriptor> segmentsToMap,
      SegmentMapFunction segmentMapFunction,
      long timeout
  )
  {
    final long startLoadTime = System.nanoTime();
    // closer to collect everything that needs cleaned up in the event of failure, if we make it out of this function,
    // closing the segment reference handles everything and it is the callers responsibility
    final Closer safetyNet = Closer.create();

    // build list of acquire reference actions, this does not initiate the actions until we collect the futures. this
    // does place a hold on any weakly held references if the cache includes segments that reside on virtual storage
    // fabric
    final List<AcquireSegmentAction> actions = new ArrayList<>();
    try {
      for (DataSegmentAndDescriptor segment : segmentsToMap) {
        if (segment.getDataSegment() == null) {
          actions.add(safetyNet.register(AcquireSegmentAction.missingSegment()));
        } else {
          actions.add(safetyNet.register(segmentManager.acquireSegment(segment.getDataSegment())));
        }
      }
    }
    catch (Throwable t) {
      throw CloseableUtils.closeInCatch(
          t instanceof DruidException
          ? (DruidException) t
          : DruidException.forPersona(DruidException.Persona.OPERATOR)
                          .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                          .build(t, t.getMessage()),
          safetyNet
      );
    }

    long timeoutAt = System.currentTimeMillis() + timeout;

    Throwable failure = null;

    // getting the future kicks off any background action, so materialize them all to a list to get things started
    final List<ListenableFuture<AcquireSegmentResult>> futures = new ArrayList<>(actions.size());
    for (AcquireSegmentAction acquireSegmentAction : actions) {
      // if we haven't failed yet, keep collecting futures
      if (failure == null) {
        try {
          futures.add(acquireSegmentAction.getSegmentFuture());
        }
        catch (Throwable t) {
          failure = t;
        }
      } else {
        futures.add(Futures.immediateFuture(AcquireSegmentResult.empty()));
      }
    }

    if (failure != null) {
      throw CloseableUtils.closeInCatch(
          failure instanceof DruidException
          ? (DruidException) failure
          : DruidException.forPersona(DruidException.Persona.OPERATOR)
                          .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                          .build(failure, "Failed to acquire segment references to process query"),
          safetyNet
      );
    }

    final ArrayList<SegmentReference> segmentReferences = new ArrayList<>(actions.size());
    long totalSegmentsLoadTime = 0;
    long totalSegmentsLoadWaitTime = 0;
    long maxSegmentLoadTime = 0;
    long maxSegmentWaitTime = 0;
    long bytesLoaded = 0;
    boolean timedOut = false;
    boolean interrupted = false;
    for (int i = 0; i < actions.size(); i++) {
      try {
        final DataSegmentAndDescriptor segmentAndDescriptor = segmentsToMap.get(i);
        final AcquireSegmentAction action = actions.get(i);
        final ListenableFuture<AcquireSegmentResult> future = futures.get(i);
        final AcquireSegmentResult result = future.get(timeoutAt - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        if (result == null) {
          segmentReferences.add(
              new SegmentReference(segmentAndDescriptor.getDescriptor(), Optional.empty(), action)
          );
        } else {
          totalSegmentsLoadTime += result.getLoadTimeNanos();
          totalSegmentsLoadWaitTime += result.getWaitTimeNanos();
          maxSegmentLoadTime = Math.max(maxSegmentLoadTime, result.getLoadTimeNanos());
          maxSegmentWaitTime = Math.max(maxSegmentWaitTime, result.getWaitTimeNanos());
          bytesLoaded += result.getLoadSizeBytes();
          final Optional<Segment> segment = result.getReferenceProvider().acquireReference();
          try {
            final Optional<Segment> mappedSegment = segmentMapFunction.apply(segment).map(safetyNet::register);
            segmentReferences.add(
                new SegmentReference(
                    segmentAndDescriptor.getDescriptor(),
                    mappedSegment,
                    action
                )
            );
          }
          catch (Throwable t) {
            // if applying the mapFn failed, attach the base segment to the closer and rethrow
            segment.ifPresent(safetyNet::register);
            throw t;
          }
        }
      }
      catch (Throwable t) {
        if (t instanceof InterruptedException) {
          interrupted = true;
        }
        if (t instanceof TimeoutException) {
          timedOut = true;
        }
        if (failure == null) {
          failure = t;
        } else {
          // no need to get carried away, if a bunch fail this ceases to be useful
          if (failure.getSuppressed().length <= 10) {
            failure.addSuppressed(t);
          }
        }
      }
    }
    if (failure != null) {
      final DruidException toThrow;
      if (failure instanceof DruidException) {
        toThrow = (DruidException) failure;
      } else if (timedOut) {
        toThrow = DruidException.forPersona(DruidException.Persona.USER)
                                .ofCategory(DruidException.Category.TIMEOUT)
                                .build(failure, "Failed to acquire segment references to process query");
      } else if (interrupted) {
        Thread.currentThread().interrupt();
        toThrow = DruidException.forPersona(DruidException.Persona.OPERATOR)
                                .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                                .build(failure, "Interrupted waiting for segments");
      } else {
        toThrow = DruidException.forPersona(DruidException.Persona.OPERATOR)
                                .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                                .build(failure, "Failed to acquire segment references to process query");
      }
      throw CloseableUtils.closeInCatch(toThrow, safetyNet);
    }
    final long loadTime = System.nanoTime() - startLoadTime;
    final long count = actions.size();
    return new LoadSegmentsResult(
        segmentReferences,
        loadTime,
        maxSegmentLoadTime,
        count == 0 ? 0 : totalSegmentsLoadTime / count,
        maxSegmentWaitTime,
        count == 0 ? 0 : totalSegmentsLoadWaitTime / count,
        bytesLoaded,
        count
    );
  }

  protected <T> FunctionalIterable<QueryRunner<T>> getQueryRunnersForSegments(
      final Query<T> query,
      final QueryRunnerFactory<T, Query<T>> factory,
      final QueryToolChest<T, Query<T>> toolChest,
      final List<SegmentReference> segmentReferences,
      final AtomicLong cpuTimeAccumulator,
      final Optional<byte[]> cacheKeyPrefix
  )
  {
    return FunctionalIterable
        .create(segmentReferences)
        .transform(
            ref ->
                ref.getSegmentReference()
                   .map(segment ->
                            buildQueryRunnerForSegment(
                                ref.getSegmentDescriptor(),
                                segment,
                                factory,
                                toolChest,
                                cpuTimeAccumulator,
                                cacheKeyPrefix
                            )
                   ).orElseThrow(
                       () -> DruidException.defensive("Unexpected missing segment[%s]", ref.getSegmentDescriptor())
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
        // TODO: Maybe also apply a log to see when this is called.
        queryMetrics -> {
          log.info("Segment MetricsEmittingQueryRunner accepting metrics[%s]", queryMetrics);
          queryMetrics.segment(segmentIdString);
        }
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

  protected <T> QueryRunnerFactory<T, Query<T>> getQueryRunnerFactory(Query<T> query)
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

  protected static <T> QueryToolChest<T, Query<T>> getQueryToolChest(Query<T> query, QueryRunnerFactory<T, Query<T>> factory)
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
  public class ResourceManagingQueryRunner<T> implements QueryRunner<T>
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
      ).withQueryMetrics(toolChest);
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
        final LeafSegmentsBundle segmentsBundle = getLeafSegmentsBundle(query, segmentMapFn);

        responseContext.addMissingSegments(segmentsBundle.getMissingSegments());

        final List<SegmentReference> segmentReferences = getOrLoadBundleSegments(
            queryPlus,
            segmentsBundle,
            segmentMapFn
        );
        closer.registerAll(segmentReferences);

        final FunctionalIterable<QueryRunner<T>> queryRunners = getQueryRunnersForSegments(
            query,
            factory,
            toolChest,
            segmentReferences,
            cpuTimeAccumulator,
            cacheKeyPrefix
        );

        final QueryRunner<T> finalizeResultsQueryRunner = new FinalizeResultsQueryRunner<>(
            toolChest.mergeResults(factory.mergeRunners(queryProcessingPool, queryRunners), true),
            toolChest
        );

        final QueryRunner<T> datasourceMetricsEmittingQueryRunner = new MetricsEmittingQueryRunner<>(
            emitter,
            toolChest,
            finalizeResultsQueryRunner,
            MetricsEmittingQueryRunner.NOOP_METRIC_REPORTER,
            metrics -> {
              // TODO: Remove the logs after finished.
              log.info("Datasource MetricsEmittingQueryRunner accepting metrics[%s]", metrics);
              metrics.queryId(query.getId());
              metrics.sqlQueryId(query.getSqlQueryId());
            }
        );

        final QueryRunner<T> queryRunner = CPUTimeMetricQueryRunner.safeBuild(
            datasourceMetricsEmittingQueryRunner,
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


    @VisibleForTesting
    protected LeafSegmentsBundle getLeafSegmentsBundle(Query<T> query, SegmentMapFunction segmentMapFunction)
    {
      return ServerManager.this.getSegmentsBundle(timeline, specs, segmentMapFunction);
    }
  }

  public static class LoadSegmentsResult
  {
    private final ArrayList<SegmentReference> segmentReferences;
    private final long wallTimeNanos;
    private final long maxTimeNanos;
    private final long avgTimeNanos;
    private final long maxWaitNanos;
    private final long avgWaitNanos;
    private final long totalBytes;
    private final long count;

    public LoadSegmentsResult(
        ArrayList<SegmentReference> segmentReferences,
        long wallTimeNanos,
        long maxTimeNanos,
        long avgTimeNanos,
        long maxWaitNanos,
        long avgWaitNanos,
        long totalBytes,
        long count
    )
    {
      this.segmentReferences = segmentReferences;
      this.wallTimeNanos = wallTimeNanos;
      this.maxTimeNanos = maxTimeNanos;
      this.avgTimeNanos = avgTimeNanos;
      this.maxWaitNanos = maxWaitNanos;
      this.avgWaitNanos = avgWaitNanos;
      this.totalBytes = totalBytes;
      this.count = count;
    }

    public List<SegmentReference> getSegmentReferences()
    {
      return segmentReferences;
    }

    public void reportMetrics(@Nullable QueryMetrics<?> queryMetrics)
    {
      if (queryMetrics != null) {
        queryMetrics.reportSegmentOnDemandLoadTime(wallTimeNanos);
        queryMetrics.reportSegmentOnDemandLoadTimeMax(maxTimeNanos);
        queryMetrics.reportSegmentOnDemandLoadTimeAvg(avgTimeNanos);
        queryMetrics.reportSegmentOnDemandLoadWaitTimeMax(maxWaitNanos);
        queryMetrics.reportSegmentOnDemandLoadWaitTimeAvg(avgWaitNanos);
        queryMetrics.reportSegmentOnDemandLoadBytes(totalBytes);
        queryMetrics.reportSegmentOnDemandLoadCount(count);
      }
    }
  }
}
