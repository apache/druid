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

package org.apache.druid.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.RangeSet;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.inject.Inject;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulator;
import org.apache.druid.client.selector.QueryableDruidServer;
import org.apache.druid.client.selector.ServerSelector;
import org.apache.druid.guice.annotations.Client;
import org.apache.druid.guice.annotations.Merging;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.guice.http.DruidHttpClientConfig;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.NonnullPair;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.LazySequence;
import org.apache.druid.java.util.common.guava.ParallelMergeCombiningSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.BySegmentResultValueClass;
import org.apache.druid.query.CacheStrategy;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.Queries;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.Result;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.aggregation.MetricManipulatorFns;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.context.ResponseContext.Key;
import org.apache.druid.query.filter.DimFilterUtils;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.server.QueryResource;
import org.apache.druid.server.QueryScheduler;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.TimelineLookup;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.apache.druid.timeline.partition.PartitionHolder;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BinaryOperator;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

/**
 * This is the class on the Broker that is responsible for making native Druid queries to a cluster of data servers.
 *
 * The main user of this class is {@link org.apache.druid.server.ClientQuerySegmentWalker}. In tests, its behavior
 * is partially mimicked by TestClusterQuerySegmentWalker.
 */
public class CachingClusteredClient implements QuerySegmentWalker
{
  private static final EmittingLogger log = new EmittingLogger(CachingClusteredClient.class);
  private final QueryToolChestWarehouse warehouse;
  private final TimelineServerView serverView;
  private final Cache cache;
  private final ObjectMapper objectMapper;
  private final CachePopulator cachePopulator;
  private final CacheConfig cacheConfig;
  private final DruidHttpClientConfig httpClientConfig;
  private final DruidProcessingConfig processingConfig;
  private final ForkJoinPool pool;
  private final QueryScheduler scheduler;

  @Inject
  public CachingClusteredClient(
      QueryToolChestWarehouse warehouse,
      TimelineServerView serverView,
      Cache cache,
      @Smile ObjectMapper objectMapper,
      CachePopulator cachePopulator,
      CacheConfig cacheConfig,
      @Client DruidHttpClientConfig httpClientConfig,
      DruidProcessingConfig processingConfig,
      @Merging ForkJoinPool pool,
      QueryScheduler scheduler
  )
  {
    this.warehouse = warehouse;
    this.serverView = serverView;
    this.cache = cache;
    this.objectMapper = objectMapper;
    this.cachePopulator = cachePopulator;
    this.cacheConfig = cacheConfig;
    this.httpClientConfig = httpClientConfig;
    this.processingConfig = processingConfig;
    this.pool = pool;
    this.scheduler = scheduler;

    if (cacheConfig.isQueryCacheable(Query.GROUP_BY) && (cacheConfig.isUseCache() || cacheConfig.isPopulateCache())) {
      log.warn(
          "Even though groupBy caching is enabled in your configuration, v2 groupBys will not be cached on the broker. "
          + "Consider enabling caching on your data nodes if it is not already enabled."
      );
    }

    serverView.registerSegmentCallback(
        Execs.singleThreaded("CCClient-ServerView-CB-%d"),
        new ServerView.BaseSegmentCallback()
        {
          @Override
          public ServerView.CallbackAction segmentRemoved(DruidServerMetadata server, DataSegment segment)
          {
            CachingClusteredClient.this.cache.close(segment.getId().toString());
            return ServerView.CallbackAction.CONTINUE;
          }
        }
    );
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(final Query<T> query, final Iterable<Interval> intervals)
  {
    return new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(final QueryPlus<T> queryPlus, final ResponseContext responseContext)
      {
        return CachingClusteredClient.this.run(queryPlus, responseContext, timeline -> timeline, false);
      }
    };
  }

  /**
   * Run a query. The timelineConverter will be given the "master" timeline and can be used to return a different
   * timeline, if desired. This is used by getQueryRunnerForSegments.
   */
  private <T> Sequence<T> run(
      final QueryPlus<T> queryPlus,
      final ResponseContext responseContext,
      final UnaryOperator<TimelineLookup<String, ServerSelector>> timelineConverter,
      final boolean specificSegments
  )
  {
    final ClusterQueryResult<T> result = new SpecificQueryRunnable<>(queryPlus, responseContext)
        .run(timelineConverter, specificSegments);
    initializeNumRemainingResponsesInResponseContext(queryPlus.getQuery(), responseContext, result.numQueryServers);
    return result.sequence;
  }

  private static <T> void initializeNumRemainingResponsesInResponseContext(
      final Query<T> query,
      final ResponseContext responseContext,
      final int numQueryServers
  )
  {
    responseContext.add(
        Key.REMAINING_RESPONSES_FROM_QUERY_SERVERS,
        new NonnullPair<>(query.getMostSpecificId(), numQueryServers)
    );
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(final Query<T> query, final Iterable<SegmentDescriptor> specs)
  {
    return new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(final QueryPlus<T> queryPlus, final ResponseContext responseContext)
      {
        return CachingClusteredClient.this.run(
            queryPlus,
            responseContext,
            timeline -> {
              final VersionedIntervalTimeline<String, ServerSelector> timeline2 =
                  new VersionedIntervalTimeline<>(Ordering.natural());
              for (SegmentDescriptor spec : specs) {
                final PartitionHolder<ServerSelector> entry = timeline.findEntry(spec.getInterval(), spec.getVersion());
                if (entry != null) {
                  final PartitionChunk<ServerSelector> chunk = entry.getChunk(spec.getPartitionNumber());
                  if (chunk != null) {
                    timeline2.add(spec.getInterval(), spec.getVersion(), chunk);
                  }
                }
              }
              return timeline2;
            },
            true
        );
      }
    };
  }

  private static class ClusterQueryResult<T>
  {
    private final Sequence<T> sequence;
    private final int numQueryServers;

    private ClusterQueryResult(Sequence<T> sequence, int numQueryServers)
    {
      this.sequence = sequence;
      this.numQueryServers = numQueryServers;
    }
  }

  /**
   * This class essentially encapsulates the major part of the logic of {@link CachingClusteredClient}. It's state and
   * methods couldn't belong to {@link CachingClusteredClient} itself, because they depend on the specific query object
   * being run, but {@link QuerySegmentWalker} API is designed so that implementations should be able to accept
   * arbitrary queries.
   */
  private class SpecificQueryRunnable<T>
  {
    private final ResponseContext responseContext;
    private QueryPlus<T> queryPlus;
    private Query<T> query;
    private final QueryToolChest<T, Query<T>> toolChest;
    @Nullable
    private final CacheStrategy<T, Object, Query<T>> strategy;
    private final boolean useCache;
    private final boolean populateCache;
    private final boolean isBySegment;
    private final int uncoveredIntervalsLimit;
    private final Map<String, Cache.NamedKey> cachePopulatorKeyMap = new HashMap<>();
    private final DataSourceAnalysis dataSourceAnalysis;
    private final List<Interval> intervals;

    SpecificQueryRunnable(final QueryPlus<T> queryPlus, final ResponseContext responseContext)
    {
      this.queryPlus = queryPlus;
      this.responseContext = responseContext;
      this.query = queryPlus.getQuery();
      this.toolChest = warehouse.getToolChest(query);
      this.strategy = toolChest.getCacheStrategy(query);

      this.useCache = CacheUtil.isUseSegmentCache(query, strategy, cacheConfig, CacheUtil.ServerType.BROKER);
      this.populateCache = CacheUtil.isPopulateSegmentCache(query, strategy, cacheConfig, CacheUtil.ServerType.BROKER);
      this.isBySegment = QueryContexts.isBySegment(query);
      // Note that enabling this leads to putting uncovered intervals information in the response headers
      // and might blow up in some cases https://github.com/apache/druid/issues/2108
      this.uncoveredIntervalsLimit = QueryContexts.getUncoveredIntervalsLimit(query);
      this.dataSourceAnalysis = DataSourceAnalysis.forDataSource(query.getDataSource());
      // For nested queries, we need to look at the intervals of the inner most query.
      this.intervals = dataSourceAnalysis.getBaseQuerySegmentSpec()
                                         .map(QuerySegmentSpec::getIntervals)
                                         .orElseGet(() -> query.getIntervals());
    }

    private ImmutableMap<String, Object> makeDownstreamQueryContext()
    {
      final ImmutableMap.Builder<String, Object> contextBuilder = new ImmutableMap.Builder<>();

      final int priority = QueryContexts.getPriority(query);
      contextBuilder.put(QueryContexts.PRIORITY_KEY, priority);
      final String lane = QueryContexts.getLane(query);
      if (lane != null) {
        contextBuilder.put(QueryContexts.LANE_KEY, lane);
      }

      if (populateCache) {
        // prevent down-stream nodes from caching results as well if we are populating the cache
        contextBuilder.put(CacheConfig.POPULATE_CACHE, false);
        contextBuilder.put("bySegment", true);
      }
      return contextBuilder.build();
    }

    /**
     * Builds a query distribution and merge plan.
     *
     * This method returns an empty sequence if the query datasource is unknown or there is matching result-level cache.
     * Otherwise, it creates a sequence merging sequences from the regular broker cache and remote servers. If parallel
     * merge is enabled, it can merge and *combine* the underlying sequences in parallel.
     *
     * @return a pair of a sequence merging results from remote query servers and the number of remote servers
     *         participating in query processing.
     */
    ClusterQueryResult<T> run(
        final UnaryOperator<TimelineLookup<String, ServerSelector>> timelineConverter,
        final boolean specificSegments
    )
    {
      final Optional<? extends TimelineLookup<String, ServerSelector>> maybeTimeline = serverView.getTimeline(
          dataSourceAnalysis
      );
      if (!maybeTimeline.isPresent()) {
        return new ClusterQueryResult<>(Sequences.empty(), 0);
      }

      final TimelineLookup<String, ServerSelector> timeline = timelineConverter.apply(maybeTimeline.get());
      if (uncoveredIntervalsLimit > 0) {
        computeUncoveredIntervals(timeline);
      }

      final Set<SegmentServerSelector> segmentServers = computeSegmentsToQuery(timeline, specificSegments);
      @Nullable
      final byte[] queryCacheKey = computeQueryCacheKey();
      if (query.getContext().get(QueryResource.HEADER_IF_NONE_MATCH) != null) {
        @Nullable
        final String prevEtag = (String) query.getContext().get(QueryResource.HEADER_IF_NONE_MATCH);
        @Nullable
        final String currentEtag = computeCurrentEtag(segmentServers, queryCacheKey);
        if (currentEtag != null && currentEtag.equals(prevEtag)) {
          return new ClusterQueryResult<>(Sequences.empty(), 0);
        }
      }

      final List<Pair<Interval, byte[]>> alreadyCachedResults =
          pruneSegmentsWithCachedResults(queryCacheKey, segmentServers);

      query = scheduler.prioritizeAndLaneQuery(queryPlus, segmentServers);
      queryPlus = queryPlus.withQuery(query);

      final SortedMap<DruidServer, List<SegmentDescriptor>> segmentsByServer = groupSegmentsByServer(segmentServers);
      LazySequence<T> mergedResultSequence = new LazySequence<>(() -> {
        List<Sequence<T>> sequencesByInterval = new ArrayList<>(alreadyCachedResults.size() + segmentsByServer.size());
        addSequencesFromCache(sequencesByInterval, alreadyCachedResults);
        addSequencesFromServer(sequencesByInterval, segmentsByServer);
        return merge(sequencesByInterval);
      });

      return new ClusterQueryResult<>(scheduler.run(query, mergedResultSequence), segmentsByServer.size());
    }

    private Sequence<T> merge(List<Sequence<T>> sequencesByInterval)
    {
      BinaryOperator<T> mergeFn = toolChest.createMergeFn(query);
      if (processingConfig.useParallelMergePool() && QueryContexts.getEnableParallelMerges(query) && mergeFn != null) {
        return new ParallelMergeCombiningSequence<>(
            pool,
            sequencesByInterval,
            query.getResultOrdering(),
            mergeFn,
            QueryContexts.hasTimeout(query),
            QueryContexts.getTimeout(query),
            QueryContexts.getPriority(query),
            QueryContexts.getParallelMergeParallelism(query, processingConfig.getMergePoolDefaultMaxQueryParallelism()),
            QueryContexts.getParallelMergeInitialYieldRows(query, processingConfig.getMergePoolTaskInitialYieldRows()),
            QueryContexts.getParallelMergeSmallBatchRows(query, processingConfig.getMergePoolSmallBatchRows()),
            processingConfig.getMergePoolTargetTaskRunTimeMillis(),
            reportMetrics -> {
              QueryMetrics<?> queryMetrics = queryPlus.getQueryMetrics();
              if (queryMetrics != null) {
                queryMetrics.parallelMergeParallelism(reportMetrics.getParallelism());
                queryMetrics.reportParallelMergeParallelism(reportMetrics.getParallelism());
                queryMetrics.reportParallelMergeInputSequences(reportMetrics.getInputSequences());
                queryMetrics.reportParallelMergeInputRows(reportMetrics.getInputRows());
                queryMetrics.reportParallelMergeOutputRows(reportMetrics.getOutputRows());
                queryMetrics.reportParallelMergeTaskCount(reportMetrics.getTaskCount());
                queryMetrics.reportParallelMergeTotalCpuTime(reportMetrics.getTotalCpuTime());
              }
            }
        );
      } else {
        return Sequences
            .simple(sequencesByInterval)
            .flatMerge(seq -> seq, query.getResultOrdering());
      }
    }

    private Set<SegmentServerSelector> computeSegmentsToQuery(
        TimelineLookup<String, ServerSelector> timeline,
        boolean specificSegments
    )
    {
      final java.util.function.Function<Interval, List<TimelineObjectHolder<String, ServerSelector>>> lookupFn
          = specificSegments ? timeline::lookupWithIncompletePartitions : timeline::lookup;
      final List<TimelineObjectHolder<String, ServerSelector>> serversLookup = toolChest.filterSegments(
          query,
          intervals.stream().flatMap(i -> lookupFn.apply(i).stream()).collect(Collectors.toList())
      );

      final Set<SegmentServerSelector> segments = new LinkedHashSet<>();
      final Map<String, Optional<RangeSet<String>>> dimensionRangeCache = new HashMap<>();
      // Filter unneeded chunks based on partition dimension
      for (TimelineObjectHolder<String, ServerSelector> holder : serversLookup) {
        final Set<PartitionChunk<ServerSelector>> filteredChunks = DimFilterUtils.filterShards(
            query.getFilter(),
            holder.getObject(),
            partitionChunk -> partitionChunk.getObject().getSegment().getShardSpec(),
            dimensionRangeCache
        );
        for (PartitionChunk<ServerSelector> chunk : filteredChunks) {
          ServerSelector server = chunk.getObject();
          final SegmentDescriptor segment = new SegmentDescriptor(
              holder.getInterval(),
              holder.getVersion(),
              chunk.getChunkNumber()
          );
          segments.add(new SegmentServerSelector(server, segment));
        }
      }
      return segments;
    }

    private void computeUncoveredIntervals(TimelineLookup<String, ServerSelector> timeline)
    {
      final List<Interval> uncoveredIntervals = new ArrayList<>(uncoveredIntervalsLimit);
      boolean uncoveredIntervalsOverflowed = false;

      for (Interval interval : intervals) {
        Iterable<TimelineObjectHolder<String, ServerSelector>> lookup = timeline.lookup(interval);
        long startMillis = interval.getStartMillis();
        long endMillis = interval.getEndMillis();
        for (TimelineObjectHolder<String, ServerSelector> holder : lookup) {
          Interval holderInterval = holder.getInterval();
          long intervalStart = holderInterval.getStartMillis();
          if (!uncoveredIntervalsOverflowed && startMillis != intervalStart) {
            if (uncoveredIntervalsLimit > uncoveredIntervals.size()) {
              uncoveredIntervals.add(Intervals.utc(startMillis, intervalStart));
            } else {
              uncoveredIntervalsOverflowed = true;
            }
          }
          startMillis = holderInterval.getEndMillis();
        }

        if (!uncoveredIntervalsOverflowed && startMillis < endMillis) {
          if (uncoveredIntervalsLimit > uncoveredIntervals.size()) {
            uncoveredIntervals.add(Intervals.utc(startMillis, endMillis));
          } else {
            uncoveredIntervalsOverflowed = true;
          }
        }
      }

      if (!uncoveredIntervals.isEmpty()) {
        // Record in the response context the interval for which NO segment is present.
        // Which is not necessarily an indication that the data doesn't exist or is
        // incomplete. The data could exist and just not be loaded yet.  In either
        // case, though, this query will not include any data from the identified intervals.
        responseContext.add(ResponseContext.Key.UNCOVERED_INTERVALS, uncoveredIntervals);
        responseContext.add(ResponseContext.Key.UNCOVERED_INTERVALS_OVERFLOWED, uncoveredIntervalsOverflowed);
      }
    }

    @Nullable
    private byte[] computeQueryCacheKey()
    {
      if ((populateCache || useCache) // implies strategy != null
          && !isBySegment) { // explicit bySegment queries are never cached
        assert strategy != null;
        return strategy.computeCacheKey(query);
      } else {
        return null;
      }
    }

    @Nullable
    private String computeCurrentEtag(final Set<SegmentServerSelector> segments, @Nullable byte[] queryCacheKey)
    {
      Hasher hasher = Hashing.sha1().newHasher();
      boolean hasOnlyHistoricalSegments = true;
      for (SegmentServerSelector p : segments) {
        if (!p.getServer().pick().getServer().isSegmentReplicationTarget()) {
          hasOnlyHistoricalSegments = false;
          break;
        }
        hasher.putString(p.getServer().getSegment().getId().toString(), StandardCharsets.UTF_8);
        // it is important to add the "query interval" as part ETag calculation
        // to have result level cache work correctly for queries with different
        // intervals covering the same set of segments
        hasher.putString(p.rhs.getInterval().toString(), StandardCharsets.UTF_8);
      }

      if (hasOnlyHistoricalSegments) {
        hasher.putBytes(queryCacheKey == null ? strategy.computeCacheKey(query) : queryCacheKey);

        String currEtag = StringUtils.encodeBase64String(hasher.hash().asBytes());
        responseContext.put(ResponseContext.Key.ETAG, currEtag);
        return currEtag;
      } else {
        return null;
      }
    }

    private List<Pair<Interval, byte[]>> pruneSegmentsWithCachedResults(
        final byte[] queryCacheKey,
        final Set<SegmentServerSelector> segments
    )
    {
      if (queryCacheKey == null) {
        return Collections.emptyList();
      }
      final List<Pair<Interval, byte[]>> alreadyCachedResults = new ArrayList<>();
      Map<SegmentServerSelector, Cache.NamedKey> perSegmentCacheKeys = computePerSegmentCacheKeys(
          segments,
          queryCacheKey
      );
      // Pull cached segments from cache and remove from set of segments to query
      final Map<Cache.NamedKey, byte[]> cachedValues = computeCachedValues(perSegmentCacheKeys);

      perSegmentCacheKeys.forEach((segment, segmentCacheKey) -> {
        final Interval segmentQueryInterval = segment.getSegmentDescriptor().getInterval();

        final byte[] cachedValue = cachedValues.get(segmentCacheKey);
        if (cachedValue != null) {
          // remove cached segment from set of segments to query
          segments.remove(segment);
          alreadyCachedResults.add(Pair.of(segmentQueryInterval, cachedValue));
        } else if (populateCache) {
          // otherwise, if populating cache, add segment to list of segments to cache
          final SegmentId segmentId = segment.getServer().getSegment().getId();
          addCachePopulatorKey(segmentCacheKey, segmentId, segmentQueryInterval);
        }
      });
      return alreadyCachedResults;
    }

    private Map<SegmentServerSelector, Cache.NamedKey> computePerSegmentCacheKeys(
        Set<SegmentServerSelector> segments,
        byte[] queryCacheKey
    )
    {
      // cacheKeys map must preserve segment ordering, in order for shards to always be combined in the same order
      Map<SegmentServerSelector, Cache.NamedKey> cacheKeys = Maps.newLinkedHashMap();
      for (SegmentServerSelector segmentServer : segments) {
        final Cache.NamedKey segmentCacheKey = CacheUtil.computeSegmentCacheKey(
            segmentServer.getServer().getSegment().getId().toString(),
            segmentServer.getSegmentDescriptor(),
            queryCacheKey
        );
        cacheKeys.put(segmentServer, segmentCacheKey);
      }
      return cacheKeys;
    }

    private Map<Cache.NamedKey, byte[]> computeCachedValues(Map<SegmentServerSelector, Cache.NamedKey> cacheKeys)
    {
      if (useCache) {
        return cache.getBulk(Iterables.limit(cacheKeys.values(), cacheConfig.getCacheBulkMergeLimit()));
      } else {
        return ImmutableMap.of();
      }
    }

    private void addCachePopulatorKey(
        Cache.NamedKey segmentCacheKey,
        SegmentId segmentId,
        Interval segmentQueryInterval
    )
    {
      cachePopulatorKeyMap.put(StringUtils.format("%s_%s", segmentId, segmentQueryInterval), segmentCacheKey);
    }

    @Nullable
    private Cache.NamedKey getCachePopulatorKey(String segmentId, Interval segmentInterval)
    {
      return cachePopulatorKeyMap.get(StringUtils.format("%s_%s", segmentId, segmentInterval));
    }

    private SortedMap<DruidServer, List<SegmentDescriptor>> groupSegmentsByServer(Set<SegmentServerSelector> segments)
    {
      final SortedMap<DruidServer, List<SegmentDescriptor>> serverSegments = new TreeMap<>();
      for (SegmentServerSelector segmentServer : segments) {
        final QueryableDruidServer queryableDruidServer = segmentServer.getServer().pick();

        if (queryableDruidServer == null) {
          log.makeAlert(
              "No servers found for SegmentDescriptor[%s] for DataSource[%s]?! How can this be?!",
              segmentServer.getSegmentDescriptor(),
              query.getDataSource()
          ).emit();
        } else {
          final DruidServer server = queryableDruidServer.getServer();
          serverSegments.computeIfAbsent(server, s -> new ArrayList<>()).add(segmentServer.getSegmentDescriptor());
        }
      }
      return serverSegments;
    }

    private void addSequencesFromCache(
        final List<Sequence<T>> listOfSequences,
        final List<Pair<Interval, byte[]>> cachedResults
    )
    {
      if (strategy == null) {
        return;
      }

      final Function<Object, T> pullFromCacheFunction = strategy.pullFromSegmentLevelCache();
      final TypeReference<Object> cacheObjectClazz = strategy.getCacheObjectClazz();
      for (Pair<Interval, byte[]> cachedResultPair : cachedResults) {
        final byte[] cachedResult = cachedResultPair.rhs;
        Sequence<Object> cachedSequence = new BaseSequence<>(
            new BaseSequence.IteratorMaker<Object, Iterator<Object>>()
            {
              @Override
              public Iterator<Object> make()
              {
                try {
                  if (cachedResult.length == 0) {
                    return Collections.emptyIterator();
                  }

                  return objectMapper.readValues(
                      objectMapper.getFactory().createParser(cachedResult),
                      cacheObjectClazz
                  );
                }
                catch (IOException e) {
                  throw new RuntimeException(e);
                }
              }

              @Override
              public void cleanup(Iterator<Object> iterFromMake)
              {
              }
            }
        );
        listOfSequences.add(Sequences.map(cachedSequence, pullFromCacheFunction));
      }
    }

    /**
     * Create sequences that reads from remote query servers (historicals and tasks). Note that the broker will
     * hold an HTTP connection per server after this method is called.
     */
    private void addSequencesFromServer(
        final List<Sequence<T>> listOfSequences,
        final SortedMap<DruidServer, List<SegmentDescriptor>> segmentsByServer
    )
    {
      segmentsByServer.forEach((server, segmentsOfServer) -> {
        final QueryRunner serverRunner = serverView.getQueryRunner(server);

        if (serverRunner == null) {
          log.error("Server[%s] doesn't have a query runner", server.getName());
          return;
        }

        // Divide user-provided maxQueuedBytes by the number of servers, and limit each server to that much.
        final long maxQueuedBytes = QueryContexts.getMaxQueuedBytes(query, httpClientConfig.getMaxQueuedBytes());
        final long maxQueuedBytesPerServer = maxQueuedBytes / segmentsByServer.size();
        final Sequence<T> serverResults;

        if (isBySegment) {
          serverResults = getBySegmentServerResults(serverRunner, segmentsOfServer, maxQueuedBytesPerServer);
        } else if (!server.isSegmentReplicationTarget() || !populateCache) {
          serverResults = getSimpleServerResults(serverRunner, segmentsOfServer, maxQueuedBytesPerServer);
        } else {
          serverResults = getAndCacheServerResults(serverRunner, segmentsOfServer, maxQueuedBytesPerServer);
        }
        listOfSequences.add(serverResults);
      });
    }

    @SuppressWarnings("unchecked")
    private Sequence<T> getBySegmentServerResults(
        final QueryRunner serverRunner,
        final List<SegmentDescriptor> segmentsOfServer,
        long maxQueuedBytesPerServer
    )
    {
      Sequence<Result<BySegmentResultValueClass<T>>> resultsBySegments = serverRunner
          .run(
              queryPlus.withQuery(
                  Queries.withSpecificSegments(queryPlus.getQuery(), segmentsOfServer)
              ).withMaxQueuedBytes(maxQueuedBytesPerServer),
              responseContext
          );
      // bySegment results need to be de-serialized, see DirectDruidClient.run()
      return (Sequence<T>) resultsBySegments
          .map(result -> result.map(
              resultsOfSegment -> resultsOfSegment.mapResults(
                  toolChest.makePreComputeManipulatorFn(query, MetricManipulatorFns.deserializing())::apply
              )
          ));
    }

    @SuppressWarnings("unchecked")
    private Sequence<T> getSimpleServerResults(
        final QueryRunner serverRunner,
        final List<SegmentDescriptor> segmentsOfServer,
        long maxQueuedBytesPerServer
    )
    {
      return serverRunner.run(
          queryPlus.withQuery(
              Queries.withSpecificSegments(queryPlus.getQuery(), segmentsOfServer)
          ).withMaxQueuedBytes(maxQueuedBytesPerServer),
          responseContext
      );
    }

    private Sequence<T> getAndCacheServerResults(
        final QueryRunner serverRunner,
        final List<SegmentDescriptor> segmentsOfServer,
        long maxQueuedBytesPerServer
    )
    {
      @SuppressWarnings("unchecked")
      final Query<T> downstreamQuery = query.withOverriddenContext(makeDownstreamQueryContext());
      final Sequence<Result<BySegmentResultValueClass<T>>> resultsBySegments = serverRunner.run(
          queryPlus
              .withQuery(
                  Queries.withSpecificSegments(
                      downstreamQuery,
                      segmentsOfServer
                  )
              )
              .withMaxQueuedBytes(maxQueuedBytesPerServer),
          responseContext
      );
      final Function<T, Object> cacheFn = strategy.prepareForSegmentLevelCache();

      return resultsBySegments
          .map(result -> {
            final BySegmentResultValueClass<T> resultsOfSegment = result.getValue();
            final Cache.NamedKey cachePopulatorKey =
                getCachePopulatorKey(resultsOfSegment.getSegmentId(), resultsOfSegment.getInterval());
            Sequence<T> res = Sequences.simple(resultsOfSegment.getResults());
            if (cachePopulatorKey != null) {
              res = cachePopulator.wrap(res, cacheFn::apply, cache, cachePopulatorKey);
            }
            return res.map(
                toolChest.makePreComputeManipulatorFn(downstreamQuery, MetricManipulatorFns.deserializing())::apply
            );
          })
          .flatMerge(seq -> seq, query.getResultOrdering());
    }
  }
}
