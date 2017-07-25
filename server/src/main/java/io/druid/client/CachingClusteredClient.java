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

package io.druid.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.RangeSet;
import com.google.common.collect.Sets;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import io.druid.client.cache.Cache;
import io.druid.client.cache.CacheConfig;
import io.druid.client.selector.QueryableDruidServer;
import io.druid.client.selector.ServerSelector;
import io.druid.concurrent.Execs;
import io.druid.guice.annotations.BackgroundCaching;
import io.druid.guice.annotations.Smile;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.guava.BaseSequence;
import io.druid.java.util.common.guava.LazySequence;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.BySegmentResultValueClass;
import io.druid.query.CacheStrategy;
import io.druid.query.Query;
import io.druid.query.QueryContexts;
import io.druid.query.QueryPlus;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.Result;
import io.druid.query.SegmentDescriptor;
import io.druid.query.aggregation.MetricManipulatorFns;
import io.druid.query.filter.DimFilterUtils;
import io.druid.query.spec.MultipleSpecificSegmentSpec;
import io.druid.server.QueryResource;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineLookup;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.PartitionChunk;
import io.druid.timeline.partition.PartitionHolder;
import org.apache.commons.codec.binary.Base64;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

/**
 */
public class CachingClusteredClient implements QuerySegmentWalker
{
  private static final EmittingLogger log = new EmittingLogger(CachingClusteredClient.class);
  private final QueryToolChestWarehouse warehouse;
  private final TimelineServerView serverView;
  private final Cache cache;
  private final ObjectMapper objectMapper;
  private final CacheConfig cacheConfig;
  private final ListeningExecutorService backgroundExecutorService;

  @Inject
  public CachingClusteredClient(
      QueryToolChestWarehouse warehouse,
      TimelineServerView serverView,
      Cache cache,
      @Smile ObjectMapper objectMapper,
      @BackgroundCaching ExecutorService backgroundExecutorService,
      CacheConfig cacheConfig
  )
  {
    this.warehouse = warehouse;
    this.serverView = serverView;
    this.cache = cache;
    this.objectMapper = objectMapper;
    this.cacheConfig = cacheConfig;
    this.backgroundExecutorService = MoreExecutors.listeningDecorator(backgroundExecutorService);

    if (cacheConfig.isQueryCacheable(Query.GROUP_BY)) {
      log.warn(
          "Even though groupBy caching is enabled, v2 groupBys will not be cached. "
          + "Consider disabling cache on your broker and enabling it on your data nodes to enable v2 groupBy caching."
      );
    }

    serverView.registerSegmentCallback(
        Execs.singleThreaded("CCClient-ServerView-CB-%d"),
        new ServerView.BaseSegmentCallback()
        {
          @Override
          public ServerView.CallbackAction segmentRemoved(DruidServerMetadata server, DataSegment segment)
          {
            CachingClusteredClient.this.cache.close(segment.getIdentifier());
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
      public Sequence<T> run(final QueryPlus<T> queryPlus, final Map<String, Object> responseContext)
      {
        return CachingClusteredClient.this.run(queryPlus, responseContext, timeline -> timeline);
      }
    };
  }

  /**
   * Run a query. The timelineConverter will be given the "master" timeline and can be used to return a different
   * timeline, if desired. This is used by getQueryRunnerForSegments.
   */
  private <T> Sequence<T> run(
      final QueryPlus<T> queryPlus,
      final Map<String, Object> responseContext,
      final UnaryOperator<TimelineLookup<String, ServerSelector>> timelineConverter
  )
  {
    return new SpecificQueryRunnable<>(queryPlus, responseContext).run(timelineConverter);
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(final Query<T> query, final Iterable<SegmentDescriptor> specs)
  {
    return new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(final QueryPlus<T> queryPlus, final Map<String, Object> responseContext)
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
            }
        );
      }
    };
  }

  /**
   * This class essentially incapsulates the major part of the logic of {@link CachingClusteredClient}. It's state and
   * methods couldn't belong to {@link CachingClusteredClient} itself, because they depend on the specific query object
   * being run, but {@link QuerySegmentWalker} API is designed so that implementations should be able to accept
   * arbitrary queries.
   */
  private class SpecificQueryRunnable<T>
  {
    private final QueryPlus<T> queryPlus;
    private final Map<String, Object> responseContext;
    private final Query<T> query;
    private final QueryToolChest<T, Query<T>> toolChest;
    @Nullable
    private final CacheStrategy<T, Object, Query<T>> strategy;
    private final boolean useCache;
    private final boolean populateCache;
    private final boolean isBySegment;
    private final int uncoveredIntervalsLimit;
    private final Query<T> downstreamQuery;
    private final Map<String, CachePopulator> cachePopulatorMap = Maps.newHashMap();

    SpecificQueryRunnable(final QueryPlus<T> queryPlus, final Map<String, Object> responseContext)
    {
      this.queryPlus = queryPlus;
      this.responseContext = responseContext;
      this.query = queryPlus.getQuery();
      this.toolChest = warehouse.getToolChest(query);
      this.strategy = toolChest.getCacheStrategy(query);

      this.useCache = CacheUtil.useCacheOnBrokers(query, strategy, cacheConfig);
      this.populateCache = CacheUtil.populateCacheOnBrokers(query, strategy, cacheConfig);
      this.isBySegment = QueryContexts.isBySegment(query);
      // Note that enabling this leads to putting uncovered intervals information in the response headers
      // and might blow up in some cases https://github.com/druid-io/druid/issues/2108
      this.uncoveredIntervalsLimit = QueryContexts.getUncoveredIntervalsLimit(query);
      this.downstreamQuery = query.withOverriddenContext(makeDownstreamQueryContext());
    }

    private ImmutableMap<String, Object> makeDownstreamQueryContext()
    {
      final ImmutableMap.Builder<String, Object> contextBuilder = new ImmutableMap.Builder<>();

      final int priority = QueryContexts.getPriority(query);
      contextBuilder.put(QueryContexts.PRIORITY_KEY, priority);

      if (populateCache) {
        // prevent down-stream nodes from caching results as well if we are populating the cache
        contextBuilder.put(CacheConfig.POPULATE_CACHE, false);
        contextBuilder.put("bySegment", true);
      }
      return contextBuilder.build();
    }

    Sequence<T> run(final UnaryOperator<TimelineLookup<String, ServerSelector>> timelineConverter)
    {
      @Nullable TimelineLookup<String, ServerSelector> timeline = serverView.getTimeline(query.getDataSource());
      if (timeline == null) {
        return Sequences.empty();
      }
      timeline = timelineConverter.apply(timeline);
      if (uncoveredIntervalsLimit > 0) {
        computeUncoveredIntervals(timeline);
      }

      final Set<ServerToSegment> segments = computeSegmentsToQuery(timeline);
      @Nullable final byte[] queryCacheKey = computeQueryCacheKey();
      if (query.getContext().get(QueryResource.HEADER_IF_NONE_MATCH) != null) {
        @Nullable final String prevEtag = (String) query.getContext().get(QueryResource.HEADER_IF_NONE_MATCH);
        @Nullable final String currentEtag = computeCurrentEtag(segments, queryCacheKey);
        if (currentEtag != null && currentEtag.equals(prevEtag)) {
          return Sequences.empty();
        }
      }

      final List<Pair<Interval, byte[]>> alreadyCachedResults = pruneSegmentsWithCachedResults(queryCacheKey, segments);
      final SortedMap<DruidServer, List<SegmentDescriptor>> segmentsByServer = groupSegmentsByServer(segments);
      return new LazySequence<>(() -> {
        List<Sequence<T>> sequencesByInterval = new ArrayList<>(alreadyCachedResults.size() + segmentsByServer.size());
        addSequencesFromCache(sequencesByInterval, alreadyCachedResults);
        addSequencesFromServer(sequencesByInterval, segmentsByServer);
        return Sequences
            .simple(sequencesByInterval)
            .flatMerge(seq -> seq, query.getResultOrdering());
      });
    }

    private Set<ServerToSegment> computeSegmentsToQuery(TimelineLookup<String, ServerSelector> timeline)
    {
      final List<TimelineObjectHolder<String, ServerSelector>> serversLookup = toolChest.filterSegments(
          query,
          query.getIntervals().stream().flatMap(i -> timeline.lookup(i).stream()).collect(Collectors.toList())
      );

      final Set<ServerToSegment> segments = Sets.newLinkedHashSet();
      final Map<String, Optional<RangeSet<String>>> dimensionRangeCache = Maps.newHashMap();
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
          segments.add(new ServerToSegment(server, segment));
        }
      }
      return segments;
    }

    private void computeUncoveredIntervals(TimelineLookup<String, ServerSelector> timeline)
    {
      final List<Interval> uncoveredIntervals = new ArrayList<>(uncoveredIntervalsLimit);
      boolean uncoveredIntervalsOverflowed = false;

      for (Interval interval : query.getIntervals()) {
        Iterable<TimelineObjectHolder<String, ServerSelector>> lookup = timeline.lookup(interval);
        long startMillis = interval.getStartMillis();
        long endMillis = interval.getEndMillis();
        for (TimelineObjectHolder<String, ServerSelector> holder : lookup) {
          Interval holderInterval = holder.getInterval();
          long intervalStart = holderInterval.getStartMillis();
          if (!uncoveredIntervalsOverflowed && startMillis != intervalStart) {
            if (uncoveredIntervalsLimit > uncoveredIntervals.size()) {
              uncoveredIntervals.add(new Interval(startMillis, intervalStart));
            } else {
              uncoveredIntervalsOverflowed = true;
            }
          }
          startMillis = holderInterval.getEndMillis();
        }

        if (!uncoveredIntervalsOverflowed && startMillis < endMillis) {
          if (uncoveredIntervalsLimit > uncoveredIntervals.size()) {
            uncoveredIntervals.add(new Interval(startMillis, endMillis));
          } else {
            uncoveredIntervalsOverflowed = true;
          }
        }
      }

      if (!uncoveredIntervals.isEmpty()) {
        // This returns intervals for which NO segment is present.
        // Which is not necessarily an indication that the data doesn't exist or is
        // incomplete. The data could exist and just not be loaded yet.  In either
        // case, though, this query will not include any data from the identified intervals.
        responseContext.put("uncoveredIntervals", uncoveredIntervals);
        responseContext.put("uncoveredIntervalsOverflowed", uncoveredIntervalsOverflowed);
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
    private String computeCurrentEtag(final Set<ServerToSegment> segments, @Nullable byte[] queryCacheKey)
    {
      Hasher hasher = Hashing.sha1().newHasher();
      boolean hasOnlyHistoricalSegments = true;
      for (ServerToSegment p : segments) {
        if (!p.getServer().pick().getServer().segmentReplicatable()) {
          hasOnlyHistoricalSegments = false;
          break;
        }
        hasher.putString(p.getServer().getSegment().getIdentifier(), Charsets.UTF_8);
      }

      if (hasOnlyHistoricalSegments) {
        hasher.putBytes(queryCacheKey == null ? strategy.computeCacheKey(query) : queryCacheKey);

        String currEtag = Base64.encodeBase64String(hasher.hash().asBytes());
        responseContext.put(QueryResource.HEADER_ETAG, currEtag);
        return currEtag;
      } else {
        return null;
      }
    }

    private List<Pair<Interval, byte[]>> pruneSegmentsWithCachedResults(
        final byte[] queryCacheKey,
        final Set<ServerToSegment> segments
    )
    {
      if (queryCacheKey == null) {
        return Collections.emptyList();
      }
      final List<Pair<Interval, byte[]>> alreadyCachedResults = Lists.newArrayList();
      Map<ServerToSegment, Cache.NamedKey> perSegmentCacheKeys = computePerSegmentCacheKeys(segments, queryCacheKey);
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
          final String segmentIdentifier = segment.getServer().getSegment().getIdentifier();
          addCachePopulator(segmentCacheKey, segmentIdentifier, segmentQueryInterval);
        }
      });
      return alreadyCachedResults;
    }

    private Map<ServerToSegment, Cache.NamedKey> computePerSegmentCacheKeys(
        Set<ServerToSegment> segments,
        byte[] queryCacheKey
    )
    {
      // cacheKeys map must preserve segment ordering, in order for shards to always be combined in the same order
      Map<ServerToSegment, Cache.NamedKey> cacheKeys = Maps.newLinkedHashMap();
      for (ServerToSegment serverToSegment : segments) {
        final Cache.NamedKey segmentCacheKey = CacheUtil.computeSegmentCacheKey(
            serverToSegment.getServer().getSegment().getIdentifier(),
            serverToSegment.getSegmentDescriptor(),
            queryCacheKey
        );
        cacheKeys.put(serverToSegment, segmentCacheKey);
      }
      return cacheKeys;
    }

    private Map<Cache.NamedKey, byte[]> computeCachedValues(Map<ServerToSegment, Cache.NamedKey> cacheKeys)
    {
      if (useCache) {
        return cache.getBulk(Iterables.limit(cacheKeys.values(), cacheConfig.getCacheBulkMergeLimit()));
      } else {
        return ImmutableMap.of();
      }
    }

    private void addCachePopulator(
        Cache.NamedKey segmentCacheKey,
        String segmentIdentifier,
        Interval segmentQueryInterval
    )
    {
      cachePopulatorMap.put(
          StringUtils.format("%s_%s", segmentIdentifier, segmentQueryInterval),
          new CachePopulator(cache, objectMapper, segmentCacheKey)
      );
    }

    @Nullable
    private CachePopulator getCachePopulator(String segmentId, Interval segmentInterval)
    {
      return cachePopulatorMap.get(StringUtils.format("%s_%s", segmentId, segmentInterval));
    }

    private SortedMap<DruidServer, List<SegmentDescriptor>> groupSegmentsByServer(Set<ServerToSegment> segments)
    {
      final SortedMap<DruidServer, List<SegmentDescriptor>> serverSegments = Maps.newTreeMap();
      for (ServerToSegment serverToSegment : segments) {
        final QueryableDruidServer queryableDruidServer = serverToSegment.getServer().pick();

        if (queryableDruidServer == null) {
          log.makeAlert(
              "No servers found for SegmentDescriptor[%s] for DataSource[%s]?! How can this be?!",
              serverToSegment.getSegmentDescriptor(),
              query.getDataSource()
          ).emit();
        } else {
          final DruidServer server = queryableDruidServer.getServer();
          serverSegments.computeIfAbsent(server, s -> new ArrayList<>()).add(serverToSegment.getSegmentDescriptor());
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

      final Function<Object, T> pullFromCacheFunction = strategy.pullFromCache();
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
                    return Iterators.emptyIterator();
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

    private void addSequencesFromServer(
        final List<Sequence<T>> listOfSequences,
        final SortedMap<DruidServer, List<SegmentDescriptor>> segmentsByServer
    )
    {
      segmentsByServer.forEach((server, segmentsOfServer) -> {
        final QueryRunner serverRunner = serverView.getQueryRunner(server);

        if (serverRunner == null) {
          log.error("Server[%s] doesn't have a query runner", server);
          return;
        }

        final MultipleSpecificSegmentSpec segmentsOfServerSpec = new MultipleSpecificSegmentSpec(segmentsOfServer);

        Sequence<T> serverResults;
        if (isBySegment) {
          serverResults = getBySegmentServerResults(serverRunner, segmentsOfServerSpec);
        } else if (!server.segmentReplicatable() || !populateCache) {
          serverResults = getSimpleServerResults(serverRunner, segmentsOfServerSpec);
        } else {
          serverResults = getAndCacheServerResults(serverRunner, segmentsOfServerSpec);
        }
        listOfSequences.add(serverResults);
      });
    }

    @SuppressWarnings("unchecked")
    private Sequence<T> getBySegmentServerResults(
        final QueryRunner serverRunner,
        final MultipleSpecificSegmentSpec segmentsOfServerSpec
    )
    {
      Sequence<Result<BySegmentResultValueClass<T>>> resultsBySegments = serverRunner
          .run(queryPlus.withQuerySegmentSpec(segmentsOfServerSpec), responseContext);
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
        final MultipleSpecificSegmentSpec segmentsOfServerSpec
    )
    {
      return serverRunner.run(queryPlus.withQuerySegmentSpec(segmentsOfServerSpec), responseContext);
    }

    private Sequence<T> getAndCacheServerResults(
        final QueryRunner serverRunner,
        final MultipleSpecificSegmentSpec segmentsOfServerSpec
    )
    {
      @SuppressWarnings("unchecked")
      final Sequence<Result<BySegmentResultValueClass<T>>> resultsBySegments = serverRunner.run(
          queryPlus
              .withQuery((Query<Result<BySegmentResultValueClass<T>>>) downstreamQuery)
              .withQuerySegmentSpec(segmentsOfServerSpec),
          responseContext
      );
      final Function<T, Object> cacheFn = strategy.prepareForCache();
      return resultsBySegments
          .map(result -> {
            final BySegmentResultValueClass<T> resultsOfSegment = result.getValue();
            final CachePopulator cachePopulator =
                getCachePopulator(resultsOfSegment.getSegmentId(), resultsOfSegment.getInterval());
            Sequence<T> res = Sequences
                .simple(resultsOfSegment.getResults())
                .map(r -> {
                  if (cachePopulator != null) {
                    // only compute cache data if populating cache
                    cachePopulator.cacheFutures.add(backgroundExecutorService.submit(() -> cacheFn.apply(r)));
                  }
                  return r;
                })
                .map(
                    toolChest.makePreComputeManipulatorFn(downstreamQuery, MetricManipulatorFns.deserializing())::apply
                );
            if (cachePopulator != null) {
              res = res.withEffect(cachePopulator::populate, MoreExecutors.sameThreadExecutor());
            }
            return res;
          })
          .flatMerge(seq -> seq, query.getResultOrdering());
    }
  }

  private static class ServerToSegment extends Pair<ServerSelector, SegmentDescriptor>
  {
    private ServerToSegment(ServerSelector server, SegmentDescriptor segment)
    {
      super(server, segment);
    }

    ServerSelector getServer()
    {
      return lhs;
    }

    SegmentDescriptor getSegmentDescriptor()
    {
      return rhs;
    }
  }

  private class CachePopulator
  {
    private final Cache cache;
    private final ObjectMapper mapper;
    private final Cache.NamedKey key;
    private final ConcurrentLinkedQueue<ListenableFuture<Object>> cacheFutures = new ConcurrentLinkedQueue<>();

    CachePopulator(Cache cache, ObjectMapper mapper, Cache.NamedKey key)
    {
      this.cache = cache;
      this.mapper = mapper;
      this.key = key;
    }

    public void populate()
    {
      Futures.addCallback(
          Futures.allAsList(cacheFutures),
          new FutureCallback<List<Object>>()
          {
            @Override
            public void onSuccess(List<Object> cacheData)
            {
              CacheUtil.populate(cache, mapper, key, cacheData);
              // Help out GC by making sure all references are gone
              cacheFutures.clear();
            }

            @Override
            public void onFailure(Throwable throwable)
            {
              log.error(throwable, "Background caching failed");
            }
          },
          backgroundExecutorService
      );
    }
  }
}
