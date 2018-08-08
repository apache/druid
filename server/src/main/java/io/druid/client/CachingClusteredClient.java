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

package io.druid.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.inject.Inject;
import io.druid.client.cache.Cache;
import io.druid.client.cache.CacheConfig;
import io.druid.client.cache.CachePopulator;
import io.druid.client.selector.QueryableDruidServer;
import io.druid.client.selector.ServerSelector;
import io.druid.collections.SerializablePair;
import io.druid.guice.annotations.Processing;
import io.druid.guice.annotations.Smile;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.concurrent.Execs;
import io.druid.java.util.common.guava.MergeSequence;
import io.druid.java.util.common.guava.MergeWorkTask;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.query.BySegmentResultValueClass;
import io.druid.query.CacheStrategy;
import io.druid.query.FluentQueryRunnerBuilder;
import io.druid.query.Query;
import io.druid.query.QueryContexts;
import io.druid.query.QueryPlus;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.Result;
import io.druid.query.SegmentDescriptor;
import io.druid.query.aggregation.MetricManipulatorFns;
import io.druid.query.filter.DimFilterUtils;
import io.druid.query.spec.MultipleSpecificSegmentSpec;
import io.druid.server.DruidNode;
import io.druid.server.QueryResource;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.coordination.ServerType;
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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Spliterators;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 */
public class CachingClusteredClient implements QuerySegmentWalker
{
  private static final EmittingLogger log = new EmittingLogger(CachingClusteredClient.class);
  private static final DruidServer ALREADY_CACHED_SERVER = new DruidServer(
      new DruidNode(
          "__internal-client-cache",
          "localhost",
          -1,
          -1,
          true,
          false
      ),
      new DruidServerConfig(),
      ServerType.HISTORICAL
  );
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final QueryToolChestWarehouse warehouse;
  private final TimelineServerView serverView;
  private final Cache cache;
  private final ObjectMapper objectMapper;
  private final CachePopulator cachePopulator;
  private final CacheConfig cacheConfig;
  private final ForkJoinPool mergeFjp;

  @Inject
  public CachingClusteredClient(
      QueryRunnerFactoryConglomerate conglomerate,
      QueryToolChestWarehouse warehouse,
      TimelineServerView serverView,
      Cache cache,
      @Smile ObjectMapper objectMapper,
      @Processing ForkJoinPool mergeFjp,
      CachePopulator cachePopulator,
      CacheConfig cacheConfig
  )
  {
    this.conglomerate = conglomerate;
    this.warehouse = warehouse;
    this.serverView = serverView;
    this.cache = cache;
    this.objectMapper = objectMapper;
    this.cachePopulator = cachePopulator;
    this.cacheConfig = cacheConfig;
    this.mergeFjp = mergeFjp;

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
            CachingClusteredClient.this.cache.close(segment.getIdentifier());
            return ServerView.CallbackAction.CONTINUE;
          }
        }
    );
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(final Query<T> query, final Iterable<Interval> intervals)
  {
    return runAndMergeWithTimelineChange(
        query,
        // No change, but Function.identity() doesn't work here for some reason
        stringServerSelectorTimelineLookup -> stringServerSelectorTimelineLookup
    );
  }

  /**
   * Run a query. The timelineConverter will be given the "master" timeline and can be used to return a different
   * timeline, if desired. This is used by getQueryRunnerForSegments.
   */
  @VisibleForTesting
  <T> Stream<Sequence<T>> run(
      final QueryPlus<T> queryPlus,
      final Map<String, Object> responseContext,
      final UnaryOperator<TimelineLookup<String, ServerSelector>> timelineConverter
  )
  {
    return new SpecificQueryRunnable<>(queryPlus, responseContext).run(timelineConverter);
  }

  private <T> QueryRunner<T> runAndMergeWithTimelineChange(
      final Query<T> query,
      final UnaryOperator<TimelineLookup<String, ServerSelector>> timelineConverter
  )
  {
    final OptionalLong mergeBatch = QueryContexts.getIntermediateMergeBatchThreshold(query);

    if (mergeBatch.isPresent()) {
      final QueryRunnerFactory<T, Query<T>> queryRunnerFactory = conglomerate.findFactory(query);
      final QueryToolChest<T, Query<T>> toolChest = queryRunnerFactory.getToolchest();
      return (queryPlus, responseContext) -> {
        final Stream<? extends Sequence<T>> sequences = run(
            queryPlus,
            responseContext,
            timelineConverter
        );
        return MergeWorkTask.parallelMerge(
            sequences.parallel(),
            sequenceStream ->
                new FluentQueryRunnerBuilder<>(toolChest)
                    .create(
                        queryRunnerFactory.mergeRunners(
                            mergeFjp,
                            sequenceStream.map(
                                s -> (QueryRunner<T>) (ignored0, ignored1) -> (Sequence<T>) s
                            ).collect(
                                Collectors.toList()
                            )
                        )
                    )
                    .mergeResults()
                    .run(queryPlus, responseContext),
            mergeBatch.getAsLong(),
            mergeFjp
        );
      };
    } else {
      return (queryPlus, responseContext) -> {
        final Stream<? extends Sequence<T>> sequences = run(
            queryPlus,
            responseContext,
            timelineConverter
        );
        return new MergeSequence<>(
            query.getResultOrdering(),
            Sequences.fromStream(sequences)
        );
      };
    }
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(final Query<T> query, final Iterable<SegmentDescriptor> specs)
  {
    return runAndMergeWithTimelineChange(
        query,
        timeline -> {
          final VersionedIntervalTimeline<String, ServerSelector> timeline2 =
              new VersionedIntervalTimeline<>(Ordering.natural());
          for (SegmentDescriptor spec : specs) {
            final PartitionHolder<ServerSelector> entry = timeline.findEntry(
                spec.getInterval(),
                spec.getVersion()
            );
            if (entry != null) {
              final PartitionChunk<ServerSelector> chunk = entry.getChunk(
                  spec.getPartitionNumber());
              if (chunk != null) {
                timeline2.add(
                    spec.getInterval(),
                    spec.getVersion(),
                    chunk
                );
              }
            }
          }
          return timeline2;
        }
    );
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
    private final Map<String, Cache.NamedKey> cachePopulatorKeyMap = Maps.newHashMap();

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

    private Map<String, Object> makeDownstreamQueryContext()
    {
      final Map<String, Object> contextBuilder = new LinkedHashMap<>();

      final int priority = QueryContexts.getPriority(query);
      contextBuilder.put(QueryContexts.PRIORITY_KEY, priority);

      if (populateCache) {
        // prevent down-stream nodes from caching results as well if we are populating the cache
        contextBuilder.put(CacheConfig.POPULATE_CACHE, false);
        contextBuilder.put("bySegment", true);
      }
      return Collections.unmodifiableMap(contextBuilder);
    }

    Stream<Sequence<T>> run(final UnaryOperator<TimelineLookup<String, ServerSelector>> timelineConverter)
    {
      @Nullable
      TimelineLookup<String, ServerSelector> timeline = serverView.getTimeline(query.getDataSource());
      if (timeline == null) {
        return Stream.empty();
      }
      timeline = timelineConverter.apply(timeline);
      if (uncoveredIntervalsLimit > 0) {
        computeUncoveredIntervals(timeline);
      }

      Stream<ServerToSegment> segments = computeSegmentsToQuery(timeline);
      @Nullable
      final byte[] queryCacheKey = computeQueryCacheKey();
      if (query.getContext().get(QueryResource.HEADER_IF_NONE_MATCH) != null) {
        // Materialize then re-stream
        List<ServerToSegment> materializedSegments = segments.collect(Collectors.toList());
        segments = materializedSegments.stream();

        @Nullable
        final String prevEtag = (String) query.getContext().get(QueryResource.HEADER_IF_NONE_MATCH);
        @Nullable
        final String currentEtag = computeCurrentEtag(materializedSegments, queryCacheKey);
        if (currentEtag != null && currentEtag.equals(prevEtag)) {
          return Stream.empty();
        }
      }

      // This pipeline follows a few general steps:
      // 1. Fetch cache results - Unfortunately this is an eager operation so that the non cached items can
      // be batched per server. Cached results are assigned to a mock server ALREADY_CACHED_SERVER
      // 2. Group the segment information by server
      // 3. Per server (including the ALREADY_CACHED_SERVER) create the appropriate Sequence results - cached results
      // are handled in their own merge
      final Stream<SerializablePair<ServerToSegment, Optional<T>>> cacheResolvedResults = deserializeFromCache(
          maybeFetchCacheResults(
              queryCacheKey,
              segments
          )
      );
      return groupCachedResultsByServer(cacheResolvedResults)
          .map(this::runOnServer)
          // We do a hard materialization here so that the resulting spliterators have properties that we want
          // Otherwise the stream's spliterator is of a hash map entry spliterator from the group-by-server operation
          // This also causes eager initialization of the **sequences**, aka forking off the direct druid client requests
          // Sequence result accumulation should still be lazy
          .collect(Collectors.toList())
          .stream();
    }

    /**
     * Create a stream of the partition chunks which are useful in this query
     *
     * @param holder The holder of the shard to server component of the timeline
     *
     * @return Chunks and the segment descriptors corresponding to the chunk
     */
    private Stream<ServerToSegment> extractServerAndSegment(TimelineObjectHolder<String, ServerSelector> holder)
    {
      return DimFilterUtils
          .filterShards(
              query.getFilter(),
              holder.getObject(),
              partitionChunk -> partitionChunk.getObject().getSegment().getShardSpec(),
              Maps.newHashMap()
          )
          .stream()
          .map(chunk -> new ServerToSegment(
              chunk.getObject(),
              new SegmentDescriptor(
                  holder.getInterval(),
                  holder.getVersion(),
                  chunk.getChunkNumber()
              )
          ));
    }

    private Stream<ServerToSegment> computeSegmentsToQuery(TimelineLookup<String, ServerSelector> timeline)
    {
      return toolChest
          .filterSegments(
              query,
              query.getIntervals().stream().flatMap(i -> timeline.lookup(i).stream()).collect(Collectors.toList())
          )
          .stream()
          .flatMap(this::extractServerAndSegment)
          .distinct();
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
    private String computeCurrentEtag(final Iterable<ServerToSegment> segments, @Nullable byte[] queryCacheKey)
    {
      Hasher hasher = Hashing.sha1().newHasher();
      boolean hasOnlyHistoricalSegments = true;
      for (ServerToSegment p : segments) {
        if (!p.getServer().pick().getServer().segmentReplicatable()) {
          hasOnlyHistoricalSegments = false;
          break;
        }
        hasher.putString(p.getServer().getSegment().getIdentifier(), StandardCharsets.UTF_8);
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

    private SerializablePair<ServerToSegment, Optional<byte[]>> lookupInCache(
        SerializablePair<ServerToSegment, Cache.NamedKey> key,
        Map<Cache.NamedKey, Optional<byte[]>> cache
    )
    {
      final ServerToSegment segment = key.getLhs();
      final Cache.NamedKey segmentCacheKey = key.getRhs();
      final Interval segmentQueryInterval = segment.getSegmentDescriptor().getInterval();
      final Optional<byte[]> cachedValue = Optional
          .ofNullable(cache.get(segmentCacheKey))
          // Shouldn't happen in practice, but can screw up unit tests where cache state is mutated in crazy
          // ways when the cache returns null instead of an optional.
          .orElse(Optional.empty());
      if (!cachedValue.isPresent()) {
        // if populating cache, add segment to list of segments to cache if it is not cached
        final String segmentIdentifier = segment.getServer().getSegment().getIdentifier();
        addCachePopulatorKey(segmentCacheKey, segmentIdentifier, segmentQueryInterval);
      }
      return new SerializablePair<>(segment, cachedValue);
    }

    /**
     * This materializes the input segment stream in order to let the BulkGet stuff in the cache system work
     *
     * @param queryCacheKey The cache key that is for the query (not-segment) portion
     * @param segments      The segments to check if they are in cache
     *
     * @return A stream of the server and segment combinations as well as an optional that is present
     * if a cached value was found
     */
    private Stream<SerializablePair<ServerToSegment, Optional<byte[]>>> maybeFetchCacheResults(
        final byte[] queryCacheKey,
        final Stream<ServerToSegment> segments
    )
    {
      if (queryCacheKey == null) {
        return segments.map(s -> new SerializablePair<>(s, Optional.empty()));
      }
      // We materialize the stream here in order to have the bulk cache fetching work as expected
      final List<SerializablePair<ServerToSegment, Cache.NamedKey>> materializedKeyList = computePerSegmentCacheKeys(
          segments,
          queryCacheKey
      ).collect(Collectors.toList());

      // Do bulk fetch
      final Map<Cache.NamedKey, Optional<byte[]>> cachedValues = computeCachedValues(materializedKeyList.stream())
          .collect(SerializablePair.mapCollector());

      // A limitation of the cache system is that the cached values are returned without passing through the original
      // objects. This hash join is a way to get the ServerToSegment and Optional<byte[]> matched up again
      return materializedKeyList
          .stream()
          .map(serializedPairSegmentAndKey -> lookupInCache(serializedPairSegmentAndKey, cachedValues));
    }

    private Stream<SerializablePair<ServerToSegment, Cache.NamedKey>> computePerSegmentCacheKeys(
        Stream<ServerToSegment> segments,
        byte[] queryCacheKey
    )
    {
      return segments.map(
          serverToSegment -> {
            // cacheKeys map must preserve segment ordering, in order for shards to always be combined in the same order
            final Cache.NamedKey segmentCacheKey = CacheUtil.computeSegmentCacheKey(
                serverToSegment.getServer().getSegment().getIdentifier(),
                serverToSegment.getSegmentDescriptor(),
                queryCacheKey
            );
            return new SerializablePair<>(serverToSegment, segmentCacheKey);
          }
      );
    }

    private Stream<SerializablePair<Cache.NamedKey, Optional<byte[]>>> computeCachedValues(
        Stream<SerializablePair<ServerToSegment, Cache.NamedKey>> cacheKeys
    )
    {
      if (useCache) {
        return cache.getBulk(cacheKeys.limit(cacheConfig.getCacheBulkMergeLimit()).map(SerializablePair::getRhs));
      } else {
        return Stream.empty();
      }
    }

    private String cacheKey(String segmentId, Interval segmentInterval)
    {
      return StringUtils.format("%s_%s", segmentId, segmentInterval);
    }

    private void addCachePopulatorKey(
        Cache.NamedKey segmentCacheKey,
        String segmentIdentifier,
        Interval segmentQueryInterval
    )
    {
      cachePopulatorKeyMap.put(
          cacheKey(segmentIdentifier, segmentQueryInterval),
          segmentCacheKey
      );
    }

    @Nullable
    private Cache.NamedKey getCachePopulatorKey(String segmentId, Interval segmentInterval)
    {
      return cachePopulatorKeyMap.get(cacheKey(segmentId, segmentInterval));
    }

    /**
     * Check the input stream to see what was cached and what was not. For the ones that were cached, merge the results
     * and return the merged sequence. For the ones that were NOT cached, get the server result sequence queued up into
     * the stream response
     *
     * @param segmentOrResult A list that is traversed in order to determine what should be sent back. All segments
     *                        should be on the same server.
     *
     * @return A sequence of either the merged cached results, or the server results from any particular server
     */
    private Sequence<T> runOnServer(List<ServerMaybeSegmentMaybeCache<T>> segmentOrResult)
    {
      final List<SegmentDescriptor> segmentsOfServer = segmentOrResult
          .stream()
          .map(ServerMaybeSegmentMaybeCache::getSegmentDescriptor)
          .filter(Optional::isPresent)
          .map(Optional::get)
          .collect(Collectors.toList());

      // We should only ever have cache or queries to run, not both. So if we have no segments, try caches
      if (segmentsOfServer.isEmpty()) {
        // Have a special sequence for the cache results so the merge doesn't go all crazy.
        // See io.druid.java.util.common.guava.MergeSequenceTest.testScrewsUpOnOutOfOrder for an example
        // With zero results actually being found (no segments no caches) this should essentially return a no-op
        // merge sequence
        return new MergeSequence<>(query.getResultOrdering(), Sequences.fromStream(
            segmentOrResult
                .stream()
                .map(ServerMaybeSegmentMaybeCache::getCachedValue)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(Collections::singletonList)
                .map(Sequences::simple)
        ));
      }

      final DruidServer server = segmentOrResult.get(0).getServer();
      final QueryRunner serverRunner = serverView.getQueryRunner(server);

      if (serverRunner == null) {
        log.error("Server[%s] doesn't have a query runner", server);
        return Sequences.empty();
      }

      final MultipleSpecificSegmentSpec segmentsOfServerSpec = new MultipleSpecificSegmentSpec(segmentsOfServer);

      final Sequence<T> serverResults;
      if (isBySegment) {
        serverResults = getBySegmentServerResults(serverRunner, segmentsOfServerSpec);
      } else if (!server.segmentReplicatable() || !populateCache) {
        serverResults = getSimpleServerResults(serverRunner, segmentsOfServerSpec);
      } else {
        serverResults = getAndCacheServerResults(serverRunner, segmentsOfServerSpec);
      }
      return serverResults;
    }

    private ServerMaybeSegmentMaybeCache<T> pickServer(SerializablePair<ServerToSegment, Optional<T>> tuple)
    {
      final Optional<T> maybeResult = tuple.getRhs();
      if (maybeResult.isPresent()) {
        return new ServerMaybeSegmentMaybeCache<T>(ALREADY_CACHED_SERVER, Optional.empty(), maybeResult);
      }
      final ServerToSegment serverToSegment = tuple.getLhs();
      final QueryableDruidServer queryableDruidServer = serverToSegment.getServer().pick();
      if (queryableDruidServer == null) {
        log.makeAlert(
            "No servers found for SegmentDescriptor[%s] for DataSource[%s]?! How can this be?!",
            serverToSegment.getSegmentDescriptor(),
            query.getDataSource()
        ).emit();
        return new ServerMaybeSegmentMaybeCache<T>(
            ALREADY_CACHED_SERVER,
            Optional.empty(),
            Optional.empty()
        );
      }
      final DruidServer server = queryableDruidServer.getServer();
      return new ServerMaybeSegmentMaybeCache<T>(
          server,
          Optional.ofNullable(serverToSegment.getSegmentDescriptor()),
          Optional.empty()
      );
    }

    /**
     * This materializes the input stream in order to group it by server. This method takes in the stream of cache
     * resolved items and will group all the items by server. Each entry in the output stream contains a list whose
     * entries' getServer is the same. Each entry will either have a present segemnt descriptor or a present result,
     * but not both. Downstream consumers should check each and handle appropriately.
     *
     * @param cacheResolvedStream A stream of the cached results for different segment queries
     *
     * @return A stream of potentially cached results per server
     */

    private Stream<List<ServerMaybeSegmentMaybeCache<T>>> groupCachedResultsByServer(
        Stream<SerializablePair<ServerToSegment, Optional<T>>> cacheResolvedStream
    )
    {
      return cacheResolvedStream
          .map(this::pickServer)
          .collect(Collectors.groupingBy(ServerMaybeSegmentMaybeCache::getServer))
          .values()
          // At this point we have the segments per server, and a special entry for the pre-cached results.
          // As of the time of this writing, this results in a java.util.HashMap.ValueSpliterator which
          // does not have great properties for splitting in parallel since it does not have total size awareness
          // yet. I hope future implementations of the grouping collector can handle such a scenario where the
          // grouping result is immutable and can be split very easily into parallel spliterators
          .stream()
          .filter(l -> !l.isEmpty())
          // Get rid of any alerted conditions missing queryableDruidServer
          .filter(l -> l.get(0).getCachedValue().isPresent() || l.get(0).getSegmentDescriptor().isPresent());
    }

    private Stream<SerializablePair<ServerToSegment, Optional<T>>> deserializeFromCache(
        final Stream<SerializablePair<ServerToSegment, Optional<byte[]>>> cachedResults
    )
    {
      if (strategy == null) {
        return cachedResults.map(s -> new SerializablePair<>(s.getLhs(), Optional.empty()));
      }
      final Function<Object, T> pullFromCacheFunction = strategy.pullFromSegmentLevelCache()::apply;
      final TypeReference<Object> cacheObjectClazz = strategy.getCacheObjectClazz();
      return cachedResults.flatMap(cachedResultPair -> {
        if (!cachedResultPair.getRhs().isPresent()) {
          return Stream.of(new SerializablePair<>(cachedResultPair.getLhs(), Optional.empty()));
        }
        final byte[] cachedResult = cachedResultPair.getRhs().get();
        try {
          if (cachedResult.length == 0) {
            return Stream.of(new SerializablePair<>(cachedResultPair.getLhs(), Optional.empty()));
          }
          // Query granularity in a segment may be higher fidelity than the segment as a file, so this might have multiple results
          return StreamSupport.stream(
              Spliterators.spliteratorUnknownSize(
                  objectMapper.readValues(
                      objectMapper.getFactory().createParser(cachedResult),
                      cacheObjectClazz
                  ),
                  0
              ),
              false
          ).map(
              pullFromCacheFunction
          ).map(
              obj -> new SerializablePair<>(cachedResultPair.getLhs(), Optional.ofNullable(obj))
          );
        }
        catch (IOException e) {
          throw new RuntimeException(e);
        }
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

    private Sequence<T> bySegmentWithCachePopulator(
        Result<BySegmentResultValueClass<T>> result,
        Function<T, Object> cachePrep
    )
    {
      final BySegmentResultValueClass<T> resultsOfSegment = result.getValue();
      final Cache.NamedKey cachePopulatorKey = getCachePopulatorKey(
          resultsOfSegment.getSegmentId(),
          resultsOfSegment.getInterval()
      );
      Sequence<T> res = Sequences
          .simple(resultsOfSegment.getResults());
      if (cachePopulatorKey != null) {
        res = cachePopulator.wrap(res, cachePrep, cache, cachePopulatorKey);
      }
      return res.map(
          toolChest.makePreComputeManipulatorFn(downstreamQuery, MetricManipulatorFns.deserializing())::apply
      );
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
      final Function<T, Object> cacheFn = strategy.prepareForSegmentLevelCache()::apply;
      return resultsBySegments
          .map(result -> bySegmentWithCachePopulator(result, cacheFn))
          .flatMerge(Function.identity(), query.getResultOrdering());
    }
  }

  // POJO
  private static class ServerMaybeSegmentMaybeCache<T>
  {
    private final DruidServer server;
    private final Optional<SegmentDescriptor> segmentDescriptor;
    private final Optional<T> cachedValue;

    public DruidServer getServer()
    {
      return server;
    }

    public Optional<SegmentDescriptor> getSegmentDescriptor()
    {
      return segmentDescriptor;
    }

    public Optional<T> getCachedValue()
    {
      return cachedValue;
    }

    private ServerMaybeSegmentMaybeCache(
        DruidServer server,
        Optional<SegmentDescriptor> segmentDescriptor,
        Optional<T> cachedValue
    )
    {
      this.server = server;
      this.segmentDescriptor = segmentDescriptor;
      this.cachedValue = cachedValue;
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
}
