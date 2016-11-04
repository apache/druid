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
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.RangeSet;
import com.google.common.collect.Sets;
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
import io.druid.java.util.common.guava.BaseSequence;
import io.druid.java.util.common.guava.LazySequence;
import io.druid.java.util.common.guava.MergeSequence;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.BaseQuery;
import io.druid.query.BySegmentResultValueClass;
import io.druid.query.CacheStrategy;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.Result;
import io.druid.query.SegmentDescriptor;
import io.druid.query.aggregation.MetricManipulatorFns;
import io.druid.query.filter.DimFilterUtils;
import io.druid.query.spec.MultipleSpecificSegmentSpec;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineLookup;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.partition.PartitionChunk;
import io.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;

/**
 */
public class CachingClusteredClient<T> implements QueryRunner<T>
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
  public Sequence<T> run(final Query<T> query, final Map<String, Object> responseContext)
  {
    final QueryToolChest<T, Query<T>> toolChest = warehouse.getToolChest(query);
    final CacheStrategy<T, Object, Query<T>> strategy = toolChest.getCacheStrategy(query);

    final Map<DruidServer, List<SegmentDescriptor>> serverSegments = Maps.newTreeMap();

    final List<Pair<Interval, byte[]>> cachedResults = Lists.newArrayList();
    final Map<String, CachePopulator> cachePopulatorMap = Maps.newHashMap();

    final boolean useCache = BaseQuery.getContextUseCache(query, true)
                             && strategy != null
                             && cacheConfig.isUseCache()
                             && cacheConfig.isQueryCacheable(query);
    final boolean populateCache = BaseQuery.getContextPopulateCache(query, true)
                                  && strategy != null
                                  && cacheConfig.isPopulateCache()
                                  && cacheConfig.isQueryCacheable(query);
    final boolean isBySegment = BaseQuery.getContextBySegment(query, false);


    final ImmutableMap.Builder<String, Object> contextBuilder = new ImmutableMap.Builder<>();

    final int priority = BaseQuery.getContextPriority(query, 0);
    contextBuilder.put("priority", priority);

    if (populateCache) {
      // prevent down-stream nodes from caching results as well if we are populating the cache
      contextBuilder.put(CacheConfig.POPULATE_CACHE, false);
      contextBuilder.put("bySegment", true);
    }

    TimelineLookup<String, ServerSelector> timeline = serverView.getTimeline(query.getDataSource());

    if (timeline == null) {
      return Sequences.empty();
    }

    // build set of segments to query
    Set<Pair<ServerSelector, SegmentDescriptor>> segments = Sets.newLinkedHashSet();

    List<TimelineObjectHolder<String, ServerSelector>> serversLookup = Lists.newLinkedList();

    // Note that enabling this leads to putting uncovered intervals information in the response headers
    // and might blow up in some cases https://github.com/druid-io/druid/issues/2108
    int uncoveredIntervalsLimit = BaseQuery.getContextUncoveredIntervalsLimit(query, 0);

    if (uncoveredIntervalsLimit > 0) {
      List<Interval> uncoveredIntervals = Lists.newArrayListWithCapacity(uncoveredIntervalsLimit);
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
          serversLookup.add(holder);
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
    } else {
      for (Interval interval : query.getIntervals()) {
        Iterables.addAll(serversLookup, timeline.lookup(interval));
      }
    }

    // Let tool chest filter out unneeded segments
    final List<TimelineObjectHolder<String, ServerSelector>> filteredServersLookup =
        toolChest.filterSegments(query, serversLookup);
    Map<String, Optional<RangeSet<String>>> dimensionRangeCache = Maps.newHashMap();

    // Filter unneeded chunks based on partition dimension
    for (TimelineObjectHolder<String, ServerSelector> holder : filteredServersLookup) {
      final Set<PartitionChunk<ServerSelector>> filteredChunks = DimFilterUtils.filterShards(
          query.getFilter(),
          holder.getObject(),
          new Function<PartitionChunk<ServerSelector>, ShardSpec>()
          {
            @Override
            public ShardSpec apply(PartitionChunk<ServerSelector> input)
            {
              return input.getObject().getSegment().getShardSpec();
            }
          },
          dimensionRangeCache
      );
      for (PartitionChunk<ServerSelector> chunk : filteredChunks) {
        ServerSelector selector = chunk.getObject();
        final SegmentDescriptor descriptor = new SegmentDescriptor(
            holder.getInterval(), holder.getVersion(), chunk.getChunkNumber()
        );
        segments.add(Pair.of(selector, descriptor));
      }
    }

    final byte[] queryCacheKey;

    if ((populateCache || useCache) // implies strategy != null
        && !isBySegment) // explicit bySegment queries are never cached
    {
      queryCacheKey = strategy.computeCacheKey(query);
    } else {
      queryCacheKey = null;
    }

    if (queryCacheKey != null) {
      // cachKeys map must preserve segment ordering, in order for shards to always be combined in the same order
      Map<Pair<ServerSelector, SegmentDescriptor>, Cache.NamedKey> cacheKeys = Maps.newLinkedHashMap();
      for (Pair<ServerSelector, SegmentDescriptor> segment : segments) {
        final Cache.NamedKey segmentCacheKey = CacheUtil.computeSegmentCacheKey(
            segment.lhs.getSegment().getIdentifier(),
            segment.rhs,
            queryCacheKey
        );
        cacheKeys.put(segment, segmentCacheKey);
      }

      // Pull cached segments from cache and remove from set of segments to query
      final Map<Cache.NamedKey, byte[]> cachedValues;
      if (useCache) {
        cachedValues = cache.getBulk(Iterables.limit(cacheKeys.values(), cacheConfig.getCacheBulkMergeLimit()));
      } else {
        cachedValues = ImmutableMap.of();
      }

      for (Map.Entry<Pair<ServerSelector, SegmentDescriptor>, Cache.NamedKey> entry : cacheKeys.entrySet()) {
        Pair<ServerSelector, SegmentDescriptor> segment = entry.getKey();
        Cache.NamedKey segmentCacheKey = entry.getValue();
        final Interval segmentQueryInterval = segment.rhs.getInterval();

        final byte[] cachedValue = cachedValues.get(segmentCacheKey);
        if (cachedValue != null) {
          // remove cached segment from set of segments to query
          segments.remove(segment);
          cachedResults.add(Pair.of(segmentQueryInterval, cachedValue));
        } else if (populateCache) {
          // otherwise, if populating cache, add segment to list of segments to cache
          final String segmentIdentifier = segment.lhs.getSegment().getIdentifier();
          cachePopulatorMap.put(
              String.format("%s_%s", segmentIdentifier, segmentQueryInterval),
              new CachePopulator(cache, objectMapper, segmentCacheKey)
          );
        }
      }
    }

    // Compile list of all segments not pulled from cache
    for (Pair<ServerSelector, SegmentDescriptor> segment : segments) {
      final QueryableDruidServer queryableDruidServer = segment.lhs.pick();

      if (queryableDruidServer == null) {
        log.makeAlert(
            "No servers found for SegmentDescriptor[%s] for DataSource[%s]?! How can this be?!",
            segment.rhs,
            query.getDataSource()
        ).emit();
      } else {
        final DruidServer server = queryableDruidServer.getServer();
        List<SegmentDescriptor> descriptors = serverSegments.get(server);

        if (descriptors == null) {
          descriptors = Lists.newArrayList();
          serverSegments.put(server, descriptors);
        }

        descriptors.add(segment.rhs);
      }
    }

    return new LazySequence<>(
        new Supplier<Sequence<T>>()
        {
          @Override
          public Sequence<T> get()
          {
            ArrayList<Sequence<T>> sequencesByInterval = Lists.newArrayList();
            addSequencesFromCache(sequencesByInterval);
            addSequencesFromServer(sequencesByInterval);

            return mergeCachedAndUncachedSequences(query, sequencesByInterval);
          }

          private void addSequencesFromCache(ArrayList<Sequence<T>> listOfSequences)
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
                        throw Throwables.propagate(e);
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

          private void addSequencesFromServer(ArrayList<Sequence<T>> listOfSequences)
          {
            listOfSequences.ensureCapacity(listOfSequences.size() + serverSegments.size());

            final Query<T> rewrittenQuery = query.withOverriddenContext(contextBuilder.build());

            // Loop through each server, setting up the query and initiating it.
            // The data gets handled as a Future and parsed in the long Sequence chain in the resultSeqToAdd setter.
            for (Map.Entry<DruidServer, List<SegmentDescriptor>> entry : serverSegments.entrySet()) {
              final DruidServer server = entry.getKey();
              final List<SegmentDescriptor> descriptors = entry.getValue();

              final QueryRunner clientQueryable = serverView.getQueryRunner(server);

              if (clientQueryable == null) {
                log.error("WTF!? server[%s] doesn't have a client Queryable?", server);
                continue;
              }

              final MultipleSpecificSegmentSpec segmentSpec = new MultipleSpecificSegmentSpec(descriptors);

              final Sequence<T> resultSeqToAdd;
              if (!server.isAssignable() || !populateCache || isBySegment) { // Direct server queryable
                if (!isBySegment) {
                  resultSeqToAdd = clientQueryable.run(query.withQuerySegmentSpec(segmentSpec), responseContext);
                } else {
                  // bySegment queries need to be de-serialized, see DirectDruidClient.run()

                  @SuppressWarnings("unchecked")
                  final Query<Result<BySegmentResultValueClass<T>>> bySegmentQuery =
                      (Query<Result<BySegmentResultValueClass<T>>>) ((Query) query);

                  @SuppressWarnings("unchecked")
                  final Sequence<Result<BySegmentResultValueClass<T>>> resultSequence = clientQueryable.run(
                      bySegmentQuery.withQuerySegmentSpec(segmentSpec),
                      responseContext
                  );

                  resultSeqToAdd = (Sequence) Sequences.map(
                      resultSequence,
                      new Function<Result<BySegmentResultValueClass<T>>, Result<BySegmentResultValueClass<T>>>()
                      {
                        @Override
                        public Result<BySegmentResultValueClass<T>> apply(Result<BySegmentResultValueClass<T>> input)
                        {
                          final BySegmentResultValueClass<T> bySegmentValue = input.getValue();
                          return new Result<>(
                              input.getTimestamp(),
                              new BySegmentResultValueClass<T>(
                                  Lists.transform(
                                      bySegmentValue.getResults(),
                                      toolChest.makePreComputeManipulatorFn(
                                          query,
                                          MetricManipulatorFns.deserializing()
                                      )
                                  ),
                                  bySegmentValue.getSegmentId(),
                                  bySegmentValue.getInterval()
                              )
                          );
                        }
                      }
                  );
                }
              } else { // Requires some manipulation on broker side
                @SuppressWarnings("unchecked")
                final Sequence<Result<BySegmentResultValueClass<T>>> runningSequence = clientQueryable.run(
                    rewrittenQuery.withQuerySegmentSpec(segmentSpec),
                    responseContext
                );
                resultSeqToAdd = new MergeSequence(
                    query.getResultOrdering(),
                    Sequences.<Result<BySegmentResultValueClass<T>>, Sequence<T>>map(
                        runningSequence,
                        new Function<Result<BySegmentResultValueClass<T>>, Sequence<T>>()
                        {
                          private final Function<T, Object> cacheFn = strategy.prepareForCache();

                          // Acctually do something with the results
                          @Override
                          public Sequence<T> apply(Result<BySegmentResultValueClass<T>> input)
                          {
                            final BySegmentResultValueClass<T> value = input.getValue();
                            final CachePopulator cachePopulator = cachePopulatorMap.get(
                                String.format("%s_%s", value.getSegmentId(), value.getInterval())
                            );

                            final Queue<ListenableFuture<Object>> cacheFutures = new ConcurrentLinkedQueue<>();

                            return Sequences.<T>withEffect(
                                Sequences.<T, T>map(
                                    Sequences.<T, T>map(
                                        Sequences.<T>simple(value.getResults()),
                                        new Function<T, T>()
                                        {
                                          @Override
                                          public T apply(final T input)
                                          {
                                            if (cachePopulator != null) {
                                              // only compute cache data if populating cache
                                              cacheFutures.add(
                                                  backgroundExecutorService.submit(
                                                      new Callable<Object>()
                                                      {
                                                        @Override
                                                        public Object call()
                                                        {
                                                          return cacheFn.apply(input);
                                                        }
                                                      }
                                                  )
                                              );
                                            }
                                            return input;
                                          }
                                        }
                                    ),
                                    toolChest.makePreComputeManipulatorFn(
                                        // Ick... most makePreComputeManipulatorFn directly cast to their ToolChest query type of choice
                                        // This casting is sub-optimal, but hasn't caused any major problems yet...
                                        (Query) rewrittenQuery,
                                        MetricManipulatorFns.deserializing()
                                    )
                                ),
                                new Runnable()
                                {
                                  @Override
                                  public void run()
                                  {
                                    if (cachePopulator != null) {
                                      Futures.addCallback(
                                          Futures.allAsList(cacheFutures),
                                          new FutureCallback<List<Object>>()
                                          {
                                            @Override
                                            public void onSuccess(List<Object> cacheData)
                                            {
                                              cachePopulator.populate(cacheData);
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
                                },
                                MoreExecutors.sameThreadExecutor()
                            );// End withEffect
                          }
                        }
                    )
                );
              }

              listOfSequences.add(resultSeqToAdd);
            }
          }
        }// End of Supplier
    );
  }

  protected Sequence<T> mergeCachedAndUncachedSequences(
      Query<T> query,
      List<Sequence<T>> sequencesByInterval
  )
  {
    if (sequencesByInterval.isEmpty()) {
      return Sequences.empty();
    }

    return new MergeSequence<>(
        query.getResultOrdering(),
        Sequences.simple(sequencesByInterval)
    );
  }

  private static class CachePopulator
  {
    private final Cache cache;
    private final ObjectMapper mapper;
    private final Cache.NamedKey key;

    public CachePopulator(Cache cache, ObjectMapper mapper, Cache.NamedKey key)
    {
      this.cache = cache;
      this.mapper = mapper;
      this.key = key;
    }

    public void populate(Iterable<Object> results)
    {
      CacheUtil.populate(cache, mapper, key, results);
    }
  }
}
