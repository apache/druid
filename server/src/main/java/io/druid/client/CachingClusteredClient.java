/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.metamx.common.Pair;
import com.metamx.common.guava.BaseSequence;
import com.metamx.common.guava.Comparators;
import com.metamx.common.guava.LazySequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.emitter.EmittingLogger;
import io.druid.client.cache.Cache;
import io.druid.client.cache.CacheConfig;
import io.druid.client.selector.QueryableDruidServer;
import io.druid.client.selector.ServerSelector;
import io.druid.common.utils.JodaUtils;
import io.druid.guice.annotations.Smile;
import io.druid.query.BySegmentResultValueClass;
import io.druid.query.CacheStrategy;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.Result;
import io.druid.query.SegmentDescriptor;
import io.druid.query.aggregation.MetricManipulatorFns;
import io.druid.query.spec.MultipleSpecificSegmentSpec;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.PartitionChunk;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;

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

  @Inject
  public CachingClusteredClient(
      QueryToolChestWarehouse warehouse,
      TimelineServerView serverView,
      Cache cache,
      @Smile ObjectMapper objectMapper,
      CacheConfig cacheConfig
  )
  {
    this.warehouse = warehouse;
    this.serverView = serverView;
    this.cache = cache;
    this.objectMapper = objectMapper;
    this.cacheConfig = cacheConfig;

    serverView.registerSegmentCallback(
        Executors.newFixedThreadPool(
            1, new ThreadFactoryBuilder().setDaemon(true).setNameFormat("CCClient-ServerView-CB-%d").build()
        ),
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
  public Sequence<T> run(final Query<T> query)
  {
    final QueryToolChest<T, Query<T>> toolChest = warehouse.getToolChest(query);
    final CacheStrategy<T, Object, Query<T>> strategy = toolChest.getCacheStrategy(query);

    final Map<DruidServer, List<SegmentDescriptor>> serverSegments = Maps.newTreeMap();

    final List<Pair<Interval, byte[]>> cachedResults = Lists.newArrayList();
    final Map<String, CachePopulator> cachePopulatorMap = Maps.newHashMap();

    final boolean useCache = query.getContextUseCache(true)
                             && strategy != null
                             && cacheConfig.isUseCache()
                             && cacheConfig.isQueryCacheable(query);
    final boolean populateCache = query.getContextPopulateCache(true)
                                  && strategy != null
                                  && cacheConfig.isPopulateCache()
                                  && cacheConfig.isQueryCacheable(query);
    final boolean isBySegment = query.getContextBySegment(false);


    ImmutableMap.Builder<String, Object> contextBuilder = new ImmutableMap.Builder<>();

    final int priority = query.getContextPriority(0);
    contextBuilder.put("priority", priority);

    if (populateCache) {
      contextBuilder.put(CacheConfig.POPULATE_CACHE, false);
      contextBuilder.put("bySegment", true);
    }
    contextBuilder.put("intermediate", true);

    final Query<T> rewrittenQuery = query.withOverriddenContext(contextBuilder.build());


    VersionedIntervalTimeline<String, ServerSelector> timeline = serverView.getTimeline(query.getDataSource());
    if (timeline == null) {
      return Sequences.empty();
    }

    // build set of segments to query
    Set<Pair<ServerSelector, SegmentDescriptor>> segments = Sets.newLinkedHashSet();

    List<TimelineObjectHolder<String, ServerSelector>> serversLookup = Lists.newLinkedList();

    for (Interval interval : rewrittenQuery.getIntervals()) {
      serversLookup.addAll(timeline.lookup(interval));
    }

    // Let tool chest filter out unneeded segments
    final List<TimelineObjectHolder<String, ServerSelector>> filteredServersLookup =
        toolChest.filterSegments(query, serversLookup);

    for (TimelineObjectHolder<String, ServerSelector> holder : filteredServersLookup) {
      for (PartitionChunk<ServerSelector> chunk : holder.getObject()) {
        ServerSelector selector = chunk.getObject();
        final SegmentDescriptor descriptor = new SegmentDescriptor(
            holder.getInterval(), holder.getVersion(), chunk.getChunkNumber()
        );

        segments.add(Pair.of(selector, descriptor));
      }
    }

    final byte[] queryCacheKey;
    if (strategy != null) {
      queryCacheKey = strategy.computeCacheKey(query);
    } else {
      queryCacheKey = null;
    }

    if (queryCacheKey != null) {
      // cache keys must preserve segment ordering, in order for shards to always be combined in the same order
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
        cachedValues = cache.getBulk(cacheKeys.values());
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
        log.makeAlert("No servers found for %s?! How can this be?!", segment.rhs).emit();
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
            ArrayList<Pair<Interval, Sequence<T>>> sequencesByInterval = Lists.newArrayList();
            addSequencesFromCache(sequencesByInterval);
            addSequencesFromServer(sequencesByInterval);

            return mergeCachedAndUncachedSequences(sequencesByInterval, toolChest);
          }

          private void addSequencesFromCache(ArrayList<Pair<Interval, Sequence<T>>> listOfSequences)
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
              listOfSequences.add(Pair.of(cachedResultPair.lhs, Sequences.map(cachedSequence, pullFromCacheFunction)));
            }
          }

          @SuppressWarnings("unchecked")
          private void addSequencesFromServer(ArrayList<Pair<Interval, Sequence<T>>> listOfSequences)
          {
            for (Map.Entry<DruidServer, List<SegmentDescriptor>> entry : serverSegments.entrySet()) {
              final DruidServer server = entry.getKey();
              final List<SegmentDescriptor> descriptors = entry.getValue();

              final QueryRunner clientQueryable = serverView.getQueryRunner(server);
              if (clientQueryable == null) {
                log.makeAlert("WTF!? server[%s] doesn't have a client Queryable?", server).emit();
                continue;
              }

              final Sequence<T> resultSeqToAdd;
              final MultipleSpecificSegmentSpec segmentSpec = new MultipleSpecificSegmentSpec(descriptors);
              List<Interval> intervals = segmentSpec.getIntervals();

              if (!server.isAssignable() || !populateCache || isBySegment) {
                resultSeqToAdd = clientQueryable.run(query.withQuerySegmentSpec(segmentSpec));
              } else {
                // this could be more efficient, since we only need to reorder results
                // for batches of segments with the same segment start time.
                resultSeqToAdd = toolChest.mergeSequencesUnordered(
                    Sequences.map(
                        clientQueryable.run(rewrittenQuery.withQuerySegmentSpec(segmentSpec)),
                        new Function<Object, Sequence<T>>()
                        {
                          private final Function<T, Object> cacheFn = strategy.prepareForCache();

                          @Override
                          public Sequence<T> apply(Object input)
                          {
                            Result<Object> result = (Result<Object>) input;
                            final BySegmentResultValueClass<T> value = (BySegmentResultValueClass<T>) result.getValue();

                            final List<Object> cacheData = Lists.newLinkedList();

                            return Sequences.withEffect(
                                Sequences.map(
                                    Sequences.map(
                                        Sequences.simple(value.getResults()),
                                        new Function<T, T>()
                                        {
                                          @Override
                                          public T apply(T input)
                                          {
                                            cacheData.add(cacheFn.apply(input));
                                            return input;
                                          }
                                        }
                                    ),
                                    toolChest.makePreComputeManipulatorFn(
                                        rewrittenQuery,
                                        MetricManipulatorFns.deserializing()
                                    )
                                ),
                                new Runnable()
                                {
                                  @Override
                                  public void run()
                                  {
                                    CachePopulator cachePopulator = cachePopulatorMap.get(
                                        String.format("%s_%s", value.getSegmentId(), value.getInterval())
                                    );
                                    if (cachePopulator != null) {
                                      cachePopulator.populate(cacheData);
                                    }
                                  }
                                },
                                MoreExecutors.sameThreadExecutor()
                            );
                          }
                        }
                    )
                );
              }


              listOfSequences.add(
                  Pair.of(
                      new Interval(intervals.get(0).getStart(), intervals.get(intervals.size() - 1).getEnd()),
                      resultSeqToAdd
                  )
              );
            }
          }
        }
    );
  }

  protected Sequence<T> mergeCachedAndUncachedSequences(
      List<Pair<Interval, Sequence<T>>> sequencesByInterval,
      QueryToolChest<T, Query<T>> toolChest
  )
  {
    if(sequencesByInterval.isEmpty()) {
      return Sequences.empty();
    }

    Collections.sort(
        sequencesByInterval,
        Ordering.from(Comparators.intervalsByStartThenEnd()).onResultOf(Pair.<Interval, Sequence<T>>lhsFn())
    );

    // result sequences from overlapping intervals could start anywhere within that interval
    // therefore we cannot assume any ordering with respect to the first result from each
    // and must resort to calling toolchest.mergeSequencesUnordered for those.
    Iterator<Pair<Interval, Sequence<T>>> iterator = sequencesByInterval.iterator();
    Pair<Interval, Sequence<T>> current = iterator.next();

    final List<Sequence<T>> orderedSequences = Lists.newLinkedList();
    List<Sequence<T>> unordered = Lists.newLinkedList();

    unordered.add(current.rhs);

    while(iterator.hasNext()) {
      Pair<Interval, Sequence<T>> next = iterator.next();
      if(!next.lhs.overlaps(current.lhs)) {
        orderedSequences.add(toolChest.mergeSequencesUnordered(Sequences.simple(unordered)));
        unordered = Lists.newLinkedList();
      }
      unordered.add(next.rhs);
      current = next;
    }
    if(!unordered.isEmpty()) {
      orderedSequences.add(toolChest.mergeSequencesUnordered(Sequences.simple(unordered)));
    }

    return toolChest.mergeSequencesUnordered(Sequences.simple(orderedSequences));
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
