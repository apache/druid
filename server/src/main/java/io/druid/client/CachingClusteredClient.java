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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.metamx.common.Pair;
import com.metamx.common.guava.BaseSequence;
import com.metamx.common.guava.LazySequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.emitter.EmittingLogger;
import io.druid.client.cache.Cache;
import io.druid.client.selector.ServerSelector;
import io.druid.client.selector.QueryableDruidServer;
import io.druid.guice.annotations.Smile;
import io.druid.query.BySegmentResultValueClass;
import io.druid.query.CacheStrategy;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.Result;
import io.druid.query.SegmentDescriptor;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.MetricManipulationFn;
import io.druid.query.spec.MultipleSpecificSegmentSpec;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.PartitionChunk;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.IOException;
import java.nio.ByteBuffer;
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

  @Inject
  public CachingClusteredClient(
      QueryToolChestWarehouse warehouse,
      TimelineServerView serverView,
      Cache cache,
      @Smile ObjectMapper objectMapper
  )
  {
    this.warehouse = warehouse;
    this.serverView = serverView;
    this.cache = cache;
    this.objectMapper = objectMapper;

    serverView.registerSegmentCallback(
        Executors.newFixedThreadPool(
            1, new ThreadFactoryBuilder().setDaemon(true).setNameFormat("CCClient-ServerView-CB-%d").build()
        ),
        new ServerView.BaseSegmentCallback()
        {
          @Override
          public ServerView.CallbackAction segmentRemoved(DruidServer server, DataSegment segment)
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

    final List<Pair<DateTime, byte[]>> cachedResults = Lists.newArrayList();
    final Map<String, CachePopulator> cachePopulatorMap = Maps.newHashMap();

    final boolean useCache = Boolean.parseBoolean(query.getContextValue("useCache", "true")) && strategy != null;
    final boolean populateCache = Boolean.parseBoolean(query.getContextValue("populateCache", "true"))
                                  && strategy != null;
    final boolean isBySegment = Boolean.parseBoolean(query.getContextValue("bySegment", "false"));


    ImmutableMap.Builder<String, String> contextBuilder = new ImmutableMap.Builder<String, String>();

    final String priority = query.getContextValue("priority", "0");
    contextBuilder.put("priority", priority);

    if (populateCache) {
      contextBuilder.put("bySegment", "true");
    }
    contextBuilder.put("intermediate", "true");

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

    // Pull cached segments from cache and remove from set of segments to query
    if (useCache && queryCacheKey != null) {
      Map<Pair<ServerSelector, SegmentDescriptor>, Cache.NamedKey> cacheKeys = Maps.newHashMap();
      for (Pair<ServerSelector, SegmentDescriptor> e : segments) {
        cacheKeys.put(e, computeSegmentCacheKey(e.lhs.getSegment().getIdentifier(), e.rhs, queryCacheKey));
      }

      Map<Cache.NamedKey, byte[]> cachedValues = cache.getBulk(cacheKeys.values());

      for (Map.Entry<Pair<ServerSelector, SegmentDescriptor>, Cache.NamedKey> entry : cacheKeys.entrySet()) {
        Pair<ServerSelector, SegmentDescriptor> segment = entry.getKey();
        Cache.NamedKey segmentCacheKey = entry.getValue();

        final ServerSelector selector = segment.lhs;
        final SegmentDescriptor descriptor = segment.rhs;
        final Interval segmentQueryInterval = descriptor.getInterval();

        final byte[] cachedValue = cachedValues.get(segmentCacheKey);

        if (cachedValue != null) {
          cachedResults.add(Pair.of(segmentQueryInterval.getStart(), cachedValue));

          // remove cached segment from set of segments to query
          segments.remove(segment);
        } else {
          final String segmentIdentifier = selector.getSegment().getIdentifier();
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
        log.error("No servers found for %s?! How can this be?!", segment.rhs);
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

    return new LazySequence<T>(
        new Supplier<Sequence<T>>()
        {
          @Override
          public Sequence<T> get()
          {
            ArrayList<Pair<DateTime, Sequence<T>>> listOfSequences = Lists.newArrayList();

            addSequencesFromServer(listOfSequences);
            addSequencesFromCache(listOfSequences);

            Collections.sort(
                listOfSequences,
                Ordering.natural().onResultOf(Pair.<DateTime, Sequence<T>>lhsFn())
            );

            final Sequence<Sequence<T>> seq = Sequences.simple(
                Iterables.transform(listOfSequences, Pair.<DateTime, Sequence<T>>rhsFn())
            );
            if (strategy == null) {
              return toolChest.mergeSequences(seq);
            } else {
              return strategy.mergeSequences(seq);
            }
          }

          private void addSequencesFromCache(ArrayList<Pair<DateTime, Sequence<T>>> listOfSequences)
          {
            if (strategy == null) {
              return;
            }

            final Function<Object, T> pullFromCacheFunction = strategy.pullFromCache();
            final TypeReference<Object> cacheObjectClazz = strategy.getCacheObjectClazz();
            for (Pair<DateTime, byte[]> cachedResultPair : cachedResults) {
              final byte[] cachedResult = cachedResultPair.rhs;
              Sequence<Object> cachedSequence = new BaseSequence<Object, Iterator<Object>>(
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
          private void addSequencesFromServer(ArrayList<Pair<DateTime, Sequence<T>>> listOfSequences)
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

              if ("realtime".equals(server.getType()) || !populateCache || isBySegment) {
                resultSeqToAdd = clientQueryable.run(query.withQuerySegmentSpec(segmentSpec));
              } else {
                resultSeqToAdd = toolChest.mergeSequences(
                    Sequences.map(
                        clientQueryable.run(rewrittenQuery.withQuerySegmentSpec(segmentSpec)),
                        new Function<Object, Sequence<T>>()
                        {
                          private final Function<T, Object> prepareForCache = strategy.prepareForCache();

                          @Override
                          public Sequence<T> apply(Object input)
                          {
                            Result<Object> result = (Result<Object>) input;
                            final BySegmentResultValueClass<T> value = (BySegmentResultValueClass<T>) result.getValue();
                            String segmentIdentifier = value.getSegmentId();
                            final Iterable<T> segmentResults = value.getResults();

                            cachePopulatorMap.get(
                                String.format("%s_%s", segmentIdentifier, value.getInterval())
                            ).populate(Iterables.transform(segmentResults, prepareForCache));

                            return Sequences.simple(
                                Iterables.transform(
                                    segmentResults,
                                    toolChest.makeMetricManipulatorFn(
                                        rewrittenQuery,
                                        new MetricManipulationFn()
                                        {
                                          @Override
                                          public Object manipulate(AggregatorFactory factory, Object object)
                                          {
                                            return factory.deserialize(object);
                                          }
                                        }
                                    )
                                )
                            );
                          }
                        }
                    )
                );
              }

              listOfSequences.add(Pair.of(intervals.get(0).getStart(), resultSeqToAdd));
            }
          }
        }
    );
  }

  private Cache.NamedKey computeSegmentCacheKey(
      String segmentIdentifier,
      SegmentDescriptor descriptor,
      byte[] queryCacheKey
  )
  {
    final Interval segmentQueryInterval = descriptor.getInterval();
    final byte[] versionBytes = descriptor.getVersion().getBytes();

    return new Cache.NamedKey(
        segmentIdentifier, ByteBuffer
        .allocate(16 + versionBytes.length + 4 + queryCacheKey.length)
        .putLong(segmentQueryInterval.getStartMillis())
        .putLong(segmentQueryInterval.getEndMillis())
        .put(versionBytes)
        .putInt(descriptor.getPartitionNumber())
        .put(queryCacheKey).array()
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
      try {
        List<byte[]> bytes = Lists.newArrayList();
        int size = 0;
        for (Object result : results) {
          final byte[] array = mapper.writeValueAsBytes(result);
          size += array.length;
          bytes.add(array);
        }

        byte[] valueBytes = new byte[size];
        int offset = 0;
        for (byte[] array : bytes) {
          System.arraycopy(array, 0, valueBytes, offset, array.length);
          offset += array.length;
        }

        cache.put(key, valueBytes);
      }
      catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
  }
}
