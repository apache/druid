/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.metamx.common.ISE;
import com.metamx.common.Pair;
import com.metamx.common.guava.BaseSequence;
import com.metamx.common.guava.LazySequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.logger.Logger;
import com.metamx.druid.Query;
import com.metamx.druid.TimelineObjectHolder;
import com.metamx.druid.VersionedIntervalTimeline;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.client.cache.Cache;
import com.metamx.druid.client.cache.CacheBroker;
import com.metamx.druid.client.selector.ServerSelector;
import com.metamx.druid.partition.PartitionChunk;
import com.metamx.druid.query.CacheStrategy;
import com.metamx.druid.query.MetricManipulationFn;
import com.metamx.druid.query.QueryRunner;
import com.metamx.druid.query.QueryToolChest;
import com.metamx.druid.query.QueryToolChestWarehouse;
import com.metamx.druid.query.segment.MultipleSpecificSegmentSpec;
import com.metamx.druid.query.segment.SegmentDescriptor;
import com.metamx.druid.result.BySegmentResultValueClass;
import com.metamx.druid.result.Result;

/**
 */
public class CachingClusteredClient<T> implements QueryRunner<T>
{
  private static final Logger log = new Logger(CachingClusteredClient.class);

  private final QueryToolChestWarehouse warehouse;
  private final ServerView serverView;
  private final CacheBroker cacheBroker;
  private final ObjectMapper objectMapper;

  public CachingClusteredClient(
      QueryToolChestWarehouse warehouse,
      ServerView serverView,
      CacheBroker cacheBroker,
      ObjectMapper objectMapper
  )
  {
    this.warehouse = warehouse;
    this.serverView = serverView;
    this.cacheBroker = cacheBroker;
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
            CachingClusteredClient.this.cacheBroker.provideCache(segment.getIdentifier()).close();
            return ServerView.CallbackAction.CONTINUE;
          }
        }
    );
  }

  @Override
  public Sequence<T> run(final Query<T> query)
  {
    final QueryToolChest<T, Query<T>> toolChest = warehouse.getToolChest(query);
    final CacheStrategy<T, Query<T>> strategy = toolChest.getCacheStrategy(query);

    final Map<DruidServer, List<SegmentDescriptor>> segs = Maps.newTreeMap();
    final List<Pair<DateTime, byte[]>> cachedResults = Lists.newArrayList();
    final Map<String, CachePopulator> cachePopulatorMap = Maps.newHashMap();

    final boolean useCache = Boolean.parseBoolean(query.getContextValue("useCache", "true")) && strategy != null;
    final boolean populateCache = Boolean.parseBoolean(query.getContextValue("populateCache", "true")) && strategy != null;
    final boolean isBySegment = Boolean.parseBoolean(query.getContextValue("bySegment", "false"));

    final Query<T> rewrittenQuery;
    if (populateCache) {
      rewrittenQuery = query.withOverriddenContext(ImmutableMap.of("bySegment", "true", "intermediate", "true"));
    } else {
      rewrittenQuery = query.withOverriddenContext(ImmutableMap.of("intermediate", "true"));
    }

    VersionedIntervalTimeline<String, ServerSelector> timeline = serverView.getTimeline(query.getDataSource());
    if (timeline == null) {
      return Sequences.empty();
    }

    byte[] queryCacheKey = null;
    if (strategy != null) {
      queryCacheKey = strategy.computeCacheKey(query);
    }

    for (Interval interval : rewrittenQuery.getIntervals()) {
      List<TimelineObjectHolder<String, ServerSelector>> serversLookup = timeline.lookup(interval);

      for (TimelineObjectHolder<String, ServerSelector> holder : serversLookup) {
        for (PartitionChunk<ServerSelector> chunk : holder.getObject()) {
          ServerSelector selector = chunk.getObject();
          final SegmentDescriptor descriptor = new SegmentDescriptor(
              holder.getInterval(), holder.getVersion(), chunk.getChunkNumber()
          );

          if (queryCacheKey == null) {
            final DruidServer server = selector.pick();
            List<SegmentDescriptor> descriptors = segs.get(server);

            if (descriptors == null) {
              descriptors = Lists.newArrayList();
              segs.put(server, descriptors);
            }

            descriptors.add(descriptor);
          }
          else {
            final Interval segmentQueryInterval = holder.getInterval();
            final byte[] versionBytes = descriptor.getVersion().getBytes();

            final byte[] cacheKey = ByteBuffer
                .allocate(16 + versionBytes.length + 4 + queryCacheKey.length)
                .putLong(segmentQueryInterval.getStartMillis())
                .putLong(segmentQueryInterval.getEndMillis())
                .put(versionBytes)
                .putInt(descriptor.getPartitionNumber())
                .put(queryCacheKey)
                .array();
            final String segmentIdentifier = selector.getSegment().getIdentifier();
            final Cache cache = cacheBroker.provideCache(segmentIdentifier);
            final byte[] cachedValue = cache.get(cacheKey);

            if (useCache && cachedValue != null) {
              cachedResults.add(Pair.of(segmentQueryInterval.getStart(), cachedValue));
            } else {
              final DruidServer server = selector.pick();
              List<SegmentDescriptor> descriptors = segs.get(server);

              if (descriptors == null) {
                descriptors = Lists.newArrayList();
                segs.put(server, descriptors);
              }

              descriptors.add(descriptor);
              cachePopulatorMap.put(
                  String.format("%s_%s", segmentIdentifier, segmentQueryInterval),
                  new CachePopulator(cache, objectMapper, cacheKey)
              );
            }
          }
        }
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
            }
            else {
              return strategy.mergeSequences(seq);
            }
          }

          private void addSequencesFromCache(ArrayList<Pair<DateTime, Sequence<T>>> listOfSequences)
          {
            if (strategy == null) {
              return;
            }

            final Function<Object, T> pullFromCacheFunction = strategy.pullFromCache();
            for (Pair<DateTime, byte[]> cachedResultPair : cachedResults) {
              final byte[] cachedResult = cachedResultPair.rhs;
              Sequence<Object> cachedSequence = new BaseSequence<Object, Iterator<Object>>(
                  new BaseSequence.IteratorMaker<Object, Iterator<Object>>()
                  {
                    @Override
                    public Iterator<Object> make()
                    {
                      try {
                        return objectMapper.readValues(
                            objectMapper.getJsonFactory().createJsonParser(cachedResult), Object.class
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
            for (Map.Entry<DruidServer, List<SegmentDescriptor>> entry : segs.entrySet()) {
              final DruidServer server = entry.getKey();
              final List<SegmentDescriptor> descriptors = entry.getValue();

              final QueryRunner clientQueryable = serverView.getQueryRunner(server);
              if (clientQueryable == null) {
                throw new ISE("WTF!? server[%s] doesn't have a client Queryable?", server);
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
                                String.format("%s_%s", segmentIdentifier, value.getIntervalString())
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

  private static class CachePopulator
  {
    private final Cache cache;
    private final ObjectMapper mapper;
    private final byte[] key;

    public CachePopulator(Cache cache, ObjectMapper mapper, byte[] key)
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
