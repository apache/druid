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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.metamx.common.ISE;
import com.metamx.common.Pair;
import com.metamx.common.guava.BaseSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.druid.Query;
import com.metamx.druid.TimelineObjectHolder;
import com.metamx.druid.VersionedIntervalTimeline;
import com.metamx.druid.client.cache.Cache;
import com.metamx.druid.client.selector.ServerSelector;
import com.metamx.druid.partition.PartitionChunk;
import com.metamx.druid.query.CacheStrategy;
import com.metamx.druid.query.QueryRunner;
import com.metamx.druid.query.QueryToolChest;
import com.metamx.druid.query.QueryToolChestWarehouse;
import com.metamx.emitter.EmittingLogger;
import org.joda.time.Interval;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;

/**
 */
public class ResultsCachingClient<T> implements QueryRunner<T>
{
  private static final EmittingLogger log = new EmittingLogger(ResultsCachingClient.class);

  private final QueryToolChestWarehouse warehouse;
  private final TimelineServerView serverView;
  private final Cache cache;
  private final ObjectMapper objectMapper;

  private QueryRunner<T> baseRunner;

  public ResultsCachingClient(
      QueryToolChestWarehouse warehouse,
      TimelineServerView serverView,
      Cache cache,
      ObjectMapper objectMapper
  )
  {
    this.warehouse = warehouse;
    this.serverView = serverView;
    this.cache = cache;
    this.objectMapper = objectMapper;

    serverView.registerSegmentCallback(
        Executors.newFixedThreadPool(
            1, new ThreadFactoryBuilder().setDaemon(true).setNameFormat("RCC-ServerView-CB-%d").build()
        ),
        new ServerView.BaseSegmentCallback()
        {
          @Override
          public ServerView.CallbackAction segmentRemoved(DruidServer server, DataSegment segment)
          {
            ResultsCachingClient.this.cache.close(segment.getIdentifier());
            return ServerView.CallbackAction.CONTINUE;
          }
        }
    );
  }

  public QueryRunner<T> executeWithBaseRunner(QueryRunner<T> baseRunner)
  {
    this.baseRunner = baseRunner;
    return this;
  }

  /**
   * Gets the results from the segment results cache, if useCache is true.
   * Rest of the segments are queried and results are stored in the cache, lazily.
   *
   * @param query
   *
   * @return LazySequence which merges the results from the cache and servers.
   *         It also populates the cache when retrieving results from the servers.
   */
  @Override
  public Sequence<T> run(final Query<T> query)
  {
    final QueryToolChest<T, Query<T>> toolChest = warehouse.getToolChest(query);
    final CacheStrategy<T, Object, Query<T>> strategy = toolChest.getCacheStrategy(query);

    final boolean useResultsCache = Boolean.parseBoolean(query.getContextValue("useResultsCache", "true"))
                                    && strategy != null;
    final boolean populateResultsCache = Boolean.parseBoolean(query.getContextValue("populateResultsCache", "true"))
                                         && strategy != null;

    Cache.NamedKey resultsCacheKey = (strategy != null) ? computeResultsCacheKey(query) : null;
    if (useResultsCache && resultsCacheKey != null) {
      final byte[] cachedResults = cache.get(resultsCacheKey);

      if (cachedResults != null) {
        final TypeReference<Object> cacheObjectClazz = strategy.getCacheObjectClazz();

        Sequence<T> cachedSequence = new BaseSequence<T, Iterator<T>>(
            new BaseSequence.IteratorMaker<T, Iterator<T>>()
            {
              @Override
              public Iterator<T> make()
              {
                try {
                  if (cachedResults.length == 0) {
                    return Iterators.emptyIterator();
                  }
                  return objectMapper.readValues(
                      objectMapper.getJsonFactory().createJsonParser(cachedResults), cacheObjectClazz
                  );
                }
                catch (IOException e) {
                  throw Throwables.propagate(e);
                }
              }

              @Override
              public void cleanup(Iterator<T> iterFromMake)
              {
              }
            }
        );
        return cachedSequence;
      }
    }

    if (baseRunner == null) {
      throw new ISE("Client cannot execute without baserunner.");
    }
    Sequence retVal = baseRunner.run(query);

    if (populateResultsCache && resultsCacheKey != null) {
      populate(resultsCacheKey, Sequences.toList(retVal, Lists.<Object>newArrayList()));
    }

    return retVal;
  }

  public void populate(Cache.NamedKey key, Iterable<Object> results)
  {
    try {
      List<byte[]> bytes = Lists.newArrayList();
      int size = 0;
      for (Object result : results) {
        final byte[] array = objectMapper.writeValueAsBytes(result);
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

  /**
   * Compute cache key including all the segment identifiers and versions for the particular query.
   * This should take care if any of the segments are modified.
   *
   * @param query
   *
   * @return
   */
  private Cache.NamedKey computeResultsCacheKey(Query<T> query)
  {
    final QueryToolChest<T, Query<T>> toolChest = warehouse.getToolChest(query);
    VersionedIntervalTimeline<String, ServerSelector> timeline = serverView.getTimeline(query.getDataSource());
    ImmutableMap.Builder<String, String> contextBuilder = new ImmutableMap.Builder<String, String>();

    Set<Pair<String, String>> segments = Sets.newLinkedHashSet();

    final String priority = query.getContextValue("priority", "0");
    contextBuilder.put("priority", priority);
    contextBuilder.put("bySegment", "true");
    contextBuilder.put("intermediate", "true");
    final Query<T> rewrittenQuery = query.withOverriddenContext(contextBuilder.build());

    List<TimelineObjectHolder<String, ServerSelector>> serversLookup = Lists.newLinkedList();
    for (Interval interval : rewrittenQuery.getIntervals()) {
      serversLookup.addAll(timeline.lookup(interval));
    }

    // Let tool chest filter out unneeded segments
    final List<TimelineObjectHolder<String, ServerSelector>> filteredServersLookup =
        toolChest.filterSegments(query, serversLookup);
    int segmentSize = 0, versionSize = 0;
    for (TimelineObjectHolder<String, ServerSelector> holder : filteredServersLookup) {
      for (PartitionChunk<ServerSelector> chunk : holder.getObject()) {
        DataSegment segment = chunk.getObject().getSegment();
        segmentSize += segment.getIdentifier().getBytes().length;
        versionSize += segment.getVersion().getBytes().length;
        segments.add(Pair.of(segment.getIdentifier(), segment.getVersion()));
      }
    }

    log.debug(
        "Results cache key size includes %d bytes of identifier and %d of version.", segmentSize, versionSize
    );
    ByteBuffer byteBuffer = ByteBuffer.allocate(segmentSize + versionSize);
    for (Pair<String, String> segment : segments) {
      byteBuffer.put(segment.lhs.getBytes()).put(segment.rhs.getBytes());
    }

    return new Cache.NamedKey(query.toString(), byteBuffer.array());
  }

}
