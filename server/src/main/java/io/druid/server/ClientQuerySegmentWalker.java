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

package io.druid.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.metamx.common.ISE;
import com.metamx.common.Pair;
import com.metamx.common.guava.BaseSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.client.CacheClientUtils;
import io.druid.client.CachePopulator;
import io.druid.client.CachingClusteredClient;
import io.druid.client.TimelineServerView;
import io.druid.client.cache.Cache;
import io.druid.client.selector.ServerSelector;
import io.druid.query.CacheStrategy;
import io.druid.query.FinalizeResultsQueryRunner;
import io.druid.query.MetricsEmittingQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.SegmentDescriptor;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 */
public class ClientQuerySegmentWalker implements QuerySegmentWalker
{
  private final QueryToolChestWarehouse warehouse;
  private final TimelineServerView serverView;
  private final Cache resultsCache;
  private final ObjectMapper objectMapper;
  private final ServiceEmitter emitter;

  private final CachingClusteredClient baseClient;

  @Inject
  public ClientQuerySegmentWalker(
      QueryToolChestWarehouse warehouse,
      TimelineServerView serverView,
      ObjectMapper objectMapper,
      ServiceEmitter emitter,
      @Named("queryCache") Cache queryCache,
      @Named("resultsCache") Cache resultsCache
  )
  {
    this.warehouse = warehouse;
    this.serverView = serverView;
    this.resultsCache = resultsCache;
    this.objectMapper = objectMapper;
    this.emitter = emitter;

    this.baseClient = new CachingClusteredClient(warehouse, serverView, queryCache, objectMapper);
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals)
  {
    return makeRunner(query);
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs)
  {
    return makeRunner(query);
  }

  private <T> QueryRunner<T> makeRunner(final Query<T> query)
  {
    final QueryToolChest<T, Query<T>> toolChest = warehouse.getToolChest(query);
    return
        new ResultsCachingClient<T>(
            warehouse, serverView, resultsCache, objectMapper,
            new FinalizeResultsQueryRunner<T>(
                toolChest.postMergeQueryDecoration(
                    toolChest.mergeResults(
                        new MetricsEmittingQueryRunner<T>(
                            emitter,
                            new Function<Query<T>, ServiceMetricEvent.Builder>()
                            {
                              @Override
                              public ServiceMetricEvent.Builder apply(@Nullable Query<T> input)
                              {
                                return toolChest.makeMetricBuilder(query);
                              }
                            },
                            toolChest.preMergeQueryDecoration(baseClient)
                        ).withWaitMeasuredFromNow()
                    )
                ),
                toolChest
            )
        );
  }

  /**
   */
  public static class ResultsCachingClient<T> implements QueryRunner<T>
  {
    private static final EmittingLogger log = new EmittingLogger(ResultsCachingClient.class);

    private final QueryToolChestWarehouse warehouse;
    private final TimelineServerView serverView;
    private final Cache cache;
    private final ObjectMapper objectMapper;
    private final QueryRunner<T> baseRunner;

    public ResultsCachingClient(
        QueryToolChestWarehouse warehouse,
        TimelineServerView serverView,
        Cache cache,
        ObjectMapper objectMapper,
        QueryRunner<T> baseRunner
    )
    {
      this.warehouse = warehouse;
      this.serverView = serverView;
      this.cache = cache;
      this.objectMapper = objectMapper;

      if (baseRunner == null) {
        Throwables.propagate(new ISE("Client cannot execute without baserunner."));
      }

      this.baseRunner = baseRunner;

      // TODO: Register segment callback
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

      Sequence retVal = baseRunner.run(query);

      if (populateResultsCache && resultsCacheKey != null) {
        new CachePopulator(cache, objectMapper, resultsCacheKey).populate(
            Sequences.toList(retVal, Lists.<Object>newArrayList())
        );
      }
      return retVal;
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
      ImmutableMap.Builder<String, String> contextBuilder = new ImmutableMap.Builder<String, String>();
      final String priority = query.getContextValue("priority", "0");
      contextBuilder.put("priority", priority);
      contextBuilder.put("bySegment", "true");
      contextBuilder.put("intermediate", "true");
      final Query<T> rewrittenQuery = query.withOverriddenContext(contextBuilder.build());

      Set<Pair<ServerSelector, SegmentDescriptor>> segments = CacheClientUtils.getSegments(
          rewrittenQuery, warehouse, serverView
      );

      if (segments == null) {
        return null;
      }

      List<SegmentDescriptor> list = new ArrayList<SegmentDescriptor>();
      for (Pair<ServerSelector, SegmentDescriptor> segment : segments) {
        list.add(segment.rhs);
      }

      byte[] queryKey = warehouse.getToolChest(query).getCacheStrategy(query).computeCacheKey(query);

      try {
        byte[] bytes = objectMapper.writeValueAsBytes(list);
        // TODO: need to have better namespace to assist segment callback
        return new Cache.NamedKey(new String(queryKey), bytes);
      }
      catch (JsonProcessingException e) {
        e.printStackTrace();
      }

      return null;
    }
  }
}
