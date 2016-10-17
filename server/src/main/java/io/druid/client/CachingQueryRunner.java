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
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import io.druid.client.cache.Cache;
import io.druid.client.cache.CacheConfig;
import io.druid.java.util.common.guava.BaseSequence;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.BaseQuery;
import io.druid.query.CacheStrategy;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.SegmentDescriptor;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class CachingQueryRunner<T> implements QueryRunner<T>
{
  private static final Logger log = new Logger(CachingQueryRunner.class);
  private final String segmentIdentifier;
  private final SegmentDescriptor segmentDescriptor;
  private final QueryRunner<T> base;
  private final QueryToolChest toolChest;
  private final Cache cache;
  private final ObjectMapper mapper;
  private final CacheConfig cacheConfig;
  private final ListeningExecutorService backgroundExecutorService;

  public CachingQueryRunner(
      String segmentIdentifier,
      SegmentDescriptor segmentDescriptor,
      ObjectMapper mapper,
      Cache cache,
      QueryToolChest toolchest,
      QueryRunner<T> base,
      ExecutorService backgroundExecutorService,
      CacheConfig cacheConfig
  )
  {
    this.base = base;
    this.segmentIdentifier = segmentIdentifier;
    this.segmentDescriptor = segmentDescriptor;
    this.toolChest = toolchest;
    this.cache = cache;
    this.mapper = mapper;
    this.backgroundExecutorService = MoreExecutors.listeningDecorator(backgroundExecutorService);
    this.cacheConfig = cacheConfig;
  }

  @Override
  public Sequence<T> run(Query<T> query, Map<String, Object> responseContext)
  {
    final CacheStrategy strategy = toolChest.getCacheStrategy(query);

    final boolean populateCache = BaseQuery.getContextPopulateCache(query, true)
                                  && strategy != null
                                  && cacheConfig.isPopulateCache()
                                  && cacheConfig.isQueryCacheable(query);

    final boolean useCache = BaseQuery.getContextUseCache(query, true)
                             && strategy != null
                             && cacheConfig.isUseCache()
                             && cacheConfig.isQueryCacheable(query);

    final Cache.NamedKey key;
    if (strategy != null && (useCache || populateCache)) {
      key = CacheUtil.computeSegmentCacheKey(
          segmentIdentifier,
          segmentDescriptor,
          strategy.computeCacheKey(query)
      );
    } else {
      key = null;
    }

    if (useCache) {
      final Function cacheFn = strategy.pullFromCache();
      final byte[] cachedResult = cache.get(key);
      if (cachedResult != null) {
        final TypeReference cacheObjectClazz = strategy.getCacheObjectClazz();

        return Sequences.map(
            new BaseSequence<>(
                new BaseSequence.IteratorMaker<T, Iterator<T>>()
                {
                  @Override
                  public Iterator<T> make()
                  {
                    try {
                      if (cachedResult.length == 0) {
                        return Iterators.emptyIterator();
                      }

                      return mapper.readValues(
                          mapper.getFactory().createParser(cachedResult),
                          cacheObjectClazz
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
            ),
            cacheFn
        );
      }
    }

    final Collection<ListenableFuture<?>> cacheFutures = Collections.synchronizedList(Lists.<ListenableFuture<?>>newLinkedList());
    if (populateCache) {
      final Function cacheFn = strategy.prepareForCache();
      final List<Object> cacheResults = Lists.newLinkedList();

      return Sequences.withEffect(
          Sequences.map(
              base.run(query, responseContext),
              new Function<T, T>()
              {
                @Override
                public T apply(final T input)
                {
                  cacheFutures.add(
                      backgroundExecutorService.submit(
                          new Runnable()
                          {
                            @Override
                            public void run()
                            {
                              cacheResults.add(cacheFn.apply(input));
                            }
                          }
                      )
                  );
                  return input;
                }
              }
          ),
          new Runnable()
          {
            @Override
            public void run()
            {
              try {
                Futures.allAsList(cacheFutures).get();
                CacheUtil.populate(cache, mapper, key, cacheResults);
              }
              catch (Exception e) {
                log.error(e, "Error while getting future for cache task");
                throw Throwables.propagate(e);
              }
            }
          },
          backgroundExecutorService
      );
    } else {
      return base.run(query, responseContext);
    }
  }

}
