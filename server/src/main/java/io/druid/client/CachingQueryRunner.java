/*
 * Druid - a distributed column store.
 * Copyright (C) 2014  Metamarkets Group Inc.
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
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.guava.BaseSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.client.cache.Cache;
import io.druid.client.cache.CacheConfig;
import io.druid.query.CacheStrategy;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.SegmentDescriptor;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class CachingQueryRunner<T> implements QueryRunner<T>
{

  private final String segmentIdentifier;
  private final SegmentDescriptor segmentDescriptor;
  private final QueryRunner<T> base;
  private final QueryToolChest toolChest;
  private final Cache cache;
  private final ObjectMapper mapper;
  private final CacheConfig cacheConfig;

  public CachingQueryRunner(
      String segmentIdentifier,
      SegmentDescriptor segmentDescriptor,
      ObjectMapper mapper,
      Cache cache,
      QueryToolChest toolchest,
      QueryRunner<T> base,
      CacheConfig cacheConfig
  )
  {
    this.base = base;
    this.segmentIdentifier = segmentIdentifier;
    this.segmentDescriptor = segmentDescriptor;
    this.toolChest = toolchest;
    this.cache = cache;
    this.mapper = mapper;
    this.cacheConfig = cacheConfig;
  }

  @Override
  public Sequence<T> run(Query<T> query)
  {
    final CacheStrategy strategy = toolChest.getCacheStrategy(query);

    final boolean populateCache = query.getContextPopulateCache(true)
                                  && strategy != null
                                  && cacheConfig.isPopulateCache();

    final boolean useCache = query.getContextUseCache(true)
        && strategy != null
        && cacheConfig.isPopulateCache();

    final Cache.NamedKey key = CacheUtil.computeSegmentCacheKey(
        segmentIdentifier,
        segmentDescriptor,
        strategy.computeCacheKey(query)
    );

    if(useCache) {
      final Function cacheFn = strategy.pullFromCache();
      final byte[] cachedResult = cache.get(key);
      if(cachedResult != null) {
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

    if (populateCache) {
      final Function cacheFn = strategy.prepareForCache();
      final List<Object> cacheResults = Lists.newLinkedList();

      return Sequences.withEffect(
          Sequences.map(
              base.run(query),
              new Function<T, T>()
              {
                @Override
                public T apply(T input)
                {
                  cacheResults.add(cacheFn.apply(input));
                  return input;
                }
              }
          ),
          new Runnable()
          {
            @Override
            public void run()
            {
              CacheUtil.populate(cache, mapper, key, cacheResults);
            }
          },
          MoreExecutors.sameThreadExecutor()
      );
    } else {
      return base.run(query);
    }
  }

}
