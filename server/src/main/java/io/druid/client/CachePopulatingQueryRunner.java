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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.client.cache.Cache;
import io.druid.client.cache.CacheConfig;
import io.druid.client.cache.MapCache;
import io.druid.query.CacheStrategy;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.SegmentDescriptor;

import java.util.ArrayList;

public class CachePopulatingQueryRunner<T> implements QueryRunner<T>
{

  private final String segmentIdentifier;
  private final SegmentDescriptor segmentDescriptor;
  private final QueryRunner<T> base;
  private final QueryToolChest toolChest;
  private final Cache cache;
  private final ObjectMapper mapper;
  private final CacheConfig cacheConfig;

  public CachePopulatingQueryRunner(
      String segmentIdentifier,
      SegmentDescriptor segmentDescriptor, ObjectMapper mapper,
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
                                  && cacheConfig.isPopulateCache()
                                  // historical only populates distributed cache since the cache lookups are done at broker.
                                  && !(cache instanceof MapCache);
    if (populateCache) {
      Sequence<T> results = base.run(query);
      Cache.NamedKey key = CacheUtil.computeSegmentCacheKey(
          segmentIdentifier,
          segmentDescriptor,
          strategy.computeCacheKey(query)
      );
      ArrayList<T> resultAsList = Sequences.toList(results, new ArrayList<T>());
      CacheUtil.populate(
          cache,
          mapper,
          key,
          Lists.transform(resultAsList, strategy.prepareForCache())
      );
      return Sequences.simple(resultAsList);
    } else {
      return base.run(query);
    }
  }

}
