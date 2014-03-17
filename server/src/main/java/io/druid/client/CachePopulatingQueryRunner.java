package io.druid.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.client.cache.Cache;
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

  public CachePopulatingQueryRunner(
      String segmentIdentifier,
      SegmentDescriptor segmentDescriptor, ObjectMapper mapper,
      Cache cache, QueryToolChest toolchest,
      QueryRunner<T> base
  )
  {
    this.base = base;
    this.segmentIdentifier = segmentIdentifier;
    this.segmentDescriptor = segmentDescriptor;
    this.toolChest = toolchest;
    this.cache = cache;
    this.mapper = mapper;
  }

  @Override
  public Sequence<T> run(Query<T> query)
  {

    final CacheStrategy strategy = toolChest.getCacheStrategy(query);

    final boolean populateCache = Boolean.parseBoolean(query.getContextValue("populateCache", "true"))
                                  && strategy != null && cache.getCacheConfig().isPopulateCache();
    Sequence<T> results = base.run(query);
    if (populateCache) {
      Cache.NamedKey key = CacheUtil.computeSegmentCacheKey(
          segmentIdentifier,
          segmentDescriptor,
          strategy.computeCacheKey(query)
      );
      CacheUtil.populate(
          cache,
          mapper,
          key,
          Sequences.toList(Sequences.map(results, strategy.prepareForCache()), new ArrayList())
      );
    }
    return results;

  }
}
