package com.metamx.druid.query;

import com.google.common.base.Function;
import com.metamx.common.guava.Sequence;
import com.metamx.druid.Query;

/**
*/
public interface CacheStrategy<T, QueryType extends Query<T>>
{
  public byte[] computeCacheKey(QueryType query);

  public Function<T, Object> prepareForCache();

  public Function<Object, T> pullFromCache();

  public Sequence<T> mergeSequences(Sequence<Sequence<T>> seqOfSequences);
}
