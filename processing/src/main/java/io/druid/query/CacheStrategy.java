/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.query;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.metamx.common.guava.Sequence;

/**
*/
public interface CacheStrategy<T, CacheType, QueryType extends Query<T>>
{
  public byte[] computeCacheKey(QueryType query);

  public TypeReference<CacheType> getCacheObjectClazz();

  // Resultant function must be THREAD SAFE
  public Function<T, CacheType> prepareForCache();

  public Function<CacheType, T> pullFromCache();

  public Sequence<T> mergeSequences(Sequence<Sequence<T>> seqOfSequences);
}
