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

package io.druid.query;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import io.druid.guice.annotations.ExtensionPoint;

import java.util.concurrent.ExecutorService;

/**
*/
@ExtensionPoint
public interface CacheStrategy<T, CacheType, QueryType extends Query<T>>
{
  /**
   * Returns the given query is cacheable or not.
   * The {@code willMergeRunners} parameter can be used for distinguishing the caller is a broker or a data node.
   *
   * @param query            the query to be cached
   * @param willMergeRunners indicates that {@link QueryRunnerFactory#mergeRunners(ExecutorService, Iterable)} will be
   *                         called on the cached by-segment results
   * @return true if the query is cacheable, otherwise false.
   */
  boolean isCacheable(QueryType query, boolean willMergeRunners);

  /**
   * Computes the cache key for the given query
   *
   * @param query the query to compute a cache key for
   * @return the cache key
   */
  byte[] computeCacheKey(QueryType query);

  /**
   * Returns the class type of what is used in the cache
   *
   * @return Returns the class type of what is used in the cache
   */
  TypeReference<CacheType> getCacheObjectClazz();

  /**
   * Returns a function that converts from the QueryType's result type to something cacheable.
   *
   * The resulting function must be thread-safe.
   *
   * @return a thread-safe function that converts the QueryType's result type into something cacheable
   */
  Function<T, CacheType> prepareForCache();

  /**
   * A function that does the inverse of the operation that the function prepareForCache returns
   *
   * @return A function that does the inverse of the operation that the function prepareForCache returns
   */
  Function<CacheType, T> pullFromCache();
}
