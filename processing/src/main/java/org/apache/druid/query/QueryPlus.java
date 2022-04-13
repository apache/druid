/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.query;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.context.ResponseContext;

import javax.annotation.Nullable;

/**
 * An immutable composite object of {@link Query} + extra stuff needed in {@link QueryRunner}s.
 */
@PublicApi
public final class QueryPlus<T>
{
  /**
   * Returns the minimum bare QueryPlus object with the given query. {@link #getQueryMetrics()} of the QueryPlus object,
   * returned from this factory method, returns {@code null}.
   */
  public static <T> QueryPlus<T> wrap(Query<T> query)
  {
    Preconditions.checkNotNull(query);
    return new QueryPlus<>(query, null, null);
  }

  private final Query<T> query;
  private final QueryMetrics<?> queryMetrics;
  private final String identity;

  private QueryPlus(Query<T> query, QueryMetrics<?> queryMetrics, String identity)
  {
    this.query = query;
    this.queryMetrics = queryMetrics;
    this.identity = identity;
  }

  public Query<T> getQuery()
  {
    return query;
  }

  @Nullable
  public QueryMetrics<?> getQueryMetrics()
  {
    return queryMetrics;
  }

  /**
   * Returns the same QueryPlus object with the identity replaced. This new identity will affect future calls to
   * {@link #withoutQueryMetrics()} but will not affect any currently-existing queryMetrics.
   */
  public QueryPlus<T> withIdentity(String identity)
  {
    return new QueryPlus<>(query, queryMetrics, identity);
  }

  /**
   * Returns the same QueryPlus object, if it already has {@link QueryMetrics} ({@link #getQueryMetrics()} returns not
   * null), or returns a new QueryPlus object with {@link Query} from this QueryPlus and QueryMetrics created using the
   * given {@link QueryToolChest}, via {@link QueryToolChest#makeMetrics(Query)} method.
   *
   * By convention, callers of {@code withQueryMetrics()} must also call .getQueryMetrics().emit() on the returned
   * QueryMetrics object, regardless if this object is the same as the object on which .withQueryMetrics() was initially
   * called (i. e. it already had non-null QueryMetrics), or if it is a new QueryPlus object. See {@link
   * MetricsEmittingQueryRunner} for example.
   */
  public QueryPlus<T> withQueryMetrics(QueryToolChest<T, ? extends Query<T>> queryToolChest)
  {
    if (queryMetrics != null) {
      return this;
    } else {
      final QueryMetrics metrics = ((QueryToolChest) queryToolChest).makeMetrics(query);

      if (identity != null) {
        metrics.identity(identity);
      }

      return new QueryPlus<>(query, metrics, identity);
    }
  }

  /**
   * Returns a QueryPlus object without the components which are unsafe for concurrent use from multiple threads,
   * therefore couldn't be passed down in concurrent or async {@link QueryRunner}s.
   *
   * Currently the only unsafe component is {@link QueryMetrics}, i. e. {@code withoutThreadUnsafeState()} call is
   * equivalent to {@link #withoutQueryMetrics()}.
   */
  public QueryPlus<T> withoutThreadUnsafeState()
  {
    return withoutQueryMetrics();
  }

  /**
   * Returns the same QueryPlus object, if it doesn't have {@link QueryMetrics} ({@link #getQueryMetrics()} returns
   * null), or returns a new QueryPlus object with {@link Query} from this QueryPlus and null as QueryMetrics.
   */
  private QueryPlus<T> withoutQueryMetrics()
  {
    if (queryMetrics == null) {
      return this;
    } else {
      return new QueryPlus<>(query, null, identity);
    }
  }

  /**
   * Equivalent of withQuery(getQuery().withOverriddenContext(ImmutableMap.of(MAX_QUEUED_BYTES_KEY, maxQueuedBytes))).
   */
  public QueryPlus<T> withMaxQueuedBytes(long maxQueuedBytes)
  {
    return new QueryPlus<>(
        query.withOverriddenContext(ImmutableMap.of(QueryContexts.MAX_QUEUED_BYTES_KEY, maxQueuedBytes)),
        queryMetrics,
        identity
    );
  }

  /**
   * Returns a QueryPlus object with {@link QueryMetrics} from this QueryPlus object, and the provided {@link Query}.
   */
  public <U> QueryPlus<U> withQuery(Query<U> replacementQuery)
  {
    return new QueryPlus<>(replacementQuery, queryMetrics, identity);
  }

  public Sequence<T> run(QuerySegmentWalker walker, ResponseContext context)
  {
    return query.getRunner(walker).run(this, context);
  }

  public QueryPlus<T> optimizeForSegment(PerSegmentQueryOptimizationContext optimizationContext)
  {
    return new QueryPlus<>(query.optimizeForSegment(optimizationContext), queryMetrics, identity);
  }
}
