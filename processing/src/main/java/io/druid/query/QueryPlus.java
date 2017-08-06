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

import com.google.common.base.Preconditions;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.spec.QuerySegmentSpec;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * An immutable composite object of {@link Query} + extra stuff needed in {@link QueryRunner}s. This "extra stuff"
 * is only {@link QueryMetrics} yet.
 */
public final class QueryPlus<T>
{
  /**
   * Returns the minimum bare QueryPlus object with the given query. {@link #getQueryMetrics()} of the QueryPlus object,
   * returned from this factory method, returns {@code null}.
   */
  public static <T> QueryPlus<T> wrap(Query<T> query)
  {
    Preconditions.checkNotNull(query);
    return new QueryPlus<>(query, null);
  }

  private final Query<T> query;
  private final QueryMetrics<?> queryMetrics;

  private QueryPlus(Query<T> query, QueryMetrics<?> queryMetrics)
  {
    this.query = query;
    this.queryMetrics = queryMetrics;
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
      return new QueryPlus<>(query, ((QueryToolChest) queryToolChest).makeMetrics(query));
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
      return new QueryPlus<>(query, null);
    }
  }

  /**
   * Equivalent of withQuery(getQuery().withQuerySegmentSpec(spec)).
   */
  public QueryPlus<T> withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new QueryPlus<>(query.withQuerySegmentSpec(spec), queryMetrics);
  }

  /**
   * Returns a QueryPlus object with {@link QueryMetrics} from this QueryPlus object, and the provided {@link Query}.
   */
  public <U> QueryPlus<U> withQuery(Query<U> replacementQuery)
  {
    return new QueryPlus<>(replacementQuery, queryMetrics);
  }

  public Sequence<T> run(QuerySegmentWalker walker, Map<String, Object> context)
  {
    if (query instanceof BaseQuery) {
      return ((BaseQuery) query).getQuerySegmentSpec().lookup(query, walker).run(this, context);
    } else {
      // fallback
      return query.run(walker, context);
    }
  }
}
