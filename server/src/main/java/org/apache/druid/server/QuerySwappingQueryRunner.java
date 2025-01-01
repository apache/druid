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

package org.apache.druid.server;

import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.context.ResponseContext;

/**
 * A {@link QueryRunner} which validates that a *specific* query is passed in, and then swaps it with another one.
 * Useful in passing the modified query to the underlying runners, since the {@link QuerySegmentWalker#}
 * Useful since walkers might need to enrich the query with additional parameters and the callers actually calling the
 * `run()` won't know about this modification.
 *
 * It validates that the query passed to the `run()` was the same query that was passed while creating the runner,
 * to ensure that the enrichment was done to the correct query.
 */
public class QuerySwappingQueryRunner<T> implements QueryRunner<T>
{
  private final QueryRunner<T> baseRunner;
  private final Query<T> query;
  private final Query<T> newQuery;

  public QuerySwappingQueryRunner(QueryRunner<T> baseRunner, Query<T> query, Query<T> newQuery)
  {
    this.baseRunner = baseRunner;
    this.query = query;
    this.newQuery = newQuery;
  }

  @Override
  public Sequence<T> run(QueryPlus<T> queryPlus, ResponseContext responseContext)
  {
    //noinspection ObjectEquality
    if (queryPlus.getQuery() != query) {
      throw DruidException.defensive("Unexpected query received");
    }

    return baseRunner.run(queryPlus.withQuery(newQuery), responseContext);
  }
}
