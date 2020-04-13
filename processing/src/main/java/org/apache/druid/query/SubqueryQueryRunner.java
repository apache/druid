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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.groupby.GroupByQueryConfig;

/**
 * If there's a subquery, run it instead of the outer query
 */
public class SubqueryQueryRunner<T> implements QueryRunner<T>
{
  private final QueryRunner<T> baseRunner;

  public SubqueryQueryRunner(QueryRunner<T> baseRunner)
  {
    this.baseRunner = baseRunner;
  }

  @Override
  public Sequence<T> run(final QueryPlus<T> queryPlus, ResponseContext responseContext)
  {
    DataSource dataSource = queryPlus.getQuery().getDataSource();
    boolean forcePushDownNestedQuery = queryPlus.getQuery()
                                                .getContextBoolean(
                                                    GroupByQueryConfig.CTX_KEY_FORCE_PUSH_DOWN_NESTED_QUERY,
                                                    false
                                                );
    if (dataSource instanceof QueryDataSource && !forcePushDownNestedQuery) {
      return run(queryPlus.withQuery((Query<T>) ((QueryDataSource) dataSource).getQuery()), responseContext);
    } else {
      QueryPlus newQuery = queryPlus;
      if (forcePushDownNestedQuery) {
        // Disable any more push downs before firing off the query. But do let the historical know
        // that it is executing the complete nested query and not just the inner most part of it
        newQuery = queryPlus.withQuery(
            queryPlus.getQuery()
                     .withOverriddenContext(
                         ImmutableMap.of(
                             GroupByQueryConfig.CTX_KEY_FORCE_PUSH_DOWN_NESTED_QUERY, false,
                             GroupByQueryConfig.CTX_KEY_EXECUTING_NESTED_QUERY, true
                         )
                     )
        );
      }
      return baseRunner.run(newQuery, responseContext);
    }
  }
}
