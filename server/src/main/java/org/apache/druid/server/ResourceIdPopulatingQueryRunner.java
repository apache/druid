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

import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.context.ResponseContext;

import java.util.Collections;
import java.util.UUID;

/**
 * Populates {@link QueryContexts#QUERY_RESOURCE_ID} in the query context
 */
public class ResourceIdPopulatingQueryRunner<T> implements QueryRunner<T>
{
  private final QueryRunner<T> baseRunner;

  public ResourceIdPopulatingQueryRunner(QueryRunner<T> baseRunner)
  {
    this.baseRunner = baseRunner;
  }

  /**
   * Assigns a random resource id to the given query
   */
  public static <T> Query<T> populateResourceId(Query<T> query)
  {
    return query.withOverriddenContext(Collections.singletonMap(
        QueryContexts.QUERY_RESOURCE_ID,
        UUID.randomUUID().toString()
    ));
  }

  @Override
  public Sequence<T> run(
      final QueryPlus<T> queryPlus,
      final ResponseContext responseContext
  )
  {
    return baseRunner.run(
        queryPlus.withQuery(
            populateResourceId(queryPlus.getQuery())
        ),
        responseContext
    );
  }
}
