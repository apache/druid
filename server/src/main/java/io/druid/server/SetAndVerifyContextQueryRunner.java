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

package io.druid.server;

import io.druid.java.util.common.guava.Sequence;
import io.druid.query.Query;
import io.druid.query.QueryContexts;
import io.druid.query.QueryPlus;
import io.druid.query.QueryRunner;
import io.druid.server.initialization.ServerConfig;
import io.druid.client.DirectDruidClient;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Use this QueryRunner to set and verify Query contexts.
 */
public class SetAndVerifyContextQueryRunner<T> implements QueryRunner<T>
{
  private final ServerConfig serverConfig;
  private final QueryRunner<T> baseRunner;
  private final long startTimeMillis;

  public SetAndVerifyContextQueryRunner(ServerConfig serverConfig, QueryRunner<T> baseRunner)
  {
    this.serverConfig = serverConfig;
    this.baseRunner = baseRunner;
    this.startTimeMillis = System.currentTimeMillis();
  }

  @Override
  public Sequence<T> run(QueryPlus<T> queryPlus, Map<String, Object> responseContext)
  {
    return baseRunner.run(
        QueryPlus.wrap(withTimeoutAndMaxScatterGatherBytes(queryPlus.getQuery(), serverConfig)),
        responseContext
    );
  }

  public Query<T> withTimeoutAndMaxScatterGatherBytes(Query<T> query, ServerConfig serverConfig)
  {
    Query<T> newQuery = QueryContexts.verifyMaxQueryTimeout(
        QueryContexts.withMaxScatterGatherBytes(
            QueryContexts.withDefaultTimeout(
                query,
                Math.min(serverConfig.getDefaultQueryTimeout(), serverConfig.getMaxQueryTimeout())
            ),
            serverConfig.getMaxScatterGatherBytes()
        ),
        serverConfig.getMaxQueryTimeout()
    );
    return newQuery.withOverriddenContext(ImmutableMap.of(DirectDruidClient.QUERY_FAIL_TIME, this.startTimeMillis + QueryContexts.getTimeout(newQuery)));
  }
}
