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

package io.druid.server;

import io.druid.java.util.common.guava.Sequence;
import io.druid.query.Query;
import io.druid.query.QueryContexts;
import io.druid.query.QueryPlus;
import io.druid.query.QueryRunner;
import io.druid.server.initialization.ServerConfig;

import java.util.Map;

/**
 * Use this QueryRunner to set and verify Query contexts.
 */
public class SetAndVerifyContextQueryRunner implements QueryRunner
{
  private final ServerConfig serverConfig;
  private final QueryRunner baseRunner;

  public SetAndVerifyContextQueryRunner(ServerConfig serverConfig, QueryRunner baseRunner)
  {
    this.serverConfig = serverConfig;
    this.baseRunner = baseRunner;
  }

  @Override
  public Sequence run(QueryPlus queryPlus, Map responseContext)
  {
    return baseRunner.run(
        QueryPlus.wrap((Query) withTimeoutAndMaxScatterGatherBytes(
            queryPlus.getQuery(),
            serverConfig
        )),
        responseContext
    );
  }

  public static <T, QueryType extends Query<T>> QueryType withTimeoutAndMaxScatterGatherBytes(
      final QueryType query,
      ServerConfig serverConfig
  )
  {
    return (QueryType) QueryContexts.verifyMaxQueryTimeout(
        QueryContexts.withMaxScatterGatherBytes(
            QueryContexts.withDefaultTimeout(
                (Query) query,
                Math.min(serverConfig.getDefaultQueryTimeout(), serverConfig.getMaxQueryTimeout())
            ),
            serverConfig.getMaxScatterGatherBytes()
        ),
        serverConfig.getMaxQueryTimeout()
    );
  }
}
