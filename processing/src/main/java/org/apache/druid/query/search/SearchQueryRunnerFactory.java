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

package org.apache.druid.query.search;

import com.google.inject.Inject;
import org.apache.druid.query.ChainedExecutionQueryRunner;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryWatcher;
import org.apache.druid.query.Result;
import org.apache.druid.segment.Segment;

import java.util.concurrent.ExecutorService;

/**
 */
public class SearchQueryRunnerFactory implements QueryRunnerFactory<Result<SearchResultValue>, SearchQuery>
{
  private final SearchStrategySelector strategySelector;
  private final SearchQueryQueryToolChest toolChest;
  private final QueryWatcher queryWatcher;

  @Inject
  public SearchQueryRunnerFactory(
      SearchStrategySelector strategySelector,
      SearchQueryQueryToolChest toolChest,
      QueryWatcher queryWatcher
  )
  {
    this.strategySelector = strategySelector;
    this.toolChest = toolChest;
    this.queryWatcher = queryWatcher;
  }

  @Override
  public QueryRunner<Result<SearchResultValue>> createRunner(final Segment segment)
  {
    return new SearchQueryRunner(segment, strategySelector);
  }

  @Override
  public QueryRunner<Result<SearchResultValue>> mergeRunners(
      ExecutorService queryExecutor,
      Iterable<QueryRunner<Result<SearchResultValue>>> queryRunners
  )
  {
    return new ChainedExecutionQueryRunner<>(queryExecutor, queryWatcher, queryRunners);
  }

  @Override
  public QueryToolChest<Result<SearchResultValue>, SearchQuery> getToolchest()
  {
    return toolChest;
  }
}
