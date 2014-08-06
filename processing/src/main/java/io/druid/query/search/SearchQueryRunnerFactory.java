/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query.search;

import com.google.inject.Inject;
import io.druid.query.ChainedExecutionQueryRunner;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryWatcher;
import io.druid.query.Result;
import io.druid.query.search.search.SearchQuery;
import io.druid.segment.Segment;

import java.util.concurrent.ExecutorService;

/**
 */
public class SearchQueryRunnerFactory implements QueryRunnerFactory<Result<SearchResultValue>, SearchQuery>
{
  private final SearchQueryQueryToolChest toolChest;
  private final QueryWatcher queryWatcher;

  @Inject
  public SearchQueryRunnerFactory(
      SearchQueryQueryToolChest toolChest,
      QueryWatcher queryWatcher
  )
  {
    this.toolChest = toolChest;
    this.queryWatcher = queryWatcher;
  }

  @Override
  public QueryRunner<Result<SearchResultValue>> createRunner(final Segment segment)
  {
    return new SearchQueryRunner(segment);
  }

  @Override
  public QueryRunner<Result<SearchResultValue>> mergeRunners(
      ExecutorService queryExecutor, Iterable<QueryRunner<Result<SearchResultValue>>> queryRunners
  )
  {
    return new ChainedExecutionQueryRunner<Result<SearchResultValue>>(
        queryExecutor, toolChest.getOrdering(), queryWatcher, queryRunners
    );
  }

  @Override
  public QueryToolChest<Result<SearchResultValue>, SearchQuery> getToolchest()
  {
    return toolChest;
  }
}
