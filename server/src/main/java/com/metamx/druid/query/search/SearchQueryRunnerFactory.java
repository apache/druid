/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.query.search;

import com.google.common.collect.Iterators;
import com.metamx.common.ISE;
import com.metamx.common.guava.BaseSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.druid.Query;
import com.metamx.druid.SearchResultBuilder;
import com.metamx.druid.StorageAdapter;
import com.metamx.druid.index.Segment;
import com.metamx.druid.index.brita.Filters;
import com.metamx.druid.query.ChainedExecutionQueryRunner;
import com.metamx.druid.query.QueryRunner;
import com.metamx.druid.query.QueryRunnerFactory;
import com.metamx.druid.query.QueryToolChest;
import com.metamx.druid.result.Result;
import com.metamx.druid.result.SearchResultValue;

import java.util.Iterator;
import java.util.concurrent.ExecutorService;

/**
 */
public class SearchQueryRunnerFactory implements QueryRunnerFactory<Result<SearchResultValue>, SearchQuery>
{
  private static final SearchQueryQueryToolChest toolChest = new SearchQueryQueryToolChest();

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
        queryExecutor, toolChest.getOrdering(), queryRunners
    );
  }

  @Override
  public QueryToolChest<Result<SearchResultValue>, SearchQuery> getToolchest()
  {
    return toolChest;
  }

  private static class SearchQueryRunner implements QueryRunner<Result<SearchResultValue>>
  {
    private final StorageAdapter adapter;

    public SearchQueryRunner(Segment segment)
    {
      this.adapter = segment.asStorageAdapter();
    }

    @Override
    public Sequence<Result<SearchResultValue>> run(final Query<Result<SearchResultValue>> input)
    {
      if (!(input instanceof SearchQuery)) {
        throw new ISE("Got a [%s] which isn't a %s", input.getClass(), SearchQuery.class);
      }

      final SearchQuery query = (SearchQuery) input;

      return new BaseSequence<Result<SearchResultValue>, Iterator<Result<SearchResultValue>>>(
          new BaseSequence.IteratorMaker<Result<SearchResultValue>, Iterator<Result<SearchResultValue>>>()
          {
            @Override
            public Iterator<Result<SearchResultValue>> make()
            {
              return Iterators.singletonIterator(
                  new SearchResultBuilder(
                      adapter.getInterval().getStart(),
                      adapter.searchDimensions(
                          query,
                          Filters.convertDimensionFilters(query.getDimensionsFilter())
                      )
                  ).build()
              );
            }

            @Override
            public void cleanup(Iterator<Result<SearchResultValue>> toClean)
            {

            }
          }
      );
    }
  }
}
