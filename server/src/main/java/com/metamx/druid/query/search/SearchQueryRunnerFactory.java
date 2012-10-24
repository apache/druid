package com.metamx.druid.query.search;

import com.google.common.collect.Lists;
import com.metamx.common.ISE;
import com.metamx.common.guava.BaseSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.druid.Query;
import com.metamx.druid.SearchResultBuilder;
import com.metamx.druid.StorageAdapter;
import com.metamx.druid.index.brita.Filters;
import com.metamx.druid.query.ChainedExecutionQueryRunner;
import com.metamx.druid.query.QueryRunner;
import com.metamx.druid.query.QueryRunnerFactory;
import com.metamx.druid.query.QueryToolChest;
import com.metamx.druid.query.group.GroupByQuery;
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
  public QueryRunner<Result<SearchResultValue>> createRunner(final StorageAdapter adapter)
  {
    return new QueryRunner<Result<SearchResultValue>>()
    {
      @Override
      public Sequence<Result<SearchResultValue>> run(final Query<Result<SearchResultValue>> input)
      {
        if (!(input instanceof SearchQuery)) {
          throw new ISE("Got a [%s] which isn't a %s", input.getClass(), GroupByQuery.class);
        }

        final SearchQuery query = (SearchQuery) input;

        return new BaseSequence<Result<SearchResultValue>, Iterator<Result<SearchResultValue>>>(
            new BaseSequence.IteratorMaker<Result<SearchResultValue>, Iterator<Result<SearchResultValue>>>()
            {
              @Override
              public Iterator<Result<SearchResultValue>> make()
              {
                return Lists.newArrayList(
                    new SearchResultBuilder(
                        adapter.getInterval().getStart(),
                        adapter.searchDimensions(
                            query,
                            Filters.convertDimensionFilters(query.getDimensionsFilter())
                        )
                    ).build()
                ).iterator();
              }

              @Override
              public void cleanup(Iterator<Result<SearchResultValue>> toClean)
              {

              }
            }
        );
      }
    };

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
}
