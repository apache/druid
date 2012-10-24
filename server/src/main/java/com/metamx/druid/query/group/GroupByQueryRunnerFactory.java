package com.metamx.druid.query.group;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.metamx.common.ISE;
import com.metamx.common.guava.ExecutorExecutingSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.druid.GroupByQueryEngine;
import com.metamx.druid.Query;
import com.metamx.druid.StorageAdapter;
import com.metamx.druid.input.Row;
import com.metamx.druid.query.ConcatQueryRunner;
import com.metamx.druid.query.QueryRunner;
import com.metamx.druid.query.QueryRunnerFactory;
import com.metamx.druid.query.QueryToolChest;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 */
public class GroupByQueryRunnerFactory implements QueryRunnerFactory<Row, GroupByQuery>
{
  private static final GroupByQueryQueryToolChest toolChest = new GroupByQueryQueryToolChest(){
    @Override
    public QueryRunner<Row> mergeResults(QueryRunner<Row> runner)
    {
      return new ConcatQueryRunner<Row>(Sequences.simple(ImmutableList.of(runner)));
    }
  };

  private final GroupByQueryEngine engine;

  public GroupByQueryRunnerFactory(
      GroupByQueryEngine engine
  )
  {
    this.engine = engine;
  }

  @Override
  public QueryRunner<Row> createRunner(final StorageAdapter adapter)
  {
    return new QueryRunner<Row>()
    {
      @Override
      public Sequence<Row> run(Query<Row> input)
      {
        if (! (input instanceof GroupByQuery)) {
          throw new ISE("Got a [%s] which isn't a %s", input.getClass(), GroupByQuery.class);
        }

        GroupByQuery query = (GroupByQuery) input;

        return engine.process(query, adapter);
      }
    };
  }

  @Override
  public QueryRunner<Row> mergeRunners(final ExecutorService queryExecutor, Iterable<QueryRunner<Row>> queryRunners)
  {
    return new ConcatQueryRunner<Row>(
        Sequences.map(
            Sequences.simple(queryRunners),
            new Function<QueryRunner<Row>, QueryRunner<Row>>()
            {
              @Override
              public QueryRunner<Row> apply(final QueryRunner<Row> input)
              {
                return new QueryRunner<Row>()
                {
                  @Override
                  public Sequence<Row> run(final Query<Row> query)
                  {

                    Future<Sequence<Row>> future = queryExecutor.submit(
                        new Callable<Sequence<Row>>()
                        {
                          @Override
                          public Sequence<Row> call() throws Exception
                          {
                            return new ExecutorExecutingSequence<Row>(
                                input.run(query),
                                queryExecutor
                            );
                          }
                        }
                    );
                    try {
                      return future.get();
                    }
                    catch (InterruptedException e) {
                      throw Throwables.propagate(e);
                    }
                    catch (ExecutionException e) {
                      throw Throwables.propagate(e);
                    }
                  }
                };
              }
            }
        )
    );
  }

  @Override
  public QueryToolChest getToolchest()
  {
    return toolChest;
  }
}
