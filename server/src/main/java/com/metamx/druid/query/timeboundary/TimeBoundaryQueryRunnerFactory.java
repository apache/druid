package com.metamx.druid.query.timeboundary;

import com.metamx.common.ISE;
import com.metamx.common.guava.BaseSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.druid.Query;
import com.metamx.druid.StorageAdapter;
import com.metamx.druid.collect.StupidPool;
import com.metamx.druid.query.ChainedExecutionQueryRunner;
import com.metamx.druid.query.QueryRunner;
import com.metamx.druid.query.QueryRunnerFactory;
import com.metamx.druid.query.QueryToolChest;
import com.metamx.druid.query.group.GroupByQuery;
import com.metamx.druid.result.Result;
import com.metamx.druid.result.TimeBoundaryResultValue;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;

/**
 */
public class TimeBoundaryQueryRunnerFactory
    implements QueryRunnerFactory<Result<TimeBoundaryResultValue>, TimeBoundaryQuery>
{
  private static final TimeBoundaryQueryQueryToolChest toolChest = new TimeBoundaryQueryQueryToolChest();

  @Override
  public QueryRunner<Result<TimeBoundaryResultValue>> createRunner(final StorageAdapter adapter)
  {
    return new QueryRunner<Result<TimeBoundaryResultValue>>()
    {
      @Override
      public Sequence<Result<TimeBoundaryResultValue>> run(Query<Result<TimeBoundaryResultValue>> input)
      {
        if (!(input instanceof TimeBoundaryQuery)) {
          throw new ISE("Got a [%s] which isn't a %s", input.getClass(), GroupByQuery.class);
        }

        final TimeBoundaryQuery legacyQuery = (TimeBoundaryQuery) input;

        return new BaseSequence<Result<TimeBoundaryResultValue>, Iterator<Result<TimeBoundaryResultValue>>>(
            new BaseSequence.IteratorMaker<Result<TimeBoundaryResultValue>, Iterator<Result<TimeBoundaryResultValue>>>()
            {
              @Override
              public Iterator<Result<TimeBoundaryResultValue>> make()
              {
                return legacyQuery.buildResult(
                        adapter.getInterval().getStart(),
                        adapter.getMinTime(),
                        adapter.getMaxTime()
                ).iterator();
              }

              @Override
              public void cleanup(Iterator<Result<TimeBoundaryResultValue>> toClean)
              {

              }
            }
        );
      }
    };
  }

  @Override
  public QueryRunner<Result<TimeBoundaryResultValue>> mergeRunners(
      ExecutorService queryExecutor, Iterable<QueryRunner<Result<TimeBoundaryResultValue>>> queryRunners
  )
  {
    return new ChainedExecutionQueryRunner<Result<TimeBoundaryResultValue>>(
        queryExecutor, toolChest.getOrdering(), queryRunners
    );
  }

  @Override
  public QueryToolChest<Result<TimeBoundaryResultValue>, TimeBoundaryQuery> getToolchest()
  {
    return toolChest;
  }
}
