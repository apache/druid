package com.metamx.druid.query;

import com.google.common.collect.Ordering;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.nary.BinaryFn;
import com.metamx.druid.Query;
import com.metamx.druid.guava.CombiningSequence;

/**
 */
public abstract class ResultMergeQueryRunner<T> extends BySegmentSkippingQueryRunner<T>
{
  public ResultMergeQueryRunner(
      QueryRunner<T> baseRunner
  )
  {
    super(baseRunner);
  }

  @Override
  public Sequence<T> doRun(QueryRunner<T> baseRunner, Query<T> query)
  {
    return CombiningSequence.create(baseRunner.run(query), makeOrdering(query), createMergeFn(query));
  }

  protected abstract Ordering<T> makeOrdering(Query<T> query);

  protected abstract BinaryFn<T,T,T> createMergeFn(Query<T> query);
}
