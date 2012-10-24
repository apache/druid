package com.metamx.druid.query;

import com.metamx.common.guava.Sequence;
import com.metamx.druid.Query;

/**
 */
public abstract class BySegmentSkippingQueryRunner<T> implements QueryRunner<T>
{
  private final QueryRunner<T> baseRunner;

  public BySegmentSkippingQueryRunner(
      QueryRunner<T> baseRunner
  )
  {
    this.baseRunner = baseRunner;
  }

  @Override
  public Sequence<T> run(Query<T> query)
  {
    if (Boolean.parseBoolean(query.getContextValue("bySegment"))) {
      return baseRunner.run(query);
    }

    return doRun(baseRunner, query);
  }

  protected abstract Sequence<T> doRun(QueryRunner<T> baseRunner, Query<T> query);
}
