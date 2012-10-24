package com.metamx.druid.query;

import com.google.common.base.Function;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.druid.Query;

/**
*/
public class ConcatQueryRunner<T> implements QueryRunner<T>
{
  private final Sequence<QueryRunner<T>> queryRunners;

  public ConcatQueryRunner(
      Sequence<QueryRunner<T>> queryRunners
  ) {
    this.queryRunners = queryRunners;
  }

  @Override
  public Sequence<T> run(final Query<T> query)
  {
    return Sequences.concat(
        Sequences.map(
            queryRunners,
            new Function<QueryRunner<T>, Sequence<T>>()
            {
              @Override
              public Sequence<T> apply(final QueryRunner<T> input)
              {
                return input.run(query);
              }
            }
        )
    );
  }
}
