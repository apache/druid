package com.metamx.druid.query;

import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.druid.Query;

/**
*/
public class NoopQueryRunner<T> implements QueryRunner<T>
{
  @Override
  public Sequence<T> run(Query query)
  {
    return Sequences.empty();
  }
}
