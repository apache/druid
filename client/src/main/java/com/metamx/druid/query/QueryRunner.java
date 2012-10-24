package com.metamx.druid.query;

import com.metamx.common.guava.Sequence;
import com.metamx.druid.Query;

/**
 */
public interface QueryRunner<T>
{
  public Sequence<T> run(Query<T> query);
}
