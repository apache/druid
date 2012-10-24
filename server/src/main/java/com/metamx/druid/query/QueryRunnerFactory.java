package com.metamx.druid.query;

import com.metamx.druid.Query;
import com.metamx.druid.StorageAdapter;

import java.util.concurrent.ExecutorService;

/**
 */
public interface QueryRunnerFactory<T, QueryType extends Query<T>>
{
  public QueryRunner<T> createRunner(StorageAdapter adapter);
  public QueryRunner<T> mergeRunners(ExecutorService queryExecutor, Iterable<QueryRunner<T>> queryRunners);
  public QueryToolChest<T, QueryType> getToolchest();
}
