package com.metamx.druid.query;

import com.metamx.druid.Query;

/**
 */
public interface QueryRunnerFactoryConglomerate
{
  public <T, QueryType extends Query<T>> QueryRunnerFactory<T, QueryType> findFactory(QueryType query);
}
