package com.metamx.druid.query;

import com.metamx.druid.Query;

/**
*/
public class NoopQueryRunnerFactoryConglomerate implements QueryRunnerFactoryConglomerate
{
  @Override
  public <T, QueryType extends Query<T>> QueryRunnerFactory<T, QueryType> findFactory(QueryType query)
  {
    throw new UnsupportedOperationException();
  }
}
