package com.metamx.druid.query;

import com.metamx.druid.Query;

import java.util.Map;

/**
*/
public class DefaultQueryRunnerFactoryConglomerate implements QueryRunnerFactoryConglomerate
{
  private final Map<Class<? extends Query>, QueryRunnerFactory> factories;

  public DefaultQueryRunnerFactoryConglomerate(
      Map<Class<? extends Query>, QueryRunnerFactory> factories
  )
  {
    this.factories = factories;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T, QueryType extends Query<T>> QueryRunnerFactory<T, QueryType> findFactory(QueryType query)
  {
    return (QueryRunnerFactory<T, QueryType>) factories.get(query.getClass());
  }
}
