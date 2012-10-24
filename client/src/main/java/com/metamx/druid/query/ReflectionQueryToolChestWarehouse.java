package com.metamx.druid.query;

import com.metamx.druid.Query;

/**
 */
public class ReflectionQueryToolChestWarehouse implements QueryToolChestWarehouse
{
  ReflectionLoaderThingy<QueryToolChest> loader = ReflectionLoaderThingy.create(QueryToolChest.class);

  @Override
  @SuppressWarnings("unchecked")
  public <T, QueryType extends Query<T>> QueryToolChest<T, QueryType> getToolChest(QueryType query)
  {
    return (QueryToolChest<T, QueryType>) loader.getForObject(query);
  }
}
