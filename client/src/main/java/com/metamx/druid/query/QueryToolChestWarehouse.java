package com.metamx.druid.query;

import com.metamx.druid.Query;

/**
 */
public interface QueryToolChestWarehouse
{
  public <T, QueryType extends Query<T>> QueryToolChest<T, QueryType> getToolChest(QueryType query);
}
