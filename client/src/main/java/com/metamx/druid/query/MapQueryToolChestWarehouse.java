package com.metamx.druid.query;

import com.google.inject.Inject;
import io.druid.query.Query;
import io.druid.query.QueryToolChest;

import java.util.Map;

/**
 */
public class MapQueryToolChestWarehouse implements QueryToolChestWarehouse
{
  private final Map<Class<? extends Query>, QueryToolChest> toolchests;

  @Inject
  public MapQueryToolChestWarehouse(
      Map<Class<? extends Query>, QueryToolChest> toolchests
  )
  {
    this.toolchests = toolchests;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T, QueryType extends Query<T>> QueryToolChest<T, QueryType> getToolChest(QueryType query)
  {
    return toolchests.get(query.getClass());
  }
}
