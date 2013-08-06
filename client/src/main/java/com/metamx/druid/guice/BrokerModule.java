package com.metamx.druid.guice;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.metamx.druid.client.BrokerServerView;
import com.metamx.druid.client.CachingClusteredClient;
import com.metamx.druid.client.TimelineServerView;
import com.metamx.druid.client.cache.Cache;
import com.metamx.druid.client.cache.CacheProvider;
import com.metamx.druid.query.MapQueryToolChestWarehouse;
import com.metamx.druid.query.QueryToolChestWarehouse;

/**
 */
public class BrokerModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    binder.bind(QueryToolChestWarehouse.class).to(MapQueryToolChestWarehouse.class);

    binder.bind(CachingClusteredClient.class).in(LazySingleton.class);
    binder.bind(TimelineServerView.class).to(BrokerServerView.class).in(LazySingleton.class);

    binder.bind(Cache.class).toProvider(CacheProvider.class).in(ManageLifecycle.class);
    JsonConfigProvider.bind(binder, "druid.broker.cache", CacheProvider.class);
  }
}
