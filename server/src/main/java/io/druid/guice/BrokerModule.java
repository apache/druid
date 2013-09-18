/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.guice;

import com.google.common.base.Supplier;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import io.druid.client.BrokerServerView;
import io.druid.client.CachingClusteredClient;
import io.druid.client.TimelineServerView;
import io.druid.client.cache.Cache;
import io.druid.client.cache.CacheProvider;
import io.druid.collections.ResourceHolder;
import io.druid.collections.StupidPool;
import io.druid.guice.annotations.Global;
import io.druid.query.MapQueryToolChestWarehouse;
import io.druid.query.QueryToolChestWarehouse;

import java.nio.ByteBuffer;

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

    // This is a workaround and needs to be made better in the near future.
    binder.bind(
        new TypeLiteral<StupidPool<ByteBuffer>>()
        {
        }
    ).annotatedWith(Global.class).toInstance(new NoopStupidPool(null));
  }

  private static class NoopStupidPool extends StupidPool<ByteBuffer>
  {
    public NoopStupidPool(Supplier<ByteBuffer> generator)
    {
      super(generator);
    }

    @Override
    public ResourceHolder<ByteBuffer> take()
    {
      throw new UnsupportedOperationException();
    }
  }
}
