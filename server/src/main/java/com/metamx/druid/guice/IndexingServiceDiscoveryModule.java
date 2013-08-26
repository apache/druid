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

package com.metamx.druid.guice;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.metamx.druid.client.indexing.IndexingService;
import com.metamx.druid.client.indexing.IndexingServiceSelector;
import com.metamx.druid.client.indexing.IndexingServiceSelectorConfig;
import com.metamx.druid.client.selector.DiscoverySelector;
import com.metamx.druid.client.selector.Server;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceProvider;

import java.io.IOException;

/**
 */
public class IndexingServiceDiscoveryModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.selectors.indexing", IndexingServiceSelectorConfig.class);
    binder.bind(new TypeLiteral<DiscoverySelector<Server>>(){})
          .annotatedWith(IndexingService.class)
          .to(IndexingServiceSelector.class);

    binder.bind(IndexingServiceSelector.class).in(ManageLifecycle.class);
  }

  @Provides
  @LazySingleton @IndexingService
  public ServiceProvider getServiceProvider(
      IndexingServiceSelectorConfig config,
      ServiceDiscovery<Void> serviceDiscovery
  )
  {
    if (config.getServiceName() == null) {
      return new ServiceProvider()
      {
        @Override
        public void start() throws Exception
        {

        }

        @Override
        public ServiceInstance getInstance() throws Exception
        {
          return null;
        }

        @Override
        public void close() throws IOException
        {

        }
      };
    }
    return serviceDiscovery.serviceProviderBuilder().serviceName(config.getServiceName()).build();
  }
}
