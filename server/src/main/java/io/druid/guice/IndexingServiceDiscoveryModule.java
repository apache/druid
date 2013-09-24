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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import io.druid.client.indexing.IndexingService;
import io.druid.client.indexing.IndexingServiceSelectorConfig;
import io.druid.curator.discovery.ServerDiscoveryFactory;
import io.druid.curator.discovery.ServerDiscoverySelector;

/**
 */
public class IndexingServiceDiscoveryModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.selectors.indexing", IndexingServiceSelectorConfig.class);
  }

  @Provides
  @IndexingService
  @ManageLifecycle
  public ServerDiscoverySelector getServiceProvider(
      IndexingServiceSelectorConfig config,
      ServerDiscoveryFactory serverDiscoveryFactory
  )
  {
    return serverDiscoveryFactory.createSelector(config.getServiceName());
  }
}
