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

package io.druid.curator.discovery;

import com.google.inject.Inject;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceProvider;

import java.io.IOException;

/**
 */
public class ServerDiscoveryFactory
{
  private final ServiceDiscovery<Void> serviceDiscovery;

  @Inject
  public ServerDiscoveryFactory(ServiceDiscovery<Void> serviceDiscovery)
  {
    this.serviceDiscovery = serviceDiscovery;
  }

  public ServerDiscoverySelector createSelector(String serviceName)
  {
    if (serviceName == null) {
      return new ServerDiscoverySelector(new NoopServiceProvider());
    }

    final ServiceProvider serviceProvider = serviceDiscovery.serviceProviderBuilder().serviceName(serviceName).build();
    return new ServerDiscoverySelector(serviceProvider);
  }

  private static class NoopServiceProvider<T> implements ServiceProvider<T>
  {
    @Override
    public void start() throws Exception
    {
      // do nothing
    }

    @Override
    public ServiceInstance<T> getInstance() throws Exception
    {
      return null;
    }

    @Override
    public void noteError(ServiceInstance<T> tServiceInstance) {
      // do nothing
    }

    @Override
    public void close() throws IOException
    {
      // do nothing
    }
  }

}
