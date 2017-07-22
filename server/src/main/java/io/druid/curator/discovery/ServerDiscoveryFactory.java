/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.curator.discovery;

import com.google.inject.Inject;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceProvider;

import java.io.IOException;
import java.util.Collection;

/**
 */
public class ServerDiscoveryFactory
{
  private final ServiceDiscovery<Void> serviceDiscovery;

  @Inject
  public ServerDiscoveryFactory(
      ServiceDiscovery<Void> serviceDiscovery
  )
  {
    this.serviceDiscovery = serviceDiscovery;
  }

  public ServerDiscoverySelector createSelector(String serviceName)
  {
    if (serviceName == null) {
      return new ServerDiscoverySelector(new NoopServiceProvider());
    }

    final ServiceProvider serviceProvider = serviceDiscovery
        .serviceProviderBuilder()
        .serviceName(CuratorServiceUtils.makeCanonicalServiceName(serviceName))
        .build();
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
    public Collection<ServiceInstance<T>> getAllInstances() throws Exception
    {
      return null;
    }

    @Override
    public void noteError(ServiceInstance<T> tServiceInstance)
    {
      // do nothing
    }

    @Override
    public void close() throws IOException
    {
      // do nothing
    }
  }

}
