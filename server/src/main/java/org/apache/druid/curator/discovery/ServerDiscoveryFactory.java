/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.curator.discovery;

import com.google.inject.Inject;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceProvider;

import java.util.Collection;

/**
 * Use {@link org.apache.druid.discovery.DruidNodeDiscovery} for discovery.
 */
@Deprecated
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
      return new ServerDiscoverySelector(new NoopServiceProvider(), serviceName);
    }

    final ServiceProvider serviceProvider = serviceDiscovery
        .serviceProviderBuilder()
        .serviceName(CuratorServiceUtils.makeCanonicalServiceName(serviceName))
        .build();
    return new ServerDiscoverySelector(serviceProvider, serviceName);
  }

  private static class NoopServiceProvider<T> implements ServiceProvider<T>
  {
    @Override
    public void start()
    {
      // do nothing
    }

    @Override
    public ServiceInstance<T> getInstance()
    {
      return null;
    }

    @Override
    public Collection<ServiceInstance<T>> getAllInstances()
    {
      return null;
    }

    @Override
    public void noteError(ServiceInstance<T> tServiceInstance)
    {
      // do nothing
    }

    @Override
    public void close()
    {
      // do nothing
    }
  }

}
