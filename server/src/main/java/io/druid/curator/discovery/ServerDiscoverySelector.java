/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.curator.discovery;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.net.HostAndPort;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import io.druid.client.selector.DiscoverySelector;
import io.druid.client.selector.Server;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceProvider;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 */
public class ServerDiscoverySelector implements DiscoverySelector<Server>
{
  private static final Logger log = new Logger(ServerDiscoverySelector.class);

  private final ServiceProvider serviceProvider;

  public ServerDiscoverySelector(ServiceProvider serviceProvider)
  {
    this.serviceProvider = serviceProvider;
  }

  private static final Function<ServiceInstance, Server> TO_SERVER = new Function<ServiceInstance, Server>()
  {
    @Override
    public Server apply(final ServiceInstance instance)
    {
      return new Server()
      {
        @Override
        public String getHost()
        {
          return HostAndPort.fromParts(getAddress(), getPort()).toString();
        }

        @Override
        public String getAddress()
        {
          return instance.getAddress();
        }

        @Override
        public int getPort()
        {
          return instance.getPort();
        }

        @Override
        public String getScheme()
        {
          return "http";
        }
      };
    }
  };

  @Override
  public Server pick()
  {
    final ServiceInstance instance;
    try {
      instance = serviceProvider.getInstance();
    }
    catch (Exception e) {
      log.info(e, "Exception getting instance");
      return null;
    }

    if (instance == null) {
      log.error("No server instance found");
      return null;
    }

    return TO_SERVER.apply(instance);
  }

  public Collection<Server> getAll()
  {
    try {
      return Collections2.transform(serviceProvider.getAllInstances(), TO_SERVER);
    }
    catch (Exception e) {
      log.info(e, "Unable to get all instances");
      return Collections.emptyList();
    }
  }

  @LifecycleStart
  public void start() throws Exception
  {
    serviceProvider.start();
  }

  @LifecycleStop
  public void stop() throws IOException
  {
    serviceProvider.close();
  }
}
