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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.net.HostAndPort;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceProvider;
import org.apache.druid.client.selector.DiscoverySelector;
import org.apache.druid.client.selector.Server;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 * Use {@link org.apache.druid.discovery.DruidNodeDiscovery} for discovery.
 */
@Deprecated
public class ServerDiscoverySelector implements DiscoverySelector<Server>
{
  private static final Logger log = new Logger(ServerDiscoverySelector.class);

  private final ServiceProvider serviceProvider;
  private final String name;

  public ServerDiscoverySelector(ServiceProvider serviceProvider, String name)
  {
    this.serviceProvider = serviceProvider;
    this.name = name;
  }

  private static final Function<ServiceInstance, Server> TO_SERVER = new Function<ServiceInstance, Server>()
  {
    @Override
    public Server apply(final ServiceInstance instance)
    {
      Preconditions.checkState(
          instance.getPort() >= 0 || (instance.getSslPort() != null && instance.getSslPort() >= 0),
          "WTH?! Both port and sslPort not set"
      );
      final int port;
      final String scheme;
      if (instance.getSslPort() == null) {
        port = instance.getPort();
        scheme = "http";
      } else {
        port = instance.getSslPort() >= 0 ? instance.getSslPort() : instance.getPort();
        scheme = instance.getSslPort() >= 0 ? "https" : "http";
      }
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
          return port;
        }

        @Override
        public String getScheme()
        {
          return scheme;
        }
      };
    }
  };

  @Nullable
  @Override
  public Server pick()
  {
    final ServiceInstance instance;
    try {
      instance = serviceProvider.getInstance();
    }
    catch (Exception e) {
      log.info(e, "Exception getting instance for [%s]", name);
      return null;
    }

    if (instance == null) {
      log.error("No server instance found for [%s]", name);
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
