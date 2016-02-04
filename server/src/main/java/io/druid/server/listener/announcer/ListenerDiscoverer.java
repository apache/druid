/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.server.listener.announcer;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import io.druid.concurrent.Execs;
import io.druid.server.DruidNode;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.x.discovery.ServiceCache;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ListenerDiscoverer
{
  private static final Logger LOG = new Logger(ListenerDiscoverer.class);
  private final ConcurrentMap<String, ServiceCache<Void>> services = new ConcurrentHashMap<>();
  private final ServiceDiscovery<Void> serviceDiscovery;
  private final Object startStopSync = new Object();
  private volatile boolean started = false;

  @Inject
  public ListenerDiscoverer(
      CuratorFramework cf,
      ListeningAnnouncerConfig listeningAnnouncerConfig
  )
  {
    this(
        ServiceDiscoveryBuilder
            .builder(Void.class)
            .basePath(listeningAnnouncerConfig.getListenersPath())
            .client(cf)
            .watchInstances(false)
            .build()
    );
  }

  // Exposed for unit tests
  ListenerDiscoverer(
      ServiceDiscovery<Void> serviceDiscovery
  )
  {
    this.serviceDiscovery = serviceDiscovery;
  }

  @LifecycleStart
  public void start()
  {
    synchronized (startStopSync) {
      if (started) {
        LOG.debug("Already started");
        return;
      }
      try {
        serviceDiscovery.start();
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
      started = true;
    }
  }

  @LifecycleStop
  public void stop()
  {
    synchronized (startStopSync) {
      if (!started) {
        LOG.debug("Already stopped");
        return;
      }
      final Closer closer = Closer.create();
      closer.register(serviceDiscovery);
      for (ServiceCache<Void> serviceCache : services.values()) {
        closer.register(serviceCache);
      }
      try {
        closer.close();
      }
      catch (IOException e) {
        throw Throwables.propagate(e);
      }
      services.clear();
      started = false;
    }
  }

  /**
   * Get nodes at a particular listener.
   * This method lazily adds service discovery
   *
   * @param listener_key The Listener's service key
   *
   * @return A collection of druid nodes as established by the service discovery
   */
  public Collection<DruidNode> getNodes(final String listener_key)
  {
    ServiceCache<Void> serviceCache = services.get(listener_key);
    if (serviceCache == null) {
      synchronized (startStopSync) {
        if (!started) {
          throw new ISE("ListenerDiscoverer not started");
        }
        serviceCache = serviceDiscovery
            .serviceCacheBuilder()
            .name(listener_key)
            .threadFactory(Execs.makeThreadFactory("ListenerDiscoverer--%s"))
            .build();
        try {
          serviceCache.start();
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
        if (services.putIfAbsent(listener_key, serviceCache) != null) {
          try {
            serviceCache.close();
          }
          catch (IOException e) {
            throw Throwables.propagate(e);
          }
          serviceCache = services.get(listener_key);
          if (serviceCache == null) {
            throw new ISE("Race condition on listener key [%s]. Should not happen!", listener_key);
          }
        }
      }
    }
    return ImmutableList.copyOf(Collections2.filter(
        Lists.transform(
            serviceCache.getInstances(),
            new Function<ServiceInstance<Void>, DruidNode>()
            {
              @Nullable
              @Override
              public DruidNode apply(@Nullable ServiceInstance<Void> input)
              {
                if (input == null) {
                  LOG.debug("Instance for listener group [%s] was null", listener_key);
                  return null;
                }
                return new DruidNode(input.getName(), input.getAddress(), input.getPort());
              }
            }
        ), new Predicate<DruidNode>()
        {
          @Override
          public boolean apply(@Nullable DruidNode input)
          {
            return input != null;
          }
        }
    ));
  }
}
