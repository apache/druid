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

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import io.druid.server.DruidNode;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceType;

import java.io.IOException;

/**
 * Announces that there is a particular ListenerResource at the listener_key.
 */
public abstract class ListenerResourceAnnouncer
{
  private static final Logger LOG = new Logger(ListenerResourceAnnouncer.class);
  private final Object startStopSync = new Object();
  private volatile boolean started = false;
  private final ServiceDiscovery<Void> serviceDiscovery;
  private final ServiceInstance<Void> me;

  public ListenerResourceAnnouncer(
      CuratorFramework cf,
      ListeningAnnouncerConfig listeningAnnouncerConfig,
      String listener_key,
      DruidNode node
  )
  {
    this(
        ServiceDiscoveryBuilder
            .builder(Void.class)
            .basePath(listeningAnnouncerConfig.getListenersPath())
            .client(cf)
            .watchInstances(false)
            .build(),
        listener_key,
        node
    );
  }

  ListenerResourceAnnouncer(
      ServiceDiscovery<Void> serviceDiscovery,
      String listener_key,
      DruidNode node
  )
  {
    this.serviceDiscovery = serviceDiscovery;
    try {
      this.me = ServiceInstance
          .<Void>builder()
          .address(node.getHost())
          .port(node.getPort())
          .name(listener_key)
          .serviceType(ServiceType.DYNAMIC)
          .build();
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @LifecycleStart
  public void start()
  {
    synchronized (startStopSync) {
      if (started) {
        LOG.debug("Already started, ignoring");
        return;
      }
      try {
        serviceDiscovery.start();
        serviceDiscovery.registerService(me);
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
        LOG.debug("Already stopped, ignoring");
        return;
      }
      try {
        serviceDiscovery.close();
      }
      catch (IOException e) {
        throw Throwables.propagate(e);
      }
      started = false;
    }
  }
}
