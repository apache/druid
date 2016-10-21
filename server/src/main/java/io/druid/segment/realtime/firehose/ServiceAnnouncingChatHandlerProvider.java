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

package io.druid.segment.realtime.firehose;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.inject.Inject;

import io.druid.curator.discovery.ServiceAnnouncer;
import io.druid.server.DruidNode;
import io.druid.guice.annotations.RemoteChatHandler;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Provides a way for the outside world to talk to objects in the indexing service. The {@link #get(String)} method
 * allows anyone with a reference to this object to obtain a particular {@link ChatHandler}. An embedded
 * {@link ServiceAnnouncer} will be used to advertise handlers on this host.
 */
public class ServiceAnnouncingChatHandlerProvider implements ChatHandlerProvider
{
  private static final Logger log = new Logger(ServiceAnnouncingChatHandlerProvider.class);

  private final DruidNode node;
  private final ServiceAnnouncer serviceAnnouncer;
  private final ConcurrentMap<String, ChatHandler> handlers;
  private final ConcurrentSkipListSet<String> announcements;

  @Inject
  public ServiceAnnouncingChatHandlerProvider(
      @RemoteChatHandler DruidNode node,
      ServiceAnnouncer serviceAnnouncer
  )
  {
    this.node = node;
    this.serviceAnnouncer = serviceAnnouncer;
    this.handlers = Maps.newConcurrentMap();
    this.announcements = new ConcurrentSkipListSet<>();
  }

  @Override
  public void register(final String service, ChatHandler handler)
  {
    register(service, handler, true);
  }

  @Override
  public void register(final String service, ChatHandler handler, boolean announce)
  {
    log.info("Registering Eventhandler[%s]", service);

    if (handlers.putIfAbsent(service, handler) != null) {
      throw new ISE("handler already registered for service[%s]", service);
    }

    if (announce)
    {
      try {
        serviceAnnouncer.announce(makeDruidNode(service));
        if (!announcements.add(service)) {
          throw new ISE("announcements already has an entry for service[%s]", service);
        }
      }
      catch (Exception e) {
        log.warn(e, "Failed to register service[%s]", service);
        handlers.remove(service, handler);
      }
    }
  }

  @Override
  public void unregister(final String service)
  {
    log.info("Unregistering chat handler[%s]", service);

    final ChatHandler handler = handlers.get(service);
    if (handler == null) {
      log.warn("handler[%s] not currently registered, ignoring.", service);
      return;
    }

    if (announcements.contains(service))
    {
      try {
        serviceAnnouncer.unannounce(makeDruidNode(service));
      }
      catch (Exception e) {
        log.warn(e, "Failed to unregister service[%s]", service);
      }

      announcements.remove(service);
    }

    handlers.remove(service, handler);
  }

  @Override
  public Optional<ChatHandler> get(final String key)
  {
    return Optional.fromNullable(handlers.get(key));
  }

  private DruidNode makeDruidNode(String key)
  {
    return new DruidNode(key, node.getHost(), node.getPort());
  }
}
