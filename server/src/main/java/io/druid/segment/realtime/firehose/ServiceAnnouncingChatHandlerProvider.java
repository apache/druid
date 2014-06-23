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

package io.druid.segment.realtime.firehose;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import io.druid.curator.discovery.ServiceAnnouncer;
import io.druid.guice.annotations.Self;
import io.druid.server.DruidNode;

import java.util.concurrent.ConcurrentMap;

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

  @Inject
  public ServiceAnnouncingChatHandlerProvider(
      @Self DruidNode node,
      ServiceAnnouncer serviceAnnouncer
  )
  {
    this.node = node;
    this.serviceAnnouncer = serviceAnnouncer;
    this.handlers = Maps.newConcurrentMap();
  }

  @Override
  public void register(final String service, ChatHandler handler)
  {
    final DruidNode node = makeDruidNode(service);
    log.info("Registering Eventhandler[%s]", service);

    if (handlers.putIfAbsent(service, handler) != null) {
      throw new ISE("handler already registered for service[%s]", service);
    }

    try {
      serviceAnnouncer.announce(node);
    }
    catch (Exception e) {
      log.warn(e, "Failed to register service[%s]", service);
      handlers.remove(service, handler);
    }
  }

  @Override
  public void unregister(final String service)
  {
    log.info("Unregistering chat handler[%s]", service);

    final ChatHandler handler = handlers.get(service);
    if (handler == null) {
      log.warn("handler[%s] not currently registered, ignoring.", service);
    }

    try {
      serviceAnnouncer.unannounce(makeDruidNode(service));
    }
    catch (Exception e) {
      log.warn(e, "Failed to unregister service[%s]", service);
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
