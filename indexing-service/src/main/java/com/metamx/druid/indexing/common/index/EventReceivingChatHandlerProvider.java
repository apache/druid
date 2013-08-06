/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.indexing.common.index;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import com.metamx.druid.curator.discovery.ServiceAnnouncer;
import com.metamx.druid.indexing.worker.config.ChatHandlerProviderConfig;

import java.util.concurrent.ConcurrentMap;

/**
 * Provides a way for the outside world to talk to objects in the indexing service. The {@link #get(String)} method
 * allows anyone with a reference to this object to obtain a particular {@link ChatHandler}. An embedded
 * {@link ServiceAnnouncer} will be used to advertise handlers on this host.
 */
public class EventReceivingChatHandlerProvider implements ChatHandlerProvider
{
  private static final Logger log = new Logger(EventReceivingChatHandlerProvider.class);

  private final ChatHandlerProviderConfig config;
  private final ServiceAnnouncer serviceAnnouncer;
  private final ConcurrentMap<String, ChatHandler> handlers;

  public EventReceivingChatHandlerProvider(
      ChatHandlerProviderConfig config,
      ServiceAnnouncer serviceAnnouncer
  )
  {
    this.config = config;
    this.serviceAnnouncer = serviceAnnouncer;
    this.handlers = Maps.newConcurrentMap();
  }

  @Override
  public void register(final String key, ChatHandler handler)
  {
    final String service = serviceName(key);
    log.info("Registering Eventhandler: %s", key);

    if (handlers.putIfAbsent(key, handler) != null) {
      throw new ISE("handler already registered for key: %s", key);
    }

    try {
      serviceAnnouncer.announce(service);
    }
    catch (Exception e) {
      log.warn(e, "Failed to register service: %s", service);
      handlers.remove(key, handler);
    }
  }

  @Override
  public void unregister(final String key)
  {
    final String service = serviceName(key);

    log.info("Unregistering chat handler: %s", key);

    final ChatHandler handler = handlers.get(key);
    if (handler == null) {
      log.warn("handler not currently registered, ignoring: %s", key);
    }

    try {
      serviceAnnouncer.unannounce(service);
    }
    catch (Exception e) {
      log.warn(e, "Failed to unregister service: %s", service);
    }

    handlers.remove(key, handler);
  }

  @Override
  public Optional<ChatHandler> get(final String key)
  {
    return Optional.fromNullable(handlers.get(key));
  }

  private String serviceName(String key)
  {
    return String.format(config.getServiceFormat(), key);
  }
}
