package com.metamx.druid.indexing.common.index;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import com.metamx.druid.curator.discovery.ServiceAnnouncer;
import com.metamx.druid.indexing.worker.config.ChatHandlerProviderConfig;
import com.metamx.druid.initialization.DruidNode;

import java.util.concurrent.ConcurrentMap;

/**
 * Provides a way for the outside world to talk to objects in the indexing service. The {@link #get(String)} method
 * allows anyone with a reference to this object to obtain a particular {@link ChatHandler}. An embedded
 * {@link ServiceAnnouncer} will be used to advertise handlers on this host.
 */
public class ChatHandlerProvider
{
  private static final Logger log = new Logger(ChatHandlerProvider.class);

  private final ChatHandlerProviderConfig config;
  private final ServiceAnnouncer serviceAnnouncer;
  private final ConcurrentMap<String, ChatHandler> handlers;

  public ChatHandlerProvider(
      ChatHandlerProviderConfig config,
      ServiceAnnouncer serviceAnnouncer
  )
  {
    this.config = config;
    this.serviceAnnouncer = serviceAnnouncer;
    this.handlers = Maps.newConcurrentMap();
  }

  public void register(final String service, ChatHandler handler)
  {
    log.info("Registering Eventhandler: %s", service);

    if (handlers.putIfAbsent(service, handler) != null) {
      throw new ISE("handler already registered for service[%s]", service);
    }

    try {
      serviceAnnouncer.announce(makeDruidNode(service));
    }
    catch (Exception e) {
      log.warn(e, "Failed to register service[%s]", service);
      handlers.remove(service, handler);
    }
  }

  public void unregister(final String service)
  {
    log.info("Unregistering chat handler for service[%s]", service);

    final ChatHandler handler = handlers.get(service);
    if (handler == null) {
      log.info("handler not currently registered, ignoring: %s", service);
    }

    try {
      serviceAnnouncer.unannounce(makeDruidNode(service));
    }
    catch (Exception e) {
      log.warn(e, "Failed to unregister service: %s", service);
    }

    handlers.remove(service, handler);
  }

  public Optional<ChatHandler> get(final String key)
  {
    return Optional.fromNullable(handlers.get(key));
  }

  private DruidNode makeDruidNode(String service) {
    return new DruidNode(service, config.getHost(), config.getPort());
  }
}
