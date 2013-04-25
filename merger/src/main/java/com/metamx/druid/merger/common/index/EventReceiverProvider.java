package com.metamx.druid.merger.common.index;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import com.metamx.druid.initialization.Initialization;
import com.metamx.druid.merger.worker.config.EventReceiverProviderConfig;
import com.netflix.curator.x.discovery.ServiceDiscovery;
import com.netflix.curator.x.discovery.ServiceInstance;

import java.util.concurrent.ConcurrentMap;

/**
 * Provides a link between an {@link EventReceiver} and users. The {@link #get(String)} method allows anyone with a
 * reference to this object to obtain an event receiver with a particular name. An embedded {@link ServiceDiscovery}
 * instance, if provided, will be used to advertise event receivers on this host.
 */
public class EventReceiverProvider
{
  private static final Logger log = new Logger(EventReceiverProvider.class);

  private final EventReceiverProviderConfig config;
  private final ServiceDiscovery<?> discovery;
  private final ConcurrentMap<String, EventReceiverHolder> receivers;

  public EventReceiverProvider(
      EventReceiverProviderConfig config,
      ServiceDiscovery discovery
  )
  {
    this.config = config;
    this.discovery = discovery;
    this.receivers = Maps.newConcurrentMap();
  }

  public void register(final String key, EventReceiver receiver)
  {
    log.info("Registering event receiver: %s", key);

    final EventReceiverHolder holder = new EventReceiverHolder(
        receiver,
        isDiscoverable() ? Initialization.serviceInstance(
            String.format(config.getServiceFormat(), key),
            config.getHost(),
            config.getPort()
        ) : null
    );

    if (receivers.putIfAbsent(key, holder) != null) {
      throw new ISE("Receiver already registered for key: %s", key);
    }

    if (isDiscoverable()) {
      try {
        discovery.registerService(holder.service);
      }
      catch (Exception e) {
        log.warn(e, "Failed to register service: %s", holder.service.getName());
        receivers.remove(key, holder);
      }
    }
  }

  public void unregister(final String key)
  {
    log.info("Unregistering event receiver: %s", key);

    final EventReceiverHolder holder = receivers.get(key);
    if (holder == null) {
      log.warn("Receiver not currently registered, ignoring: %s", key);
    }

    if (isDiscoverable()) {
      try {
        discovery.unregisterService(holder.service);
      }
      catch (Exception e) {
        log.warn(e, "Failed to unregister service: %s", holder.service.getName());
      }
    }

    receivers.remove(key);
  }

  public Optional<EventReceiver> get(final String key)
  {
    final EventReceiverHolder holder = receivers.get(key);
    if (holder != null) {
      return Optional.of(holder.receiver);
    } else {
      return Optional.absent();
    }
  }

  public boolean isDiscoverable()
  {
    return discovery != null;
  }

  private static class EventReceiverHolder
  {
    final EventReceiver receiver;
    final ServiceInstance service;

    private EventReceiverHolder(EventReceiver receiver, ServiceInstance service)
    {
      this.receiver = receiver;
      this.service = service;
    }
  }
}
