package com.metamx.druid.merger.common.index;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import com.metamx.druid.curator.discovery.ServiceAnnouncer;
import com.metamx.druid.merger.worker.config.EventReceiverProviderConfig;
import org.apache.curator.x.discovery.ServiceDiscovery;

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
  private final ServiceAnnouncer serviceAnnouncer;
  private final ConcurrentMap<String, EventReceiver> receivers;

  public EventReceiverProvider(
      EventReceiverProviderConfig config,
      ServiceAnnouncer serviceAnnouncer
  )
  {
    this.config = config;
    this.serviceAnnouncer = serviceAnnouncer;
    this.receivers = Maps.newConcurrentMap();
  }

  public void register(final String key, EventReceiver receiver)
  {
    final String service = serviceName(key);
    log.info("Registering EventReceiver: %s", key);

    if (receivers.putIfAbsent(key, receiver) != null) {
      throw new ISE("Receiver already registered for key: %s", key);
    }

    try {
      serviceAnnouncer.announce(service);
    }
    catch (Exception e) {
      log.warn(e, "Failed to register service: %s", service);
      receivers.remove(key, receiver);
    }
  }

  public void unregister(final String key)
  {
    final String service = serviceName(key);

    log.info("Unregistering event receiver: %s", key);

    final EventReceiver receiver = receivers.get(key);
    if (receiver == null) {
      log.warn("Receiver not currently registered, ignoring: %s", key);
    }

    try {
      serviceAnnouncer.unannounce(service);
    }
    catch (Exception e) {
      log.warn(e, "Failed to unregister service: %s", service);
    }

    receivers.remove(key, receiver);
  }

  public Optional<EventReceiver> get(final String key)
  {
    return Optional.fromNullable(receivers.get(key));
  }

  private String serviceName(String key)
  {
    return String.format(config.getServiceFormat(), key);
  }
}
