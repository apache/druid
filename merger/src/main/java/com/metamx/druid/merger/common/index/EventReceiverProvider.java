package com.metamx.druid.merger.common.index;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;

import java.util.concurrent.ConcurrentMap;

public class EventReceiverProvider
{
  private static final Logger log = new Logger(EventReceiverProvider.class);

  private final ConcurrentMap<String, EventReceiver> receivers;

  public EventReceiverProvider()
  {
    this.receivers = Maps.newConcurrentMap();
  }

  public void register(final String key, EventReceiver receiver)
  {
    log.info("Registering event receiver for %s", key);
    if (receivers.putIfAbsent(key, receiver) != null) {
      throw new ISE("Receiver already registered for key: %s", key);
    }
  }

  public void unregister(final String key, EventReceiver receiver)
  {
    log.info("Unregistering event receiver for %s", key);
    if (!receivers.remove(key, receiver)) {
      log.warn("Receiver not currently registered, ignoring: %s", key);
    }
  }

  public Optional<EventReceiver> get(final String key)
  {
    return Optional.fromNullable(receivers.get(key));
  }
}
