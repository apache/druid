package com.metamx.druid.curator.discovery;

import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceInstance;

import java.util.Map;

/**
 * Uses the Curator Service Discovery recipe to announce services.
 */
public class CuratorServiceAnnouncer implements ServiceAnnouncer
{
  private static final Logger log = new Logger(CuratorServiceAnnouncer.class);

  private final ServiceDiscovery<Void> discovery;
  private final ServiceInstanceFactory<Void> instanceFactory;
  private final Map<String, ServiceInstance<Void>> instanceMap = Maps.newHashMap();
  private final Object monitor = new Object();

  @Inject
  public CuratorServiceAnnouncer(
      ServiceDiscovery<Void> discovery,
      ServiceInstanceFactory<Void> instanceFactory
  )
  {
    this.discovery = discovery;
    this.instanceFactory = instanceFactory;
  }

  @Override
  public void announce(String service) throws Exception
  {
    final ServiceInstance<Void> instance;

    synchronized (monitor) {
      if (instanceMap.containsKey(service)) {
        log.warn("Ignoring request to announce service[%s]", service);
        return;
      } else {
        instance = instanceFactory.create(service);
        instanceMap.put(service, instance);
      }
    }

    try {
      log.info("Announcing service[%s]", service);
      discovery.registerService(instance);
    } catch (Exception e) {
      log.warn("Failed to announce service[%s]", service);
      synchronized (monitor) {
        instanceMap.remove(service);
      }
    }
  }

  @Override
  public void unannounce(String service) throws Exception
  {
    final ServiceInstance<Void> instance;

    synchronized (monitor) {
      instance = instanceMap.get(service);
      if (instance == null) {
        log.warn("Ignoring request to unannounce service[%s]", service);
        return;
      }
    }

    log.info("Unannouncing service[%s]", service);
    try {
      discovery.unregisterService(instance);
    } catch (Exception e) {
      log.warn(e, "Failed to unannounce service[%s]", service);
    } finally {
      synchronized (monitor) {
        instanceMap.remove(service);
      }
    }
  }
}
