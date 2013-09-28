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

package io.druid.curator.discovery;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import io.druid.server.DruidNode;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceInstance;

import java.util.Map;

/**
 * Uses the Curator Service Discovery recipe to announce services.
 */
public class CuratorServiceAnnouncer implements ServiceAnnouncer
{
  private static final EmittingLogger log = new EmittingLogger(CuratorServiceAnnouncer.class);

  private final ServiceDiscovery<Void> discovery;
  private final Map<String, ServiceInstance<Void>> instanceMap = Maps.newHashMap();
  private final Object monitor = new Object();

  @Inject
  public CuratorServiceAnnouncer(
      ServiceDiscovery<Void> discovery
  )
  {
    this.discovery = discovery;
  }

  @Override
  public void announce(DruidNode service)
  {
    final String serviceName = getServiceName(service);

    final ServiceInstance<Void> instance;
    synchronized (monitor) {
      if (instanceMap.containsKey(serviceName)) {
        log.warn("Ignoring request to announce service[%s]", service);
        return;
      } else {
        try {
          instance = ServiceInstance.<Void>builder()
                                    .name(serviceName)
                                    .address(service.getHostNoPort())
                                    .port(service.getPort())
                                    .build();
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }

        instanceMap.put(serviceName, instance);
      }
    }

    try {
      log.info("Announcing service[%s]", service);
      discovery.registerService(instance);
    }
    catch (Exception e) {
      log.warn("Failed to announce service[%s]", service);
      synchronized (monitor) {
        instanceMap.remove(serviceName);
      }
    }
  }

  @Override
  public void unannounce(DruidNode service)
  {
    final String serviceName = getServiceName(service);
    final ServiceInstance<Void> instance;

    synchronized (monitor) {
      instance = instanceMap.get(serviceName);
      if (instance == null) {
        log.warn("Ignoring request to unannounce service[%s]", service);
        return;
      }
    }

    log.info("Unannouncing service[%s]", service);
    try {
      discovery.unregisterService(instance);
    }
    catch (Exception e) {
      log.makeAlert(e, "Failed to unannounce service[%s], zombie znode perhaps in existence.", serviceName)
         .addData("service", service)
         .emit();
    }
    finally {
      synchronized (monitor) {
        instanceMap.remove(serviceName);
      }
    }
  }

  private String getServiceName(DruidNode service) {
    return service.getServiceName().replaceAll("/", ":");
  }
}
