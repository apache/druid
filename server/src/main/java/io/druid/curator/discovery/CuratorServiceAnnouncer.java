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
    final String serviceName = CuratorServiceUtils.makeCanonicalServiceName(service.getServiceName());

    final ServiceInstance<Void> instance;
    synchronized (monitor) {
      if (instanceMap.containsKey(serviceName)) {
        log.warn("Ignoring request to announce service[%s]", service);
        return;
      } else {
        try {
          instance = ServiceInstance.<Void>builder()
              .name(serviceName)
              .address(service.getHost())
              .port(service.getPlaintextPort())
              .sslPort(service.getTlsPort())
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
    final String serviceName = CuratorServiceUtils.makeCanonicalServiceName(service.getServiceName());
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
}
