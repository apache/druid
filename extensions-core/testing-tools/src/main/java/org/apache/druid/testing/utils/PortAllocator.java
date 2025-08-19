/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.testing.utils;

import org.apache.druid.java.util.common.logger.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Allocates NodePorts dynamically for Druid services to avoid hardcoded port conflicts.
 * Useful for testing scenarios where multiple services need unique ports.
 */
public class PortAllocator
{
  private static final Logger log = new Logger(PortAllocator.class);
  
  private final int startPort;
  private final int endPort;
  private final Set<Integer> allocatedPorts = new HashSet<>();
  private final Map<String, Integer> servicePortMap = new HashMap<>();
  
  public PortAllocator(int startPort, int endPort)
  {
    this.startPort = startPort;
    this.endPort = endPort;
    log.info("PortAllocator initialized with range %d-%d", startPort, endPort);
  }
  
  /**
   * Allocate a port for a service. If the service already has a port, return it.
   */
  public synchronized int allocatePort(String serviceName)
  {
    if (servicePortMap.containsKey(serviceName)) {
      int existingPort = servicePortMap.get(serviceName);
      log.info("Service %s already has port %d", serviceName, existingPort);
      return existingPort;
    }
    
    for (int port = startPort; port <= endPort; port++) {
      if (!allocatedPorts.contains(port)) {
        allocatedPorts.add(port);
        servicePortMap.put(serviceName, port);
        log.info("Allocated port %d for service %s", port, serviceName);
        return port;
      }
    }
    
    throw new RuntimeException("No available ports in range " + startPort + "-" + endPort);
  }
  
  /**
   * Get the allocated port for a service.
   */
  public synchronized Integer getPort(String serviceName)
  {
    return servicePortMap.get(serviceName);
  }
  
  /**
   * Release a port for a service.
   */
  public synchronized void releasePort(String serviceName)
  {
    Integer port = servicePortMap.remove(serviceName);
    if (port != null) {
      allocatedPorts.remove(port);
      log.info("Released port %d for service %s", port, serviceName);
    }
  }
  
  /**
   * Get all allocated ports for K3S container exposure.
   */
  public synchronized int[] getAllocatedPorts()
  {
    return allocatedPorts.stream().mapToInt(Integer::intValue).toArray();
  }
  
  /**
   * Get service to port mapping for debugging.
   */
  public synchronized Map<String, Integer> getServicePortMapping()
  {
    return new HashMap<>(servicePortMap);
  }
  
  /**
   * Reset all allocations.
   */
  public synchronized void reset()
  {
    allocatedPorts.clear();
    servicePortMap.clear();
    log.info("PortAllocator reset");
  }
}
