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

package org.apache.druid.rpc;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.jboss.netty.util.internal.ThreadLocalRandom;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * Basic implmentation of {@link ServiceLocator} that returns a service location from a static set of locations. Returns
 * a random location each time one is requested.
 */
public class FixedSetServiceLocator implements ServiceLocator
{
  private ServiceLocations serviceLocations;

  public FixedSetServiceLocator(Set<DruidServerMetadata> servers)
  {
    if (servers == null) {
      serviceLocations = ServiceLocations.closed();
    } else {
      Set<ServiceLocation> serviceLocationSet = servers.stream()
                                                       .map(ServiceLocation::fromDruidServerMetadata)
                                                       .collect(Collectors.toSet());
      serviceLocations = ServiceLocations.forLocations(serviceLocationSet);
    }
  }

  @Override
  public ListenableFuture<ServiceLocations> locate()
  {
    if (serviceLocations.isClosed() || serviceLocations.getLocations().isEmpty()) {
      return Futures.immediateFuture(ServiceLocations.closed());
    }

    Set<ServiceLocation> locationSet = serviceLocations.getLocations();
    return Futures.immediateFuture(
        ServiceLocations.forLocation(
            locationSet.stream()
                       .skip(ThreadLocalRandom.current().nextInt(locationSet.size()))
                       .findFirst()
                       .orElse(null)
        )
    );
  }

  @Override
  public void close()
  {
    serviceLocations = ServiceLocations.closed();
  }
}
