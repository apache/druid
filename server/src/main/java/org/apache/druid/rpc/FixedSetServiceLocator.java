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

import javax.validation.constraints.NotNull;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Basic implmentation of {@link ServiceLocator} that returns a service location from a static set of locations. Returns
 * a random location each time one is requested.
 */
public class FixedSetServiceLocator implements ServiceLocator
{
  private ServiceLocations serviceLocations;

  private FixedSetServiceLocator(ServiceLocations serviceLocations)
  {
    this.serviceLocations = serviceLocations;
  }

  public static FixedSetServiceLocator forServiceLocation(@NotNull ServiceLocation serviceLocation)
  {
    return new FixedSetServiceLocator(ServiceLocations.forLocation(serviceLocation));
  }

  public static FixedSetServiceLocator forDruidServerMetadata(Set<DruidServerMetadata> serverMetadataSet)
  {
    if (serverMetadataSet == null || serverMetadataSet.isEmpty()) {
      return new FixedSetServiceLocator(ServiceLocations.closed());
    } else {
      Set<ServiceLocation> serviceLocationSet = serverMetadataSet.stream()
                                                                  .map(ServiceLocation::fromDruidServerMetadata)
                                                                  .collect(Collectors.toSet());

      return new FixedSetServiceLocator(ServiceLocations.forLocations(serviceLocationSet));
    }
  }

  @Override
  public ListenableFuture<ServiceLocations> locate()
  {
    if (serviceLocations.isClosed() || serviceLocations.getLocations().isEmpty()) {
      return Futures.immediateFuture(ServiceLocations.closed());
    }

    Set<ServiceLocation> locationSet = serviceLocations.getLocations();
    int size = locationSet.size();
    if (size == 1) {
      return Futures.immediateFuture(ServiceLocations.forLocation(locationSet.stream().findFirst().get()));
    }

    return Futures.immediateFuture(
        ServiceLocations.forLocation(
            locationSet.stream()
                       .skip(ThreadLocalRandom.current().nextInt(size))
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
