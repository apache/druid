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

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.IAE;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * Returned by {@link ServiceLocator#locate()}. See that function for documentation.
 */
public class ServiceLocations
{
  private final Set<ServiceLocation> locations;
  private final boolean closed;

  private ServiceLocations(final Set<ServiceLocation> locations, final boolean closed)
  {
    this.locations = Preconditions.checkNotNull(locations, "locations");
    this.closed = closed;

    if (closed && !locations.isEmpty()) {
      throw new IAE("Locations must be empty for closed services");
    }
  }

  public static ServiceLocations forLocation(final ServiceLocation location)
  {
    return new ServiceLocations(Collections.singleton(Preconditions.checkNotNull(location)), false);
  }

  public static ServiceLocations forLocations(final Set<ServiceLocation> locations)
  {
    return new ServiceLocations(locations, false);
  }

  public static ServiceLocations closed()
  {
    return new ServiceLocations(Collections.emptySet(), true);
  }

  public Set<ServiceLocation> getLocations()
  {
    return locations;
  }

  public boolean isClosed()
  {
    return closed;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ServiceLocations that = (ServiceLocations) o;
    return closed == that.closed && Objects.equals(locations, that.locations);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(locations, closed);
  }

  @Override
  public String toString()
  {
    return "ServiceLocations{" +
           "locations=" + locations +
           ", closed=" + closed +
           '}';
  }
}
