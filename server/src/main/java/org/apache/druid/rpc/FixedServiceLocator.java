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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Locator for a fixed set of {@link ServiceLocations}.
 */
public class FixedServiceLocator implements ServiceLocator
{
  private final ServiceLocations locations;

  private volatile boolean closed = false;

  public FixedServiceLocator(final ServiceLocations locations)
  {
    this.locations = Preconditions.checkNotNull(locations);
  }

  public FixedServiceLocator(final ServiceLocation location)
  {
    this(ServiceLocations.forLocation(location));
  }

  @Override
  public ListenableFuture<ServiceLocations> locate()
  {
    if (closed) {
      return Futures.immediateFuture(ServiceLocations.closed());
    } else {
      return Futures.immediateFuture(locations);
    }
  }

  @Override
  public void close()
  {
    closed = true;
  }
}
