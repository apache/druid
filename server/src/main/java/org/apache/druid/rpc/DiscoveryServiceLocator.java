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

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * A {@link ServiceLocator} that uses {@link DruidNodeDiscovery}.
 */
public class DiscoveryServiceLocator implements ServiceLocator
{
  private final DruidNodeDiscoveryProvider discoveryProvider;
  private final NodeRole nodeRole;
  private final DruidNodeDiscovery.Listener listener;

  @GuardedBy("this")
  private boolean started = false;

  @GuardedBy("this")
  private boolean initialized = false;

  @GuardedBy("this")
  private boolean closed = false;

  @GuardedBy("this")
  private final Set<ServiceLocation> locations = new HashSet<>();

  @GuardedBy("this")
  private SettableFuture<ServiceLocations> pendingFuture = null;

  @GuardedBy("this")
  private DruidNodeDiscovery discovery = null;

  public DiscoveryServiceLocator(final DruidNodeDiscoveryProvider discoveryProvider, final NodeRole nodeRole)
  {
    this.discoveryProvider = discoveryProvider;
    this.nodeRole = nodeRole;
    this.listener = new Listener();
  }

  @Override
  public ListenableFuture<ServiceLocations> locate()
  {
    synchronized (this) {
      if (closed) {
        return Futures.immediateFuture(ServiceLocations.closed());
      } else if (initialized) {
        return Futures.immediateFuture(ServiceLocations.forLocations(ImmutableSet.copyOf(locations)));
      } else {
        if (pendingFuture == null) {
          pendingFuture = SettableFuture.create();
        }

        return Futures.nonCancellationPropagating(pendingFuture);
      }
    }
  }

  @LifecycleStart
  public void start()
  {
    synchronized (this) {
      if (started || closed) {
        throw new ISE("Cannot start once already started or closed");
      } else {
        started = true;
        this.discovery = discoveryProvider.getForNodeRole(nodeRole);
        discovery.registerListener(listener);
      }
    }
  }

  @Override
  @LifecycleStop
  public void close()
  {
    synchronized (this) {
      // Idempotent: can call close() multiple times so long as start() has already been called.
      if (started && !closed) {
        if (discovery != null) {
          discovery.removeListener(listener);
        }

        if (pendingFuture != null) {
          pendingFuture.set(ServiceLocations.closed());
          pendingFuture = null;
        }

        closed = true;
      }
    }
  }

  private class Listener implements DruidNodeDiscovery.Listener
  {
    @Override
    public void nodesAdded(final Collection<DiscoveryDruidNode> nodes)
    {
      synchronized (DiscoveryServiceLocator.this) {
        for (final DiscoveryDruidNode node : nodes) {
          locations.add(ServiceLocation.fromDruidNode(node.getDruidNode()));
        }
      }
    }

    @Override
    public void nodesRemoved(final Collection<DiscoveryDruidNode> nodes)
    {
      synchronized (DiscoveryServiceLocator.this) {
        for (final DiscoveryDruidNode node : nodes) {
          locations.remove(ServiceLocation.fromDruidNode(node.getDruidNode()));
        }
      }
    }

    @Override
    public void nodeViewInitialized()
    {
      synchronized (DiscoveryServiceLocator.this) {
        initialized = true;

        if (pendingFuture != null) {
          pendingFuture.set(ServiceLocations.forLocations(ImmutableSet.copyOf(locations)));
          pendingFuture = null;
        }
      }
    }
  }
}
