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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.server.DruidNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.WARN)
public class DiscoveryServiceLocatorTest
{
  private static final DiscoveryDruidNode NODE1 = new DiscoveryDruidNode(
      new DruidNode("test-service", "node1.example.com", false, -1, 8888, false, true),
      NodeRole.BROKER,
      Collections.emptyMap()
  );

  private static final DiscoveryDruidNode NODE2 = new DiscoveryDruidNode(
      new DruidNode("test-service", "node2.example.com", false, -1, 8888, false, true),
      NodeRole.BROKER,
      Collections.emptyMap()
  );

  @Mock
  public DruidNodeDiscoveryProvider discoveryProvider;

  private DiscoveryServiceLocator locator;

  @AfterEach
  public void tearDown()
  {
    if (locator != null) {
      locator.close();
    }
  }

  @Test
  public void test_locate_initializeEmpty() throws Exception
  {
    final TestDiscovery discovery = new TestDiscovery();
    Mockito.when(discoveryProvider.getForNodeRole(NodeRole.BROKER)).thenReturn(discovery);
    locator = new DiscoveryServiceLocator(discoveryProvider, NodeRole.BROKER);
    locator.start();

    final ListenableFuture<ServiceLocations> future = locator.locate();
    Assertions.assertFalse(future.isDone());

    discovery.fire(DruidNodeDiscovery.Listener::nodeViewInitialized);
    Assertions.assertEquals(ServiceLocations.forLocations(Collections.emptySet()), future.get());
  }

  @Test
  public void test_locate_initializeNonEmpty() throws Exception
  {
    final TestDiscovery discovery = new TestDiscovery();
    Mockito.when(discoveryProvider.getForNodeRole(NodeRole.BROKER)).thenReturn(discovery);
    locator = new DiscoveryServiceLocator(discoveryProvider, NodeRole.BROKER);
    locator.start();

    final ListenableFuture<ServiceLocations> future = locator.locate();
    Assertions.assertFalse(future.isDone());

    discovery.fire(listener -> {
      listener.nodesAdded(ImmutableSet.of(NODE1));
      listener.nodesAdded(ImmutableSet.of(NODE2));
      listener.nodeViewInitialized();
    });

    Assertions.assertEquals(
        ServiceLocations.forLocations(
            ImmutableSet.of(
                ServiceLocation.fromDruidNode(NODE1.getDruidNode()),
                ServiceLocation.fromDruidNode(NODE2.getDruidNode())
            )
        ),
        future.get()
    );
  }

  @Test
  public void test_locate_removeAfterAdd() throws Exception
  {
    final TestDiscovery discovery = new TestDiscovery();
    Mockito.when(discoveryProvider.getForNodeRole(NodeRole.BROKER)).thenReturn(discovery);
    locator = new DiscoveryServiceLocator(discoveryProvider, NodeRole.BROKER);
    locator.start();

    discovery.fire(listener -> {
      listener.nodesAdded(ImmutableSet.of(NODE1));
      listener.nodesAdded(ImmutableSet.of(NODE2));
      listener.nodeViewInitialized();
      listener.nodesRemoved(ImmutableSet.of(NODE1));
    });

    Assertions.assertEquals(
        ServiceLocations.forLocations(
            ImmutableSet.of(
                ServiceLocation.fromDruidNode(NODE2.getDruidNode())
            )
        ),
        locator.locate().get()
    );
  }

  @Test
  public void test_locate_closed() throws Exception
  {
    final TestDiscovery discovery = new TestDiscovery();
    Mockito.when(discoveryProvider.getForNodeRole(NodeRole.BROKER)).thenReturn(discovery);
    locator = new DiscoveryServiceLocator(discoveryProvider, NodeRole.BROKER);
    locator.start();

    final ListenableFuture<ServiceLocations> future = locator.locate();
    locator.close();

    Assertions.assertEquals(ServiceLocations.closed(), future.get()); // Call made prior to close()
    Assertions.assertEquals(ServiceLocations.closed(), locator.locate().get()); // Call made after close()

    Assertions.assertEquals(0, discovery.getListeners().size());
  }

  private static class TestDiscovery implements DruidNodeDiscovery
  {
    @GuardedBy("this")
    private final List<Listener> listeners;

    public TestDiscovery()
    {
      listeners = new ArrayList<>();
    }

    @Override
    public Collection<DiscoveryDruidNode> getAllNodes()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public synchronized void registerListener(Listener listener)
    {
      listeners.add(listener);
    }

    @Override
    public synchronized void removeListener(Listener listener)
    {
      listeners.remove(listener);
    }

    public synchronized List<Listener> getListeners()
    {
      return ImmutableList.copyOf(listeners);
    }

    public synchronized void fire(Consumer<Listener> f)
    {
      for (final Listener listener : listeners) {
        f.accept(listener);
      }
    }
  }
}
