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

package org.apache.druid.msq.dart.controller.sql;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.msq.dart.controller.http.DartSqlResource;
import org.apache.druid.server.DruidNode;

import javax.servlet.http.HttpServletRequest;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Keeps {@link DartSqlClient} for all servers except ourselves. Currently the purpose of this is to power
 * the "get all queries" API at {@link DartSqlResource#doGetRunningQueries(String, HttpServletRequest)}.
 */
@ManageLifecycle
public class DartSqlClients implements DruidNodeDiscovery.Listener
{
  @GuardedBy("clients")
  private final Map<DruidNode, DartSqlClient> clients = new HashMap<>();
  private final DruidNode selfNode;
  private final DruidNodeDiscoveryProvider discoveryProvider;
  private final DartSqlClientFactory clientFactory;

  private volatile DruidNodeDiscovery discovery;

  @Inject
  public DartSqlClients(
      @Self DruidNode selfNode,
      DruidNodeDiscoveryProvider discoveryProvider,
      DartSqlClientFactory clientFactory
  )
  {
    this.selfNode = selfNode;
    this.discoveryProvider = discoveryProvider;
    this.clientFactory = clientFactory;
  }

  @LifecycleStart
  public void start()
  {
    discovery = discoveryProvider.getForNodeRole(NodeRole.BROKER);
    discovery.registerListener(this);
  }

  public List<DartSqlClient> getAllClients()
  {
    synchronized (clients) {
      return ImmutableList.copyOf(clients.values());
    }
  }

  @Override
  public void nodesAdded(final Collection<DiscoveryDruidNode> nodes)
  {
    synchronized (clients) {
      for (final DiscoveryDruidNode node : nodes) {
        final DruidNode druidNode = node.getDruidNode();
        if (!selfNode.equals(druidNode)) {
          clients.computeIfAbsent(druidNode, clientFactory::makeClient);
        }
      }
    }
  }

  @Override
  public void nodesRemoved(final Collection<DiscoveryDruidNode> nodes)
  {
    synchronized (clients) {
      for (final DiscoveryDruidNode node : nodes) {
        clients.remove(node.getDruidNode());
      }
    }
  }

  @LifecycleStop
  public void stop()
  {
    if (discovery != null) {
      discovery.removeListener(this);
      discovery = null;
    }

    synchronized (clients) {
      clients.clear();
    }
  }
}
