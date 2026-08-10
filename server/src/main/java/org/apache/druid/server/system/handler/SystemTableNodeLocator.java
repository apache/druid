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

package org.apache.druid.server.system.handler;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import org.apache.druid.client.DirectDruidClient;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.server.system.table.SystemTableDescriptor;
import org.apache.druid.server.system.table.SystemTableRoutingMode;

import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Resolves a descriptor's logical routing policy to concrete Druid processes. */
public class SystemTableNodeLocator
{
  private final DruidNodeDiscoveryProvider discoveryProvider;
  private final Map<NodeRole, SystemTableLeaderLocator> leaderLocators;

  @Inject
  public SystemTableNodeLocator(
      final DruidNodeDiscoveryProvider discoveryProvider,
      final CoordinatorClient coordinatorClient,
      final OverlordClient overlordClient
  )
  {
    this.discoveryProvider = discoveryProvider;
    this.leaderLocators = Map.of(
        NodeRole.COORDINATOR,
        coordinatorClient::findCurrentLeader,
        NodeRole.OVERLORD,
        overlordClient::findCurrentLeader
    );
  }

  List<SystemTableNode> locate(final SystemTableDescriptor descriptor, final Query<?> query)
  {
    if (descriptor.getRoutingMode() == SystemTableRoutingMode.ALL_NODES) {
      return discoverAllNodes(descriptor);
    }

    final NodeRole leaderRole = Iterables.getOnlyElement(descriptor.getNodeRoles());
    final URI leaderUri = findLeader(leaderRole, query);
    // Resolve leadership before taking the discovery snapshot. A leader election may complete while discovery still
    // contains the previous membership, so taking the snapshot first can reject a valid newly elected leader.
    return discoverAllNodes(descriptor).stream()
                     .filter(node -> sameServer(leaderUri, node.getDiscoveryNode().getDruidNode().getUriToUse()))
                     .findFirst()
                     .map(List::of)
                     .orElseThrow(
                         () -> new ISE(
                             "Current leader[%s] for role[%s] is not present in service discovery",
                             leaderUri,
                             leaderRole
                         )
                     );
  }

  private List<SystemTableNode> discoverAllNodes(final SystemTableDescriptor descriptor)
  {
    final Map<String, SystemTableNode> nodes = new LinkedHashMap<>();
    for (final NodeRole nodeRole : descriptor.getNodeRoles()) {
      for (final DiscoveryDruidNode node : discoveryProvider.getForNodeRole(nodeRole).getAllNodes()) {
        nodes.computeIfAbsent(
            node.getDruidNode().getHostAndPortToUse(),
            ignored -> new SystemTableNode(node)
        ).addNodeRole(node.getNodeRole());
      }
    }
    return new ArrayList<>(nodes.values());
  }

  private URI findLeader(final NodeRole nodeRole, final Query<?> query)
  {
    final SystemTableLeaderLocator leaderLocator = leaderLocators.get(nodeRole);
    if (leaderLocator == null) {
      throw new ISE("Leader-only system-table routing is not supported for role[%s]", nodeRole);
    }

    final ListenableFuture<URI> leaderFuture = leaderLocator.findCurrentLeader();
    final long timeLeft = query.context().getLong(DirectDruidClient.QUERY_FAIL_TIME) - System.currentTimeMillis();
    if (timeLeft <= 0) {
      leaderFuture.cancel(true);
      throw new QueryTimeoutException("Timed out while locating the current leader for role[" + nodeRole + "]");
    }
    try {
      return leaderFuture.get(timeLeft, TimeUnit.MILLISECONDS);
    }
    catch (InterruptedException e) {
      leaderFuture.cancel(true);
      Thread.currentThread().interrupt();
      throw QueryInterruptedException.wrapIfNeeded(e);
    }
    catch (TimeoutException e) {
      leaderFuture.cancel(true);
      throw new QueryTimeoutException("Timed out while locating the current leader for role[" + nodeRole + "]");
    }
    catch (ExecutionException e) {
      throw QueryInterruptedException.wrapIfNeeded(e.getCause() == null ? e : e.getCause());
    }
  }

  static boolean sameServer(final URI first, final URI second)
  {
    return first.getScheme().equalsIgnoreCase(second.getScheme())
           && first.getHost().equalsIgnoreCase(second.getHost())
           && effectivePort(first) == effectivePort(second);
  }

  private static int effectivePort(final URI uri)
  {
    if (uri.getPort() >= 0) {
      return uri.getPort();
    }
    return "https".equalsIgnoreCase(uri.getScheme()) ? 443 : 80;
  }
}
