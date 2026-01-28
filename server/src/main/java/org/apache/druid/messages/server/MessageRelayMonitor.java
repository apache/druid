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

package org.apache.druid.messages.server;

import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Code that runs on message servers, to monitor their clients. When a client vanishes, its outbox is reset using
 * {@link Outbox#resetOutbox(String)}.
 */
public class MessageRelayMonitor
{
  private final DruidNodeDiscoveryProvider discoveryProvider;
  private final Outbox<?> outbox;
  private final NodeRole clientRole;

  public MessageRelayMonitor(
      final DruidNodeDiscoveryProvider discoveryProvider,
      final Outbox<?> outbox,
      final NodeRole clientRole
  )
  {
    this.discoveryProvider = discoveryProvider;
    this.outbox = outbox;
    this.clientRole = clientRole;
  }

  @LifecycleStart
  public void start()
  {
    discoveryProvider.getForNodeRole(clientRole).registerListener(new ClientListener());
  }

  /**
   * Listener that cancels work associated with clients that have gone away.
   */
  private class ClientListener implements DruidNodeDiscovery.Listener
  {
    @Override
    public void nodesAdded(Collection<DiscoveryDruidNode> nodes)
    {
      // Nothing to do. Although, perhaps it would make sense to *set up* an outbox here. (Currently, outboxes are
      // created on-demand as they receive messages.)
    }

    @Override
    public void nodesRemoved(Collection<DiscoveryDruidNode> nodes)
    {
      final Set<String> hostsRemoved =
          nodes.stream().map(node -> node.getDruidNode().getHostAndPortToUse()).collect(Collectors.toSet());

      for (final String clientHost : hostsRemoved) {
        outbox.resetOutbox(clientHost);
      }
    }
  }
}
