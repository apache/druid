/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.client.selector;

import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.coordination.ServerType;
import io.druid.timeline.DataSegment;
import it.unimi.dsi.fastutil.ints.Int2ObjectRBTreeMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class ServerSelector implements DiscoverySelector<QueryableDruidServer>
{

  private final Int2ObjectRBTreeMap<Set<QueryableDruidServer>> historicalServers;

  private final Int2ObjectRBTreeMap<Set<QueryableDruidServer>> realtimeServers;

  private final TierSelectorStrategy strategy;

  private final AtomicReference<DataSegment> segment;

  public ServerSelector(
      DataSegment segment,
      TierSelectorStrategy strategy
  )
  {
    this.segment = new AtomicReference<>(segment);
    this.strategy = strategy;
    this.historicalServers = new Int2ObjectRBTreeMap<>(strategy.getComparator());
    this.realtimeServers = new Int2ObjectRBTreeMap<>(strategy.getComparator());
  }

  public DataSegment getSegment()
  {
    return segment.get();
  }

  public void addServerAndUpdateSegment(
      QueryableDruidServer server, DataSegment segment
  )
  {
    synchronized (this) {
      this.segment.set(segment);
      Set<QueryableDruidServer> priorityServers = new HashSet<>();
      if (server.getServer().getType() == ServerType.REALTIME) {
        priorityServers = realtimeServers.computeIfAbsent(
            server.getServer().getPriority(), p -> new HashSet<>());
      } else if (server.getServer().getType() == ServerType.HISTORICAL) {
        priorityServers = historicalServers.computeIfAbsent(
            server.getServer().getPriority(), p -> new HashSet<>());
      }
      priorityServers.add(server);
    }
  }

  public boolean removeServer(QueryableDruidServer server)
  {
    synchronized (this) {
      if (server.getServer().getType() == ServerType.REALTIME) {
        return realtimeServers.computeIfPresent(
            server.getServer().getPriority(),
            (p, servers) -> servers.remove(server) ? servers : null
        ) == null ? false : true;
      } else if (server.getServer().getType() == ServerType.HISTORICAL) {
        return historicalServers.computeIfPresent(
            server.getServer().getPriority(),
            (p, servers) -> servers.remove(server) ? servers : null
        ) == null ? false : true;
      }
    }
    return false;
  }

  public boolean isEmpty()
  {
    synchronized (this) {
      return historicalServers.isEmpty() && realtimeServers.isEmpty();
    }
  }

  public List<DruidServerMetadata> getCandidates(final int numCandidates)
  {
    List<DruidServerMetadata> candidates;
    synchronized (this) {
      if (numCandidates > 0) {
        candidates = new ArrayList<>(numCandidates);
        strategy.pick(historicalServers, segment.get(), numCandidates)
            .stream()
            .map(server -> server.getServer().getMetadata())
            .forEach(candidates::add);

        if (candidates.size() < numCandidates) {
          strategy.pick(realtimeServers, segment.get(), numCandidates - candidates.size())
              .stream()
              .map(server -> server.getServer().getMetadata())
              .forEach(candidates::add);
        }
      } else {
        candidates = new ArrayList<>();
        // return all servers as candidates
        historicalServers.values()
            .stream()
            .flatMap(Collection::stream)
            .map(server -> server.getServer().getMetadata())
            .forEach(candidates::add);

        realtimeServers.values()
            .stream()
            .flatMap(Collection::stream)
            .map(server -> server.getServer().getMetadata())
            .forEach(candidates::add);
      }

      return candidates;
    }
  }

  @Override
  public QueryableDruidServer pick()
  {
    synchronized (this) {
      if (!historicalServers.isEmpty()) {
        return strategy.pick(historicalServers, segment.get());
      }
      return strategy.pick(realtimeServers, segment.get());
    }
  }
}
