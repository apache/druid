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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metamx.emitter.EmittingLogger;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.timeline.DataSegment;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class ServerSelector implements DiscoverySelector<QueryableDruidServer>
{

  private static final EmittingLogger log = new EmittingLogger(ServerSelector.class);

  private final Set<QueryableDruidServer> servers = Sets.newHashSet();

  private final TierSelectorStrategy strategy;

  private final AtomicReference<DataSegment> segment;

  public ServerSelector(
      DataSegment segment,
      TierSelectorStrategy strategy
  )
  {
    this.segment = new AtomicReference<DataSegment>(segment);
    this.strategy = strategy;
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
      servers.add(server);
    }
  }

  public boolean removeServer(QueryableDruidServer server)
  {
    synchronized (this) {
      return servers.remove(server);
    }
  }

  public boolean isEmpty()
  {
    synchronized (this) {
      return servers.isEmpty();
    }
  }

  public List<DruidServerMetadata> getCandidates(final int numCandidates) {
    List<DruidServerMetadata> result = Lists.newArrayList();
    synchronized (this) {
      final DataSegment target = segment.get();
      for (Map.Entry<Integer, Set<QueryableDruidServer>> entry : toPrioritizedServers().entrySet()) {
        Set<QueryableDruidServer> servers = entry.getValue();
        TreeMap<Integer, Set<QueryableDruidServer>> tieredMap = Maps.newTreeMap();
        while (!servers.isEmpty()) {
          tieredMap.put(entry.getKey(), servers);   // strategy.pick() removes entry
          QueryableDruidServer server = strategy.pick(tieredMap, target);
          if (server == null) {
            // regard this as any server in tieredMap is not appropriate
            break;
          }
          result.add(server.getServer().getMetadata());
          if (numCandidates > 0 && result.size() >= numCandidates) {
            return result;
          }
          servers.remove(server);
        }
      }
    }
    return result;
  }

  public QueryableDruidServer pick()
  {
    synchronized (this) {
      return strategy.pick(toPrioritizedServers(), segment.get());
    }
  }

  private TreeMap<Integer, Set<QueryableDruidServer>> toPrioritizedServers()
  {
    final TreeMap<Integer, Set<QueryableDruidServer>> prioritizedServers = new TreeMap<>(strategy.getComparator());
    for (QueryableDruidServer server : servers) {
      Set<QueryableDruidServer> theServers = prioritizedServers.get(server.getServer().getPriority());
      if (theServers == null) {
        theServers = Sets.newHashSet();
        prioritizedServers.put(server.getServer().getPriority(), theServers);
      }
      theServers.add(server);
    }
    return prioritizedServers;
  }
}
