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

package org.apache.druid.client.selector;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import it.unimi.dsi.fastutil.ints.Int2ObjectRBTreeMap;
import org.apache.druid.client.DataSegmentInterner;
import org.apache.druid.client.QueryableDruidServer;
import org.apache.druid.query.CloneQueryMode;
import org.apache.druid.query.Query;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.Overshadowable;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class ServerSelector implements Overshadowable<ServerSelector>
{
  @GuardedBy("this")
  private final Int2ObjectRBTreeMap<Set<QueryableDruidServer>> historicalServers;

  @GuardedBy("this")
  private final Int2ObjectRBTreeMap<Set<QueryableDruidServer>> realtimeServers;

  private final TierSelectorStrategy strategy;

  private final AtomicReference<DataSegment> segment;

  private final HistoricalFilter filter;

  // This property is true for a used segment which has been once loaded on a historical server.
  // This is set when metadata for the segment is polled from the Coordinator.
  private boolean isQueryable;

  public ServerSelector(DataSegment segment, TierSelectorStrategy strategy, HistoricalFilter filter)
  {
    this(segment, strategy, filter, false);
  }

  public ServerSelector(
      DataSegment segment,
      TierSelectorStrategy strategy,
      HistoricalFilter filter,
      boolean isQueryable
  )
  {
    this.segment = new AtomicReference<>(DataSegmentInterner.intern(segment));
    this.strategy = strategy;
    this.historicalServers = new Int2ObjectRBTreeMap<>(strategy.getComparator());
    this.realtimeServers = new Int2ObjectRBTreeMap<>(strategy.getComparator());
    this.filter = filter;
    this.isQueryable = isQueryable;
  }

  public DataSegment getSegment()
  {
    return segment.get();
  }

  public void addServerAndUpdateSegment(QueryableDruidServer server, DataSegment segment)
  {
    synchronized (this) {
      this.segment.set(segment);
      Set<QueryableDruidServer> priorityServers;
      if (server.getServer().getType() == ServerType.HISTORICAL) {
        priorityServers = historicalServers.computeIfAbsent(
            server.getServer().getPriority(),
            p -> new HashSet<>()
        );
      } else {
        priorityServers = realtimeServers.computeIfAbsent(
            server.getServer().getPriority(),
            p -> new HashSet<>()
        );
      }
      priorityServers.add(server);
    }
  }

  public boolean removeServer(QueryableDruidServer server)
  {
    synchronized (this) {
      Int2ObjectRBTreeMap<Set<QueryableDruidServer>> servers;
      Set<QueryableDruidServer> priorityServers;
      int priority = server.getServer().getPriority();
      if (server.getServer().getType() == ServerType.HISTORICAL) {
        servers = historicalServers;
        priorityServers = historicalServers.get(priority);
      } else {
        servers = realtimeServers;
        priorityServers = realtimeServers.get(priority);
      }

      if (priorityServers == null) {
        return false;
      }

      boolean result = priorityServers.remove(server);

      if (priorityServers.isEmpty()) {
        servers.remove(priority);
      }
      return result;
    }
  }

  public boolean isEmpty()
  {
    synchronized (this) {
      return historicalServers.isEmpty() && realtimeServers.isEmpty();
    }
  }

  public List<DruidServerMetadata> getCandidates(
      final int numCandidates,
      final CloneQueryMode cloneQueryMode
  )
  {
    List<DruidServerMetadata> candidates;
    synchronized (this) {
      if (numCandidates > 0) {
        candidates = new ArrayList<>(numCandidates);
        strategy.pick(filter.getQueryableServers(historicalServers, cloneQueryMode), segment.get(), numCandidates)
            .stream()
            .map(server -> server.getServer().getMetadata())
            .forEach(candidates::add);

        if (candidates.size() < numCandidates) { //-V6007: false alarm due to a bug in PVS-Studio
          strategy.pick(realtimeServers, segment.get(), numCandidates - candidates.size())
              .stream()
              .map(server -> server.getServer().getMetadata())
              .forEach(candidates::add);
        }
        return candidates;
      } else {
        return getAllServers(cloneQueryMode);
      }
    }
  }

  public List<DruidServerMetadata> getAllServers(CloneQueryMode cloneQueryMode)
  {
    final List<DruidServerMetadata> servers = new ArrayList<>();

    synchronized (this) {
      filter.getQueryableServers(historicalServers, cloneQueryMode)
            .values()
            .stream()
            .flatMap(Collection::stream)
            .map(server -> server.getServer().getMetadata())
            .forEach(servers::add);

      realtimeServers.values()
                     .stream()
                     .flatMap(Collection::stream)
                     .map(server -> server.getServer().getMetadata())
                     .forEach(servers::add);
    }

    return servers;
  }

  @Nullable
  public <T> QueryableDruidServer pick(@Nullable Query<T> query, CloneQueryMode cloneQueryMode)
  {
    synchronized (this) {
      if (!historicalServers.isEmpty()) {
        return strategy.pick(query, filter.getQueryableServers(historicalServers, cloneQueryMode), segment.get());
      }
      return strategy.pick(query, realtimeServers, segment.get());
    }
  }

  @Override
  public boolean overshadows(ServerSelector other)
  {
    final DataSegment thisSegment = segment.get();
    final DataSegment thatSegment = other.getSegment();
    return thisSegment.overshadows(thatSegment);
  }

  @Override
  public int getStartRootPartitionId()
  {
    return segment.get().getStartRootPartitionId();
  }

  @Override
  public int getEndRootPartitionId()
  {
    return segment.get().getEndRootPartitionId();
  }

  @Override
  public String getVersion()
  {
    return segment.get().getVersion();
  }

  @Override
  public short getMinorVersion()
  {
    return segment.get().getMinorVersion();
  }

  @Override
  public short getAtomicUpdateGroupSize()
  {
    return segment.get().getAtomicUpdateGroupSize();
  }

  @Override
  public boolean hasData()
  {
    return segment.get().hasData();
  }

  public synchronized boolean isQueryable()
  {
    return isQueryable;
  }

  public synchronized void setQueryable(boolean queryable)
  {
    isQueryable = queryable;
  }

  /**
   * Checks if the segment is currently served by a realtime server, and is not served by a historical.
   */
  public boolean isRealtimeSegment()
  {
    synchronized (this) {
      return (!realtimeServers.isEmpty()) && historicalServers.isEmpty();
    }
  }
}
