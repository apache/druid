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

package org.apache.druid.server.coordinator.loading;

import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Provides iterators over historicals for a given tier that can load a
 * specified segment.
 * <p>
 * Once a selector is initialized with a {@link DruidCluster}, an iterator
 * returned by {@link #getServersInTierToLoadSegment(String, DataSegment)}
 * iterates over the historicals in a tier in a round robin fashion. The next
 * invocation of this method picks up where the last iterator had left off.
 * <p>
 * This class is not thread-safe and must be used from a single thread.
 */
@NotThreadSafe
public class RoundRobinServerSelector
{
  private final Map<String, CircularServerList> tierToServers = new HashMap<>();
  private final CoordinatorDynamicConfig dynamicConfig;
  private final double defaultServerFillThreshold;

  public RoundRobinServerSelector(DruidCluster cluster, CoordinatorDynamicConfig dynamicConfig, double defaultServerFillThreshold)
  {
    this.dynamicConfig = dynamicConfig;
    this.defaultServerFillThreshold = defaultServerFillThreshold;
    cluster.getManagedHistoricals().forEach(
        (tier, servers) -> tierToServers.put(tier, new CircularServerList(servers))
    );
  }

  /**
   * Returns an iterator over the servers in this tier which are eligible to
   * load the given segment.
   */
  public Iterator<ServerHolder> getServersInTierToLoadSegment(String tier, DataSegment segment)
  {
    final CircularServerList iterator = tierToServers.get(tier);
    if (iterator == null) {
      return Collections.emptyIterator();
    }

    return new EligibleServerIterator(
        segment,
        iterator,
        dynamicConfig.getTierServerFillThreshold().getOrDefault(tier, defaultServerFillThreshold)
    );
  }

  /**
   * Iterator over servers in a tier that are eligible to load a given segment.
   * <p>
   * Applies a fill-threshold preference: at construction time, scans all servers
   * to determine if any eligible server is below the threshold. If so, only
   * below-threshold servers are returned. If none qualify, the threshold is
   * relaxed so all eligible servers are returned (fallback to original behavior).
   * <p>
   * The cursor advances through the circular list across calls so that
   * subsequent invocations pick up where the last iterator left off.
   */
  private static class EligibleServerIterator implements Iterator<ServerHolder>
  {
    final CircularServerList delegate;
    final DataSegment segment;
    final double effectiveFillThreshold;

    ServerHolder nextEligible;
    int remainingIterations;

    EligibleServerIterator(DataSegment segment, CircularServerList delegate, double fillThreshold)
    {
      this.delegate = delegate;
      this.segment = segment;
      this.remainingIterations = delegate.servers.size();

      // Apply the threshold only if at least one eligible server is below it.
      // Otherwise fall back: allow any eligible server (preference-with-fallback).
      final boolean anyBelowThreshold = delegate.servers.stream()
                                                        .anyMatch(s -> s.canLoadSegment(segment)
                                                                       && s.getFillFraction() <= fillThreshold);
      this.effectiveFillThreshold = anyBelowThreshold ? fillThreshold : Double.POSITIVE_INFINITY;

      nextEligible = search();
    }

    @Override
    public boolean hasNext()
    {
      return nextEligible != null;
    }

    @Override
    public ServerHolder next()
    {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      ServerHolder previous = nextEligible;
      delegate.advanceCursor();
      nextEligible = search();
      return previous;
    }

    ServerHolder search()
    {
      while (remainingIterations-- > 0) {
        ServerHolder nextServer = delegate.peekNext();
        if (nextServer.canLoadSegment(segment) && nextServer.getFillFraction() <= effectiveFillThreshold) {
          return nextServer;
        } else {
          delegate.advanceCursor();
        }
      }

      return null;
    }
  }

  /**
   * Circular list over all servers in a tier. A single instance of this is
   * maintained for each tier.
   */
  private static class CircularServerList
  {
    final List<ServerHolder> servers = new ArrayList<>();
    int currentPosition;

    CircularServerList(Set<ServerHolder> servers)
    {
      this.servers.addAll(servers);
      //Collections.shuffle(this.servers);
    }

    void advanceCursor()
    {
      if (++currentPosition >= servers.size()) {
        currentPosition = 0;
      }
    }

    ServerHolder peekNext()
    {
      int nextPosition = currentPosition < servers.size() ? currentPosition : 0;
      return servers.get(nextPosition);
    }
  }

}
