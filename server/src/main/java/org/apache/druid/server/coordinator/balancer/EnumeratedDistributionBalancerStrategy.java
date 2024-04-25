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

package org.apache.druid.server.coordinator.balancer;

import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.util.Pair;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The EnumeratedDistributionBalancerStrategy is a strategy that uses the {@link EnumeratedDistribution} library to
 * allocate and move segments in a probabalistic way that ultimately converges per-server segment storage allocation
 * to a normalized state (with all servers having roughly equal disk space utilization).
 *
 * When asked to place a segment, the balancer assigns a probability between 0 and 1 to each potential
 * {@link ServerHolder}. The server with the most free storage space is assigned the highest probability and the server
 * with the least free storage space is assigned the lowest probability. The Balancer then samples the set of candidate
 * destinations and returns a candidate or set of candidates to the caller depending on if thie is a replication event
 * or a move event.
 *
 * When asked to pick an ordered list of {@link ServerHolder} to drop a segment from. The balancer simply returns
 * the servers in descending order of their disk utilization.
 */
public class EnumeratedDistributionBalancerStrategy implements BalancerStrategy
{
  /**
   * Use probabilistic sampling of the ServerHolder space to determine where to move a specific segment
   *
   * The ServerHolder with the most free segment storage space has the highest probability of being returned and the
   * ServerHolder with the least free segment storage space has the least probability of being returned. The probability
   * of the remaing ServerHolders being returned fills the intermediate space between these two extremes, ordered
   * by their free storage space (descending).
   *
   * Note that the sourceServer is a candidate to be selected as the destination (which essentially means no move)
   *
   * @param segmentToMove {@link DataSegment} that is being moved
   * @param sourceServer {@link ServerHolder} that the segment is being moved from
   * @param destinationServers {@link ServerHolder} candidates for the segment to be moved to.
   * @return {@link ServerHolder} picked to recieve the candidate segment
   */
  @Nullable
  @Override
  public ServerHolder findDestinationServerToMoveSegment(DataSegment segmentToMove, ServerHolder sourceServer, List<ServerHolder> destinationServers)
  {
    // Filter out servers which cannot load this segment due to storage capacity
    final List<ServerHolder> usableServerHolders =
        destinationServers.stream()
                     .filter(server -> server.canLoadSegment(segmentToMove))
                     .collect(Collectors.toList());
    if (usableServerHolders.size() == 0) {
      return sourceServer;
    }
    // Add source server to the usable server list
    usableServerHolders.add(sourceServer);

    EnumeratedDistribution<ServerHolder> distributionList = prepareDistributionList(usableServerHolders);
    return distributionList.sample();
  }

  /**
   * Use probabalistic sampling to return an Iterable over ServerHolders who are candidates to load the given segment.
   *
   * The more free segment storage space on the ServerHolder, the higher probability that it will be in the returned
   * iterable. The list of candidate ServerHolder objects is sampled serverHolders.size() times. Since the probabalistic
   * sampling will by definition, likely return the same ServerHolder(s) multiple times, the size of the de-duplicated
   * list of ServerHolders returned will not include all candidate servers. Given large enough set of serverHolders,
   * this shouldn't be a huge problem since users likely load 2 or 3 replicas of a segment. The de-duped set of
   * ServerHolder objects returned should suffice. If this is too big a risk, I'd suggest we modify the
   * {@link BalancerStrategy#findServersToLoadSegment(DataSegment, List)} signature to require an int minimumReturnSize
   * that is <= serverHolders.size(). That way, we could sample until our return list is large enough to meet the
   * callers needs.
   *
   * @param segmentToLoad {@link DataSegment} candidate that is being loaded to one or more servers.
   * @param serverHolders {@link ServerHolder} candidates to recieve the segment being loaded.
   * @return An iterator over {@link ServerHolder} objects who are able to load the segment.
   */
  @Override
  public Iterator<ServerHolder> findServersToLoadSegment(DataSegment segmentToLoad, List<ServerHolder> serverHolders)
  {
    // Filter out servers which cannot load this segment due to storage capacity
    final List<ServerHolder> usableServerHolders =
        serverHolders.stream()
                          .filter(server -> server.canLoadSegment(segmentToLoad))
                          .collect(Collectors.toList());
    if (usableServerHolders.size() == 0) {
      return null;
    }

    EnumeratedDistribution<ServerHolder> distributionList = prepareDistributionList(usableServerHolders);
    ServerHolder[] retList = new ServerHolder[usableServerHolders.size()];
    // The return list is de-duplicated so the caller doesn't need to handle deciding what to do if we suggest loading
    // the segment to the same server twice.
    return Arrays.stream(distributionList.sample(usableServerHolders.size(), retList)).distinct().iterator();
  }

  /**
   * The EnumeratedDistributionBalancer returns the hosts by allocation % descending
   *
   * This is an impl decision to force the removal from the highest allocated server first. While not strictly following
   * the idea of probabilistic sampling, it generally supports the goal of this balancer to trend towards storage
   * allocation being roughly balanced across servers.
   *
   * @param segmentToDrop {@link DataSegment} segment that is being dropped
   * @param serverHolders {@link ServerHolder} objects who are candidates to drop the segment.
   * @return An Iterator over {@link ServerHolder} objects to have the segment dropped from
   */
  @Override
  public Iterator<ServerHolder> findServersToDropSegment(DataSegment segmentToDrop, List<ServerHolder> serverHolders)
  {
    // The natural ordering of ServerHolders is least to most full. We want the opposite, that is why we reverse our
    // sorted list
    serverHolders.sort(Comparator.comparing(ServerHolder::getAvailableSize));
    return serverHolders.iterator();
  }

  @Override
  public CoordinatorRunStats getStats()
  {
    return CoordinatorRunStats.empty();
  }

  /**
   * Given list of ServerHolder objects, return {@link EnumeratedDistribution} using % disk free as probability input.
   *
   * See <a href=https://en.wikipedia.org/wiki/Probability_distribution#Discrete_probability_distribution>Discrete Probability Distribution Wiki</a>
   * for some background on the statistical methods involved. Long story short, this method returns a colleciton that
   * when sampled has a higher probability of returning servers with more free storage compared to servers with less
   * free storage.
   *
   * @param serverHolders {@link List} of {@link ServerHolder} objects to prepare the distribution over
   * @return {@link EnumeratedDistribution}
   */
  private EnumeratedDistribution<ServerHolder> prepareDistributionList(List<ServerHolder> serverHolders)
  {
    List<Pair<ServerHolder, Double>> weightedList = new ArrayList<>();
    serverHolders.forEach(holder -> weightedList.add(new Pair<>(holder, 1.0 - (holder.getPercentUsed() / 100.0))));
    return new EnumeratedDistribution<>(weightedList);
  }
}
