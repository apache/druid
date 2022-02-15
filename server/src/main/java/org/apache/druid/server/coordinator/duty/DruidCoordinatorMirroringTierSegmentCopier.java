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

package org.apache.druid.server.coordinator.duty;

import com.google.common.collect.Multimap;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.coordinator.CoordinatorStats;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.coordinator.DruidCoordinatorConfig;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.timeline.DataSegment;
import org.jgrapht.Graph;
import org.jgrapht.alg.matching.MaximumWeightBipartiteMatching;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleWeightedGraph;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 */
public class DruidCoordinatorMirroringTierSegmentCopier implements DruidCoordinatorHelper
{
  public static final String ASSIGNED_COUNT = "assignedCount";
  public static final String DROPPED_COUNT = "droppedCount";
  private static final EmittingLogger log = new EmittingLogger(
      DruidCoordinatorMirroringTierSegmentCopier.class);

  private DruidCoordinatorConfig druidCoordinatorConfig;

  public DruidCoordinatorMirroringTierSegmentCopier(DruidCoordinatorConfig druidCoordinatorConfig)
  {
    this.druidCoordinatorConfig = druidCoordinatorConfig;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    CoordinatorStats stats = new CoordinatorStats();
    DruidCluster cluster = params.getDruidCluster();
    if (cluster.isEmpty()) {
      log.warn("Cluster has no server, skipping!");
      return params;
    }

    Multimap<String, String> tierToMirroringTierMap = druidCoordinatorConfig.getTierToMirroringTierMap();
    for (String primaryTier : tierToMirroringTierMap.keys()) {
      if (!cluster.getHistoricals().containsKey(primaryTier)) {
        log.warn("%s does not exist, skipping!", primaryTier);
        continue;
      }
      ServerHolder[] primaryServers = cluster.getHistoricalsByTier(primaryTier)
          .toArray(new ServerHolder[0]);
      for (String mirroringTier : tierToMirroringTierMap.get(primaryTier)) {
        if (!cluster.getHistoricals().containsKey(mirroringTier) || cluster.getHistoricals()
            .get(mirroringTier)
            .isEmpty()) {
          log.warn("Nothing in mirroring tier, skipping!");
          continue;
        }
        ServerHolder[] mirroringServers = cluster.getHistoricalsByTier(mirroringTier)
            .toArray(new ServerHolder[0]);
        Map<Integer, Integer> matches = match(primaryServers, mirroringServers);
        if (!matches.isEmpty()) {
          for (Map.Entry<Integer, Integer> match : matches.entrySet()) {
            ServerHolder mirroring = mirroringServers[match.getKey()];
            ServerHolder primary = primaryServers[match.getValue()];
            Set<DataSegment> toRemove = new HashSet<>(mirroring.getServer().iterateAllSegments());
            Set<DataSegment> toAdd = new TreeSet<>(DruidCoordinator.SEGMENT_COMPARATOR_RECENT_FIRST); // most recent first
            primary.getServer().iterateAllSegments().forEach(s -> {
              if (!toRemove.contains(s)) {
                toAdd.add(s);
              } else {
                toRemove.remove(s);
              }
            });
            for (DataSegment segment : toRemove) {
              log.info("Removing segment [%s] from server [%s] in tier [%s]",
                  segment.getId().toString(), mirroring.getServer().getName(), mirroringTier
              );
              mirroring.getPeon().dropSegment(segment, null);
            }
            stats.addToTieredStat(DROPPED_COUNT, mirroringTier, toRemove.size());
            for (DataSegment segment : toAdd) {
              log.info("Assigning segment [%s] to server [%s] in tier [%s]",
                  segment.getId().toString(), mirroring.getServer().getName(), mirroringTier
              );
              mirroring.getPeon().loadSegment(segment, null);
            }
            stats.addToTieredStat(ASSIGNED_COUNT, mirroringTier, toAdd.size());
          }
        }
      }
    }
    return params.buildFromExisting().withCoordinatorStats(stats).build();
  }

  private static Map<Integer, Integer> match(
      ServerHolder[] primaryServers,
      ServerHolder[] mirroringServers
  )
  {
    Graph<Integer, DefaultWeightedEdge> g = new SimpleWeightedGraph<>(DefaultWeightedEdge.class);
    int m = mirroringServers.length;
    int p = primaryServers.length;
    int numberOfClones = (int) Math.ceil(m * 1.0 / p);
    Set<Integer> partitionM = new LinkedHashSet<>(m);
    Set<Integer> partitionP = new LinkedHashSet<>(numberOfClones * p);
    for (int i = 0; i < m; i++) {
      g.addVertex(i);
      partitionM.add(i);
    }
    for (int i = 0; i < p * numberOfClones; i++) {
      // Adding m as the offset
      g.addVertex(m + i);
      partitionP.add(m + i);
    }
    Map<DefaultWeightedEdge, Pair<Integer, Integer>> edgeToMatch = new HashMap<>();
    for (int i = 0; i < m; i++) {
      for (int j = 0; j < p; j++) {
        long sharedBytes = computeSimilarity(mirroringServers[i], primaryServers[j]);
        for (int k = 0; k < numberOfClones; k++) {
          g.addEdge(i, m + k * p + j);
          DefaultWeightedEdge e = g.getEdge(i, m + k * p + j);
          g.setEdgeWeight(e, sharedBytes);
          edgeToMatch.put(e, Pair.of(i, j));
        }
      }
    }
    MaximumWeightBipartiteMatching<Integer, DefaultWeightedEdge> matcher =
        new MaximumWeightBipartiteMatching<>(g, partitionM, partitionP);
    return matcher.getMatching()
        .getEdges()
        .stream()
        .map(e -> edgeToMatch.get(e))
        .collect(Collectors.toMap(e -> e.lhs, e -> e.rhs));
  }

  private static long computeSimilarity(ServerHolder m, ServerHolder p)
  {
    // Give the minimum weight of 1 to force the mapping if there is no shared segment
    long sharedBytes = 1;
    Set<DataSegment> pSegments = new HashSet<DataSegment>(p.getServer().iterateAllSegments());
    for (DataSegment s : m.getServer().iterateAllSegments()) {
      if (pSegments.contains(s)) {
        sharedBytes += s.getSize();
      }
    }
    return sharedBytes;
  }
}
