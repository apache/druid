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

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.SegmentCountsPerInterval;
import org.apache.druid.server.coordinator.ServerHolder;
import org.joda.time.Duration;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Calculates the maximum, minimum and required number of segments to move in a
 * Coordinator run for balancing.
 */
public class SegmentToMoveCalculator
{
  /**
   * At least this number of segments must be picked for moving in every cycle
   * to keep the cluster well balanced.
   */
  private static final int MIN_SEGMENTS_TO_MOVE = 100;

  private static final Logger log = new Logger(SegmentToMoveCalculator.class);

  /**
   * Calculates the number of segments to be picked for moving in the given tier,
   * based on the level of skew between the historicals in the tier.
   *
   * @param tier                    Name of tier used for logging purposes
   * @param historicals             Active historicals in tier
   * @param maxSegmentsToMoveInTier Maximum number of segments allowed to be moved
   *                                in the tier.
   * @return Number of segments to move in the tier in the range
   * [{@link #MIN_SEGMENTS_TO_MOVE}, {@code maxSegmentsToMoveInTier}].
   */
  public static int computeNumSegmentsToMoveInTier(
      String tier,
      List<ServerHolder> historicals,
      int maxSegmentsToMoveInTier
  )
  {
    final int totalSegments = historicals.stream().mapToInt(
        server -> server.getProjectedSegments().getTotalSegmentCount()
    ).sum();

    // Move at least some segments to ensure that the cluster is always balancing itself
    final int minSegmentsToMove = SegmentToMoveCalculator
        .computeMinSegmentsToMoveInTier(totalSegments);
    final int segmentsToMoveToFixDeviation = SegmentToMoveCalculator
        .computeNumSegmentsToMoveToBalanceTier(tier, historicals);
    log.info(
        "Need to move [%,d] segments in tier[%s] to attain balance. Allowed values are [min=%d, max=%d].",
        segmentsToMoveToFixDeviation, tier, minSegmentsToMove, maxSegmentsToMoveInTier
    );

    final int activeSegmentsToMove = Math.max(minSegmentsToMove, segmentsToMoveToFixDeviation);
    return Math.min(activeSegmentsToMove, maxSegmentsToMoveInTier);
  }

  /**
   * Calculates the minimum number of segments that should be considered for
   * moving in a tier, so that the cluster is always balancing itself.
   * <p>
   * This value must be calculated separately for every tier.
   *
   * @param totalSegmentsInTier Total number of all replicas of all segments
   *                            loaded or queued across all historicals in the tier.
   * @return {@code minSegmentsToMoveInTier} in the range
   * [{@link #MIN_SEGMENTS_TO_MOVE}, {@code ~0.15% of totalSegmentsInTier}].
   */
  public static int computeMinSegmentsToMoveInTier(int totalSegmentsInTier)
  {
    // Divide by 2^14 and multiply by 100 so that the value increases
    // in steps of 100 for every 2^16 = ~65k segments
    int upperBound = (totalSegmentsInTier >> 16) * 100;
    int lowerBound = Math.min(MIN_SEGMENTS_TO_MOVE, totalSegmentsInTier);
    return Math.max(lowerBound, upperBound);
  }

  /**
   * Calculates the maximum number of segments that can be picked for moving in
   * the cluster in a single coordinator run.
   * <p>
   * This value must be calculated at the cluster level and then applied
   * to every tier so that the total computation time is estimated correctly.
   * <p>
   * Each balancer thread can perform 1 billion computations in 20s (see #14584).
   * Therefore, keeping a buffer of 10s, in every 30s:
   * <pre>
   * numComputations = maxSegmentsToMove * totalSegments
   *
   * maxSegmentsToMove = numComputations / totalSegments
   *                   = (nThreads * 1B) / totalSegments
   * </pre>
   *
   * @param totalSegments Total number of all replicas of all segments loaded or
   *                      queued across all historicals in the cluster.
   * @return {@code maxSegmentsToMove} per tier in the range
   * [{@link #MIN_SEGMENTS_TO_MOVE}, ~20% of totalSegments].
   * @see <a href="https://github.com/apache/druid/pull/14584">#14584</a>
   */
  public static int computeMaxSegmentsToMovePerTier(
      int totalSegments,
      int numBalancerThreads,
      Duration coordinatorPeriod
  )
  {
    Preconditions.checkArgument(
        numBalancerThreads > 0 && numBalancerThreads <= 100,
        "Number of balancer threads must be in range (0, 100]."
    );
    if (totalSegments <= 0) {
      return 0;
    }

    // Divide by 2^9 and multiply by 100 so that the upperBound
    // increases in steps of 100 for every 2^9 = 512 segments (~20%)
    final int upperBound = (totalSegments >> 9) * 100;
    final int lowerBound = MIN_SEGMENTS_TO_MOVE;

    int num30sPeriods = Math.min(4, (int) (coordinatorPeriod.getMillis() / 30_000));

    // Each thread can do ~1B computations in 30s = 1M * 1k = 2^20 * 1k
    int maxComputationsInThousands = (numBalancerThreads * num30sPeriods) << 20;
    int maxSegmentsToMove = (maxComputationsInThousands / totalSegments) * 1000;

    if (upperBound < lowerBound) {
      return Math.min(lowerBound, totalSegments);
    } else {
      return Math.min(maxSegmentsToMove, upperBound);
    }
  }

  /**
   * Computes the number of segments that need to be moved across the historicals
   * in a tier to attain balance in terms of disk usage and segment counts per
   * data source.
   *
   * @param tier        Name of the tier used only for logging purposes
   * @param historicals List of historicals in the tier
   */
  public static int computeNumSegmentsToMoveToBalanceTier(String tier, List<ServerHolder> historicals)
  {
    if (historicals.isEmpty()) {
      return 0;
    }

    return Math.max(
        computeSegmentsToMoveToBalanceCountsPerDatasource(tier, historicals),
        computeSegmentsToMoveToBalanceDiskUsage(tier, historicals)
    );
  }

  private static double getAverageSegmentSize(List<ServerHolder> servers)
  {
    int totalSegmentCount = 0;
    long totalUsageBytes = 0;
    for (ServerHolder server : servers) {
      totalSegmentCount += server.getProjectedSegments().getTotalSegmentCount();
      totalUsageBytes += server.getProjectedSegments().getTotalSegmentBytes();
    }

    if (totalSegmentCount <= 0 || totalUsageBytes <= 0) {
      return 0;
    } else {
      return (1.0 * totalUsageBytes) / totalSegmentCount;
    }
  }

  /**
   * Computes the number of segments to move across the servers of the tier in
   * order to balance the segment counts of the most unbalanced datasource.
   */
  static int computeSegmentsToMoveToBalanceCountsPerDatasource(
      String tier,
      List<ServerHolder> servers
  )
  {
    // Find all the datasources
    final Set<String> datasources = servers.stream().flatMap(
        s -> s.getProjectedSegments().getDatasourceToTotalSegmentCount().keySet().stream()
    ).collect(Collectors.toSet());
    if (datasources.isEmpty()) {
      return 0;
    }

    // Compute the min and max number of segments for each datasource
    final Object2IntMap<String> datasourceToMaxSegments = new Object2IntOpenHashMap<>();
    final Object2IntMap<String> datasourceToMinSegments = new Object2IntOpenHashMap<>();
    for (ServerHolder server : servers) {
      final Object2IntMap<String> datasourceToSegmentCount
          = server.getProjectedSegments().getDatasourceToTotalSegmentCount();
      for (String datasource : datasources) {
        int count = datasourceToSegmentCount.getInt(datasource);
        datasourceToMaxSegments.mergeInt(datasource, count, Math::max);
        datasourceToMinSegments.mergeInt(datasource, count, Math::min);
      }
    }

    // Compute the gap between min and max for each datasource and order by largest first
    final TreeMap<Integer, String> countDiffToDatasource = new TreeMap<>(Comparator.reverseOrder());
    datasourceToMaxSegments.object2IntEntrySet().forEach(entry -> {
      String datasource = entry.getKey();
      int maxCount = entry.getIntValue();
      int minCount = datasourceToMinSegments.getInt(datasource);
      countDiffToDatasource.put(maxCount - minCount, datasource);
    });

    // Identify the most unbalanced datasource
    final Map.Entry<Integer, String> maxCountDifference = countDiffToDatasource.firstEntry();
    String mostUnbalancedDatasource = maxCountDifference.getValue();
    int minNumSegments = Integer.MAX_VALUE;
    int maxNumSegments = 0;
    for (ServerHolder server : servers) {
      int countForSkewedDatasource = server.getProjectedSegments()
                                           .getDatasourceToTotalSegmentCount()
                                           .getInt(mostUnbalancedDatasource);

      minNumSegments = Math.min(minNumSegments, countForSkewedDatasource);
      maxNumSegments = Math.max(maxNumSegments, countForSkewedDatasource);
    }

    final int numSegmentsToMove = maxCountDifference.getKey() / 2;
    if (numSegmentsToMove > 0) {
      log.info(
          "Need to move [%,d] segments of datasource[%s] in tier[%s] to fix gap between min[%,d] and max[%,d].",
          numSegmentsToMove, mostUnbalancedDatasource, tier, minNumSegments, maxNumSegments
      );
    }
    return numSegmentsToMove;
  }

  private static int computeSegmentsToMoveToBalanceDiskUsage(
      String tier,
      List<ServerHolder> servers
  )
  {
    if (servers.isEmpty()) {
      return 0;
    }

    double maxUsagePercent = 0.0;
    double minUsagePercent = 100.0;

    long maxUsageBytes = 0;
    long minUsageBytes = Long.MAX_VALUE;
    for (ServerHolder server : servers) {
      final SegmentCountsPerInterval projectedSegments = server.getProjectedSegments();

      // Track the maximum and minimum values
      long serverUsageBytes = projectedSegments.getTotalSegmentBytes();
      maxUsageBytes = Math.max(serverUsageBytes, maxUsageBytes);
      minUsageBytes = Math.min(serverUsageBytes, minUsageBytes);

      double diskUsage = server.getMaxSize() <= 0
                         ? 0 : (100.0 * projectedSegments.getTotalSegmentBytes()) / server.getMaxSize();
      maxUsagePercent = Math.max(diskUsage, maxUsagePercent);
      minUsagePercent = Math.min(diskUsage, minUsagePercent);
    }

    final double averageSegmentSize = getAverageSegmentSize(servers);
    final long differenceInUsageBytes = maxUsageBytes - minUsageBytes;
    final int numSegmentsToMove = averageSegmentSize <= 0
                                  ? 0 : (int) (differenceInUsageBytes / averageSegmentSize) / 2;

    if (numSegmentsToMove > 0) {
      log.info(
          "Need to move [%,d] segments of avg size [%,d MB] in tier[%s] to fix"
          + " disk usage gap between min[%d GB][%.1f%%] and max[%d GB][%.1f%%].",
          numSegmentsToMove, ((long) averageSegmentSize) >> 20, tier,
          minUsageBytes >> 30, minUsagePercent, maxUsageBytes >> 30, maxUsagePercent
      );
    }
    return numSegmentsToMove;
  }

  private SegmentToMoveCalculator()
  {
    // no instantiation
  }
}
