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

import java.util.List;

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
        .computeNumSegmentsToMoveInTierToFixSkew(tier, historicals);
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
   * [{@link #MIN_SEGMENTS_TO_MOVE}, {@code ~0.6% of totalSegmentsInTier}].
   */
  public static int computeMinSegmentsToMoveInTier(int totalSegmentsInTier)
  {
    // Divide by 2^14 and multiply by 100 so that the value increases
    // in steps of 100 for every 2^14 = ~16k segments
    int upperBound = (totalSegmentsInTier >> 14) * 100;
    int lowerBound = Math.min(MIN_SEGMENTS_TO_MOVE, totalSegmentsInTier);
    return Math.max(lowerBound, upperBound);
  }

  /**
   * Calculates the maximum number of segments that can be picked for moving in
   * the cluster in a single coordinator run, assuming that the run must finish
   * within 40s. A typical coordinator run period is 1 minute and there should
   * be a buffer of 20s for other coordinator duties.
   * <p>
   * This value must be calculated at the cluster level and then applied
   * to every tier so that the total computation time is estimated correctly.
   * <p>
   * Each balancer thread can perform 2 billion computations in 40s (see #14584).
   * Therefore,
   * <pre>
   * numComputations = maxSegmentsToMove * totalSegments
   *
   * maxSegmentsToMove = numComputations / totalSegments
   *                   = (nThreads * 2B) / totalSegments
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
      int numBalancerThreads
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

    // Each thread can do ~2B computations in one cycle = 2M * 1k = 2^21 * 1k
    int maxComputationsInThousands = numBalancerThreads << 21;
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
  public static int computeNumSegmentsToMoveInTierToFixSkew(String tier, List<ServerHolder> historicals)
  {
    if (historicals.isEmpty()) {
      return 0;
    }

    final long totalUsageBytes = getTotalUsageBytes(historicals);
    final Object2IntMap<String> datasourceToTotalSegments = getTotalSegmentsPerDatasource(historicals);

    final int totalSegments = datasourceToTotalSegments.values().intStream().sum();
    if (totalSegments <= 0) {
      return 0;
    }

    final Object2IntMap<String> datasourceToTotalDeviation
        = computeDatasourceToTotalDeviation(historicals, datasourceToTotalSegments);
    findAndLogMostSkewedDatasource(tier, historicals, datasourceToTotalDeviation);

    final int totalDeviationByCounts = datasourceToTotalDeviation.values().intStream().sum();

    final int totalDeviationByUsage
        = computeTotalDeviationBasedOnDiskUsage(tier, historicals, totalUsageBytes, totalSegments);

    // Move half of the total absolute deviation
    return Math.max(totalDeviationByCounts, totalDeviationByUsage) / 2;
  }

  private static Object2IntMap<String> getTotalSegmentsPerDatasource(List<ServerHolder> servers)
  {
    final Object2IntMap<String> datasourceToTotalSegments = new Object2IntOpenHashMap<>();
    for (ServerHolder server : servers) {
      server.getProjectedSegments().getDatasourceToTotalSegmentCount().object2IntEntrySet().forEach(
          entry -> datasourceToTotalSegments.mergeInt(entry.getKey(), entry.getIntValue(), Integer::sum)
      );
    }
    return datasourceToTotalSegments;
  }

  private static long getTotalUsageBytes(List<ServerHolder> servers)
  {
    return servers.stream()
                  .mapToLong(server -> server.getProjectedSegments().getTotalSegmentBytes())
                  .sum();
  }

  /**
   * Computes the sum of absolute deviations from the average number of segments
   * for each datasource.
   */
  private static Object2IntMap<String> computeDatasourceToTotalDeviation(
      List<ServerHolder> servers,
      Object2IntMap<String> datasourceToTotalSegments
  )
  {
    final int numServers = servers.size();
    final Object2IntMap<String> datasourceToAverageSegments = new Object2IntOpenHashMap<>();
    datasourceToTotalSegments.object2IntEntrySet().forEach(
        entry -> datasourceToAverageSegments.put(entry.getKey(), entry.getIntValue() / numServers)
    );

    final Object2IntMap<String> datasourceToTotalDeviation = new Object2IntOpenHashMap<>();
    for (ServerHolder server : servers) {
      SegmentCountsPerInterval projectedSegments = server.getProjectedSegments();
      for (Object2IntMap.Entry<String> entry :
          projectedSegments.getDatasourceToTotalSegmentCount().object2IntEntrySet()) {
        final String datasource = entry.getKey();
        int averageSegmentsForDatasource = datasourceToAverageSegments.getInt(datasource);
        datasourceToTotalDeviation.mergeInt(
            datasource,
            Math.abs(entry.getIntValue() - averageSegmentsForDatasource),
            Integer::sum
        );
      }
    }

    return datasourceToTotalDeviation;
  }

  private static int computeTotalDeviationBasedOnDiskUsage(
      String tier,
      List<ServerHolder> servers,
      long totalUsageBytes,
      int totalSegments
  )
  {
    final long averageUsageBytes = totalUsageBytes / servers.size();

    long totalDeviationInUsageBytes = 0;
    double maxDiskUsage = 0.0;
    double minDiskUsage = 100.0;
    for (ServerHolder server : servers) {
      final SegmentCountsPerInterval projectedSegments = server.getProjectedSegments();

      long serverUsageBytes = projectedSegments.getTotalSegmentBytes();
      totalDeviationInUsageBytes += Math.abs(serverUsageBytes - averageUsageBytes);

      double diskUsage = (100.0 * projectedSegments.getTotalSegmentBytes()) / server.getMaxSize();
      maxDiskUsage = Math.max(diskUsage, maxDiskUsage);
      minDiskUsage = Math.min(diskUsage, minDiskUsage);
    }

    log.info(
        "Disk usages in tier[%s] are in the range[min=%.1f%%, max=%.1f%%].",
        tier, minDiskUsage, maxDiskUsage
    );

    final double averageSegmentSize = (1.0 * totalUsageBytes) / totalSegments;
    return (int) (totalDeviationInUsageBytes / averageSegmentSize);
  }

  private static void findAndLogMostSkewedDatasource(
      String tier,
      List<ServerHolder> servers,
      Object2IntMap<String> datasourceToTotalDeviation
  )
  {
    String mostSkewedDatasource = null;
    int maxDeviation = 0;
    for (Object2IntMap.Entry<String> entry : datasourceToTotalDeviation.object2IntEntrySet()) {
      if (entry.getIntValue() > maxDeviation) {
        maxDeviation = entry.getIntValue();
        mostSkewedDatasource = entry.getKey();
      }
    }

    int minNumSegments = Integer.MAX_VALUE;
    int maxNumSegments = 0;
    for (ServerHolder server : servers) {
      int countForSkewedDatasource = server.getProjectedSegments()
                                           .getDatasourceToTotalSegmentCount()
                                           .getInt(mostSkewedDatasource);

      minNumSegments = Math.min(minNumSegments, countForSkewedDatasource);
      maxNumSegments = Math.max(maxNumSegments, countForSkewedDatasource);
    }

    log.info(
        "Most unbalanced datasource[%s] in tier[%s] has counts[min=%,d, max=%,d].",
        mostSkewedDatasource, tier, minNumSegments, maxNumSegments
    );
  }

  private SegmentToMoveCalculator()
  {
    // no instantiation
  }
}
