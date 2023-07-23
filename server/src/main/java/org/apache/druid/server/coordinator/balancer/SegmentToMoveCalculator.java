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

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.SegmentCountsPerInterval;
import org.apache.druid.server.coordinator.ServerHolder;

import java.util.List;
import java.util.function.IntUnaryOperator;

public class SegmentToMoveCalculator
{
  /**
   * At least this number of segments must be picked for moving in every cycle
   * to keep the cluster well balanced.
   */
  public static final int MIN_SEGMENTS_TO_MOVE = 100;

  private static final Logger log = new Logger(SegmentToMoveCalculator.class);

  /**
   * Calculates {@code maxSegmentsToMove} for the given number of segments in the
   * cluster.
   * <p>
   * Each balancer thread can perform 2 billion computations in every coordinator
   * cycle (see #14584). Therefore,
   * <pre>
   * numComputations = maxSegmentsToMove * totalSegments
   *
   * maxSegmentsToMove = numComputations / totalSegments
   *                   = (nThreads * 2B) / totalSegments
   * </pre>
   *
   * @param totalSegmentsOnHistoricals Total number of all replicas of all segments
   *                                   loaded or queued across all historicals.
   * @return {@code maxSegmentsToMove} per tier in the range [100, 12.5% of totalSegments].
   * @see <a href="https://github.com/apache/druid/pull/14584">#14584</a>
   */
  public static int computeMaxSegmentsToMove(int totalSegmentsOnHistoricals, int numBalancerThreads)
  {
    // Each thread can do ~2B computations in one cycle = 2M * 1k = 2^21 * 1k
    final IntUnaryOperator totalComputationsOnThreads = threads -> threads << 21;

    // Integer overflows here only if numThreads > 1000, but handle it all the same
    final int computationsInThousands = Math.max(
        totalComputationsOnThreads.applyAsInt(1),
        totalComputationsOnThreads.applyAsInt(numBalancerThreads)
    );

    // Perform an approx bit-wise division so that maxSegmentsToMove increases
    // only in steps and the values are nice whole numbers
    int divisor = totalSegmentsOnHistoricals;
    int quotient = computationsInThousands;
    while (divisor > 1) {
      divisor = divisor >> 1;
      quotient = quotient >> 1;
    }

    // maxSegmentsToMove(in k) = computations(in k) / totalSegments
    final int maxSegmentsToMove = quotient * 1000;

    // Define the bounds for maxSegmentsToMove
    final int lowerBound = MIN_SEGMENTS_TO_MOVE;
    final int upperBound = totalSegmentsOnHistoricals >> 3;

    // Integer overflow is unlikely here but handle it all the same
    if (maxSegmentsToMove < 0 || maxSegmentsToMove > upperBound) {
      return Math.max(lowerBound, upperBound);
    } else {
      return Math.max(lowerBound, maxSegmentsToMove);
    }
  }

  /**
   * Computes the number of segments that need to be moved across the given
   * servers to attain balance in terms of disk usage and segment counts per
   * data source.
   */
  static int computeNumSegmentsToMove(String tier, List<ServerHolder> servers)
  {
    if (servers.isEmpty()) {
      return 0;
    }

    final long totalUsageBytes = getTotalUsageBytes(servers);
    final Object2IntMap<String> datasourceToTotalSegments = getTotalSegmentsPerDatasource(servers);

    final int totalSegments = datasourceToTotalSegments.values().intStream().sum();
    if (totalSegments <= 0) {
      return 0;
    }

    final Object2IntMap<String> datasourceToTotalDeviation
        = computeDatasourceToTotalDeviation(servers, datasourceToTotalSegments);
    findAndLogMostSkewedDatasource(tier, servers, datasourceToTotalDeviation);

    final int totalDeviationByCounts = datasourceToTotalDeviation.values().intStream().sum();

    final int totalDeviationByUsage
        = computeTotalDeviationBasedOnDiskUsage(tier, servers, totalUsageBytes, totalSegments);

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
}
