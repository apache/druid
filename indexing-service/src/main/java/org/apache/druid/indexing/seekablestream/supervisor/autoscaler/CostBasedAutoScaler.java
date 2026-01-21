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

package org.apache.druid.indexing.seekablestream.supervisor.autoscaler;

import org.apache.druid.indexing.common.stats.DropwizardRowIngestionMeters;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.LagStats;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.SupervisorTaskAutoScaler;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisor;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.segment.incremental.RowIngestionMeters;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Cost-based auto-scaler for seekable stream supervisors.
 * Uses a weighted cost function combining lag recovery time (seconds) and idleness cost (seconds)
 * to determine optimal task counts.
 * <p>
 * Candidate task counts are derived by scanning a bounded window of partitions-per-task (PPT) values
 * around the current PPT, then converting those to task counts. This allows non-divisor task counts
 * while keeping changes gradual (no large jumps).
 * <p>
 * Scale-up and scale-down are both evaluated proactively.
 * Future versions may perform scale-down on task rollover only.
 */
public class CostBasedAutoScaler implements SupervisorTaskAutoScaler
{
  private static final EmittingLogger log = new EmittingLogger(CostBasedAutoScaler.class);

  private static final int MAX_INCREASE_IN_PARTITIONS_PER_TASK = 2;
  private static final int MAX_DECREASE_IN_PARTITIONS_PER_TASK = MAX_INCREASE_IN_PARTITIONS_PER_TASK * 2;
  private static final int LAG_STEP = 100_000;
  private static final int BASE_RAW_EXTRA = 5;
  static final int LAG_ACTIVATION_THRESHOLD = 50_000;
  static final int LAG_AGGRESSIVE_THRESHOLD = 100_000;

  public static final String LAG_COST_METRIC = "task/autoScaler/costBased/lagCost";
  public static final String IDLE_COST_METRIC = "task/autoScaler/costBased/idleCost";
  public static final String OPTIMAL_TASK_COUNT_METRIC = "task/autoScaler/costBased/optimalTaskCount";

  private final String supervisorId;
  private final SeekableStreamSupervisor supervisor;
  private final ServiceEmitter emitter;
  private final SupervisorSpec spec;
  private final CostBasedAutoScalerConfig config;
  private final ServiceMetricEvent.Builder metricBuilder;
  private final ScheduledExecutorService autoscalerExecutor;
  private final WeightedCostFunction costFunction;
  private volatile CostMetrics lastKnownMetrics;

  public CostBasedAutoScaler(
      SeekableStreamSupervisor supervisor,
      CostBasedAutoScalerConfig config,
      SupervisorSpec spec,
      ServiceEmitter emitter
  )
  {
    this.config = config;
    this.spec = spec;
    this.supervisor = supervisor;
    this.supervisorId = spec.getId();
    this.emitter = emitter;

    this.costFunction = new WeightedCostFunction();

    this.autoscalerExecutor = Execs.scheduledSingleThreaded("CostBasedAutoScaler-"
                                                            + StringUtils.encodeForFormat(spec.getId()));
    this.metricBuilder = ServiceMetricEvent.builder()
                                           .setDimension(DruidMetrics.SUPERVISOR_ID, supervisorId)
                                           .setDimension(
                                               DruidMetrics.STREAM,
                                               this.supervisor.getIoConfig().getStream()
                                           );
  }

  @Override
  public void start()
  {
    autoscalerExecutor.scheduleAtFixedRate(
        supervisor.buildDynamicAllocationTask(
            this::computeTaskCountForScaleAction, () -> {
            }, emitter
        ),
        config.getScaleActionPeriodMillis(),
        config.getScaleActionPeriodMillis(),
        TimeUnit.MILLISECONDS
    );

    log.info(
        "CostBasedAutoScaler started for supervisorId[%s]: evaluating scaling every [%d]ms",
        supervisorId,
        config.getScaleActionPeriodMillis()
    );
  }

  @Override
  public void stop()
  {
    autoscalerExecutor.shutdownNow();
    log.info("CostBasedAutoScaler stopped for supervisorId [%s]", supervisorId);
  }

  @Override
  public void reset()
  {
    // No-op.
  }

  @Override
  public int computeTaskCountForRollover()
  {
    return computeOptimalTaskCount(lastKnownMetrics);
  }

  public int computeTaskCountForScaleAction()
  {
    lastKnownMetrics = collectMetrics();
    final int optimalTaskCount = computeOptimalTaskCount(lastKnownMetrics);
    final int currentTaskCount = lastKnownMetrics.getCurrentTaskCount();

    // Perform only scale-up actions
    return optimalTaskCount >= currentTaskCount ? optimalTaskCount : -1;
  }

  public CostBasedAutoScalerConfig getConfig()
  {
    return config;
  }

  /**
   * Computes the optimal task count based on current metrics.
   * <p>
   * Returns -1 (no scaling needed) in the following cases:
   * <ul>
   *   <li>Metrics are not available</li>
   *   <li>Task count already optimal</li>
   *   <li>The current idle ratio is in the ideal range and lag considered low</li>
   *   <li>Optimal task count equals current task count</li>
   * </ul>
   *
   * @return optimal task count for scale-up, or -1 if no scaling action needed
   */
  int computeOptimalTaskCount(CostMetrics metrics)
  {
    if (metrics == null) {
      log.debug("No metrics available yet for supervisorId [%s]", supervisorId);
      return -1;
    }

    final int partitionCount = metrics.getPartitionCount();
    final int currentTaskCount = metrics.getCurrentTaskCount();
    if (partitionCount <= 0 || currentTaskCount <= 0) {
      return -1;
    }

    final int actualTaskCountMax = Math.min(config.getTaskCountMax(), partitionCount);
    final int[] validTaskCounts = CostBasedAutoScaler.computeValidTaskCounts(
        partitionCount,
        currentTaskCount,
        (long) metrics.getAggregateLag(),
        actualTaskCountMax
    );

    if (validTaskCounts.length == 0) {
      log.warn("No valid task counts after applying constraints for supervisorId [%s]", supervisorId);
      return -1;
    }

    int optimalTaskCount = -1;
    CostResult optimalCost = new CostResult();

    for (int taskCount : validTaskCounts) {
      CostResult costResult = costFunction.computeCost(metrics, taskCount, config);
      double cost = costResult.totalCost();
      log.debug(
          "Proposed task count: %d, Cost: %.4f (lag: %.4f, idle: %.4f)",
          taskCount,
          cost,
          costResult.lagCost(),
          costResult.idleCost()
      );
      if (cost < optimalCost.totalCost()) {
        optimalTaskCount = taskCount;
        optimalCost = costResult;
      }
    }

    emitter.emit(metricBuilder.setMetric(OPTIMAL_TASK_COUNT_METRIC, (long) optimalTaskCount));
    emitter.emit(metricBuilder.setMetric(LAG_COST_METRIC, optimalCost.lagCost()));
    emitter.emit(metricBuilder.setMetric(IDLE_COST_METRIC, optimalCost.idleCost()));

    log.debug(
        "Cost-based scaling evaluation for supervisorId [%s]: current=%d, optimal=%d, cost=%.4f, "
        + "avgPartitionLag=%.2f, pollIdleRatio=%.3f",
        supervisorId,
        metrics.getCurrentTaskCount(),
        optimalTaskCount,
        optimalCost.totalCost(),
        metrics.getAvgPartitionLag(),
        metrics.getPollIdleRatio()
    );

    if (optimalTaskCount == currentTaskCount) {
      return -1;
    }

    // Scale up is performed eagerly.
    return optimalTaskCount;
  }

  /**
   * Generates valid task counts based on partitions-per-task ratios and lag-driven PPT relaxation.
   * This enables gradual scaling and avoids large jumps.
   * Limits the range of task counts considered to avoid excessive computation.
   *
   * @return sorted list of valid task counts within bounds
   */
  static int[] computeValidTaskCounts(
      int partitionCount,
      int currentTaskCount,
      double aggregateLag,
      int taskCountMax
  )
  {
    if (partitionCount <= 0 || currentTaskCount <= 0 || taskCountMax <= 0) {
      return new int[]{};
    }

    Set<Integer> result = new HashSet<>();
    final int currentPartitionsPerTask = partitionCount / currentTaskCount;
    final int extraIncrease = computeExtraMaxPartitionsPerTaskIncrease(
        aggregateLag,
        partitionCount,
        currentTaskCount,
        taskCountMax
    );
    final int effectiveMaxIncrease = MAX_INCREASE_IN_PARTITIONS_PER_TASK + extraIncrease;

    // Minimum partitions per task correspond to the maximum number of tasks (scale up) and vice versa.
    final int minPartitionsPerTask = Math.max(1, currentPartitionsPerTask - effectiveMaxIncrease);
    final int maxPartitionsPerTask = Math.min(
        partitionCount,
        currentPartitionsPerTask + MAX_DECREASE_IN_PARTITIONS_PER_TASK
    );

    for (int partitionsPerTask = maxPartitionsPerTask; partitionsPerTask >= minPartitionsPerTask; partitionsPerTask--) {
      final int taskCount = (partitionCount + partitionsPerTask - 1) / partitionsPerTask;
      if (taskCount <= taskCountMax) {
        result.add(taskCount);
      }
    }
    return result.stream().mapToInt(Integer::intValue).sorted().toArray();
  }

  /**
   * Computes extra allowed increase in partitions-per-task in scenarios when the average
   * per-partition lag is relatively high.
   * Generally, one of the autoscaler priorities is to keep the lag as close to zero as possible.
   */
  static int computeExtraMaxPartitionsPerTaskIncrease(
      double aggregateLag,
      int partitionCount,
      int currentTaskCount,
      int taskCountMax
  )
  {
    if (partitionCount <= 0 || taskCountMax <= 0) {
      return 0;
    }

    final double lagPerPartition = aggregateLag / partitionCount;
    if (lagPerPartition < LAG_ACTIVATION_THRESHOLD) {
      return 0;
    }

    int rawExtra = BASE_RAW_EXTRA;
    if (lagPerPartition > LAG_AGGRESSIVE_THRESHOLD) {
      rawExtra += (int) ((lagPerPartition - LAG_AGGRESSIVE_THRESHOLD) / LAG_STEP);
    }

    final double headroomRatio = Math.max(0.0, 1.0 - (double) currentTaskCount / taskCountMax);
    return (int) (rawExtra * headroomRatio);
  }

  /**
   * Extracts the average poll-idle-ratio metric from task stats.
   * This metric indicates how much time the consumer spends idle waiting for data.
   *
   * @param taskStats the stats map from supervisor.getStats()
   * @return the average poll-idle-ratio across all tasks, or 0 if no valid metrics are available
   */
  static double extractPollIdleRatio(Map<String, Map<String, Object>> taskStats)
  {
    if (taskStats == null || taskStats.isEmpty()) {
      return 0.;
    }

    double sum = 0;
    int count = 0;
    for (Map<String, Object> groupMetrics : taskStats.values()) {
      for (Object taskMetric : groupMetrics.values()) {
        if (taskMetric instanceof Map) {
          Object autoScalerMetricsMap = ((Map<?, ?>) taskMetric).get(SeekableStreamIndexTaskRunner.AUTOSCALER_METRICS_KEY);
          if (autoScalerMetricsMap instanceof Map) {
            Object pollIdleRatioAvg = ((Map<?, ?>) autoScalerMetricsMap).get(SeekableStreamIndexTaskRunner.POLL_IDLE_RATIO_KEY);
            if (pollIdleRatioAvg instanceof Number) {
              sum += ((Number) pollIdleRatioAvg).doubleValue();
              count++;
            }
          }
        }
      }
    }

    return count > 0 ? sum / count : 0.;
  }

  /**
   * Extracts the average 15-minute moving average processing rate from task stats.
   * This rate represents the historical throughput (records per second) for each task,
   * averaged across all tasks.
   *
   * @param taskStats the stats map from supervisor.getStats()
   * @return the average 15-minute processing rate across all tasks in records/second,
   * or -1 if no valid metrics are available
   */
  static double extractMovingAverage(Map<String, Map<String, Object>> taskStats)
  {
    if (taskStats == null || taskStats.isEmpty()) {
      return -1;
    }

    double sum = 0;
    int count = 0;
    for (Map<String, Object> groupMetrics : taskStats.values()) {
      for (Object taskMetric : groupMetrics.values()) {
        if (taskMetric instanceof Map) {
          Object movingAveragesObj = ((Map<?, ?>) taskMetric).get("movingAverages");
          if (movingAveragesObj instanceof Map) {
            Object buildSegmentsObj = ((Map<?, ?>) movingAveragesObj).get(RowIngestionMeters.BUILD_SEGMENTS);
            if (buildSegmentsObj instanceof Map) {
              Object movingAvgObj = ((Map<?, ?>) buildSegmentsObj).get(DropwizardRowIngestionMeters.FIFTEEN_MINUTE_NAME);
              if (movingAvgObj == null) {
                movingAvgObj = ((Map<?, ?>) buildSegmentsObj).get(DropwizardRowIngestionMeters.FIVE_MINUTE_NAME);
                if (movingAvgObj == null) {
                  movingAvgObj = ((Map<?, ?>) buildSegmentsObj).get(DropwizardRowIngestionMeters.ONE_MINUTE_NAME);
                }
              }
              if (movingAvgObj instanceof Map) {
                Object processedRate = ((Map<?, ?>) movingAvgObj).get(RowIngestionMeters.PROCESSED);
                if (processedRate instanceof Number) {
                  sum += ((Number) processedRate).doubleValue();
                  count++;
                }
              }
            }
          }
        }
      }
    }

    return count > 0 ? sum / count : -1;
  }

  private CostMetrics collectMetrics()
  {
    if (spec.isSuspended()) {
      log.debug("Supervisor [%s] is suspended, skipping a metrics collection", supervisorId);
      return null;
    }

    final LagStats lagStats = supervisor.computeLagStats();
    if (lagStats == null) {
      log.debug("Lag stats unavailable for supervisorId [%s], skipping collection", supervisorId);
      return null;
    }

    final int currentTaskCount = supervisor.getIoConfig().getTaskCount();
    final int partitionCount = supervisor.getPartitionCount();

    final Map<String, Map<String, Object>> taskStats = supervisor.getStats();
    final double movingAvgRate = extractMovingAverage(taskStats);
    final double pollIdleRatio = extractPollIdleRatio(taskStats);

    final double avgPartitionLag = lagStats.getAvgLag();

    // Use an actual 15-minute moving average processing rate if available
    final double avgProcessingRate;
    if (movingAvgRate > 0) {
      avgProcessingRate = movingAvgRate;
    } else {
      // Fallback: estimate processing rate based on the idle ratio
      final double utilizationRatio = Math.max(0.01, 1.0 - pollIdleRatio);
      avgProcessingRate = config.getDefaultProcessingRate() * utilizationRatio;
    }

    return new CostMetrics(
        avgPartitionLag,
        currentTaskCount,
        partitionCount,
        pollIdleRatio,
        supervisor.getIoConfig().getTaskDuration().getStandardSeconds(),
        avgProcessingRate
    );
  }

}
