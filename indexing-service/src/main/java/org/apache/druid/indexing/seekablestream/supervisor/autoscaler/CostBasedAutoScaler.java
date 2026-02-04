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

import it.unimi.dsi.fastutil.ints.IntArraySet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.indexing.common.stats.DropwizardRowIngestionMeters;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.LagStats;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.SupervisorTaskAutoScaler;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisor;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.plugins.BurstScaleUpOnHighLagPlugin;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.plugins.OptimalTaskCountBoundariesPlugin;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.segment.incremental.RowIngestionMeters;

import javax.annotation.Nullable;
import java.util.Map;
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
  private OptimalTaskCountBoundariesPlugin boundariesPlugin = null;
  private BurstScaleUpOnHighLagPlugin burstScaleUpPlugin = null;
  private volatile CostMetrics lastKnownMetrics;

  private volatile long lastScaleActionTimeMillis = -1;

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
    if (config.shouldUseTaskCountBoundaries()) {
      //noinspection InstantiationOfUtilityClass
      this.boundariesPlugin = new OptimalTaskCountBoundariesPlugin();
    }

    if (config.getHighLagThreshold() >= 0) {
      this.burstScaleUpPlugin = new BurstScaleUpOnHighLagPlugin(config.getHighLagThreshold());
    }
  }

  @Override
  public void start()
  {
    autoscalerExecutor.scheduleAtFixedRate(
        supervisor.buildDynamicAllocationTask(this::computeTaskCountForScaleAction, () -> {}, emitter),
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
    if (config.isScaleDownOnTaskRolloverOnly()) {
      return computeOptimalTaskCount(lastKnownMetrics);
    } else {
      return -1;
    }
  }

  public int computeTaskCountForScaleAction()
  {
    lastKnownMetrics = collectMetrics();
    final int optimalTaskCount = computeOptimalTaskCount(lastKnownMetrics);
    final int currentTaskCount = lastKnownMetrics.getCurrentTaskCount();

    // Perform scale-up actions; scale-down actions only if configured.
    int taskCount = -1;
    if (isScaleActionAllowed() && optimalTaskCount > currentTaskCount) {
      taskCount = optimalTaskCount;
      lastScaleActionTimeMillis = DateTimes.nowUtc().getMillis();
      log.info("New task count [%d] on supervisor [%s], scaling up", taskCount, supervisorId);
    } else if (!config.isScaleDownOnTaskRolloverOnly()
               && isScaleActionAllowed()
               && optimalTaskCount < currentTaskCount
               && optimalTaskCount > 0) {
      taskCount = optimalTaskCount;
      lastScaleActionTimeMillis = DateTimes.nowUtc().getMillis();
      log.info("New task count [%d] on supervisor [%s], scaling down", taskCount, supervisorId);
    } else {
      log.info("No scaling required for supervisor [%s]", supervisorId);
    }
    return taskCount;
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
   *   <li>Current task count already optimal</li>
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

    final int[] validTaskCounts = CostBasedAutoScaler.computeValidTaskCounts(
        partitionCount,
        currentTaskCount,
        (long) metrics.getAggregateLag(),
        config.getTaskCountMin(),
        config.getTaskCountMax(),
        boundariesPlugin,
        burstScaleUpPlugin
    );

    if (validTaskCounts.length == 0) {
      log.warn("No valid task counts after applying constraints for supervisorId [%s]", supervisorId);
      return -1;
    }

    int optimalTaskCount = -1;
    CostResult optimalCost = new CostResult();

    for (int taskCount : validTaskCounts) {
      CostResult costResult = costFunction.computeCost(metrics, taskCount, config, burstScaleUpPlugin);
      double cost = costResult.totalCost();
      log.info(
          "Proposed task count[%d] has total Cost[%.4f] = lagCost[%.4f] + idleCost[%.4f]",
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
   * Generates valid task counts based on partitions-per-task ratios.
   *
   * @return sorted list of valid task counts within bounds
   */
  @SuppressWarnings({"VariableNotUsedInsideIf", "ReassignedVariable"})
  static int[] computeValidTaskCounts(
      int partitionCount,
      int currentTaskCount,
      double aggregateLag,
      int taskCountMin,
      int taskCountMax,
      @Nullable OptimalTaskCountBoundariesPlugin taskCountBoundariesPlugin,
      @Nullable BurstScaleUpOnHighLagPlugin highLagPlugin
  )
  {
    if (partitionCount <= 0 || currentTaskCount <= 0) {
      return new int[]{};
    }

    IntSet result = new IntArraySet();
    final int currentPartitionsPerTask = partitionCount / currentTaskCount;

    // Minimum partitions per task correspond to the maximum number of tasks (scale up) and vice versa.
    int minPartitionsPerTask = partitionCount / taskCountMax;
    int maxPartitionsPerTask = partitionCount / taskCountMin;

    if (taskCountBoundariesPlugin != null) {
      maxPartitionsPerTask = Math.min(
          partitionCount,
          currentPartitionsPerTask + OptimalTaskCountBoundariesPlugin.MAX_DECREASE_IN_PARTITIONS_PER_TASK
      );

      int extraIncrease = 0;
      if (highLagPlugin != null && highLagPlugin.lagThreshold() > 0) {
        extraIncrease = highLagPlugin.computeScaleUpBoost(aggregateLag, partitionCount, currentTaskCount, taskCountMax);
      }
      int effectiveMaxIncrease = OptimalTaskCountBoundariesPlugin.MAX_INCREASE_IN_PARTITIONS_PER_TASK + extraIncrease;
      minPartitionsPerTask = Math.max(minPartitionsPerTask, currentPartitionsPerTask - effectiveMaxIncrease);
    }

    for (int partitionsPerTask = maxPartitionsPerTask; partitionsPerTask >= minPartitionsPerTask && partitionsPerTask != 0; partitionsPerTask--) {
      final int taskCount = (partitionCount + partitionsPerTask - 1) / partitionsPerTask;
      if (taskCount >= taskCountMin && taskCount <= taskCountMax) {
        result.add(taskCount);
      }
    }
    return result.toIntArray();
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

  /**
   * Determines if a scale action is currently allowed based on the elapsed time
   * since the last scale action and the configured minimum scale-down delay.
   */
  private boolean isScaleActionAllowed()
  {
    if (lastScaleActionTimeMillis < 0) {
      return true;
    }

    final long barrierMillis = config.getMinScaleDownDelay().getMillis();
    if (barrierMillis <= 0) {
      return true;
    }

    final long elapsedMillis = DateTimes.nowUtc().getMillis() - lastScaleActionTimeMillis;
    return elapsedMillis >= barrierMillis;
  }
}
