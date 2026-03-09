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
import org.apache.druid.error.DruidException;
import org.apache.druid.indexing.common.stats.DropwizardRowIngestionMeters;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.LagStats;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.SupervisorTaskAutoScaler;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisor;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Either;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.utils.CollectionUtils;

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
  public static final String INVALID_METRICS_COUNT = "task/autoScaler/costBased/invalidMetrics";

  static final int MAX_INCREASE_IN_PARTITIONS_PER_TASK = 2;
  static final int MAX_DECREASE_IN_PARTITIONS_PER_TASK = MAX_INCREASE_IN_PARTITIONS_PER_TASK * 2;

  /**
   * If average partition lag crosses this value and the processing rate is
   * still zero, scaling actions are skipped and an alert is raised.
   */
  static final int MAX_IDLENESS_PARTITION_LAG = 10_000;

  /**
   * Divisor for partition count in the K formula: K = (partitionCount / K_PARTITION_DIVISOR) / sqrt(currentTaskCount).
   * This controls how aggressive the scaling is relative to partition count.
   * That value was chosen by carefully analyzing the math model behind the implementation.
   */
  static final double K_PARTITION_DIVISOR = 6.4;

  private final String supervisorId;
  private final SeekableStreamSupervisor supervisor;
  private final ServiceEmitter emitter;
  private final SupervisorSpec spec;
  private final CostBasedAutoScalerConfig config;
  private final ScheduledExecutorService autoscalerExecutor;
  private final WeightedCostFunction costFunction;
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
  }

  private ServiceMetricEvent.Builder getMetricBuilder()
  {
    return
        ServiceMetricEvent.builder()
                          .setDimension(DruidMetrics.SUPERVISOR_ID, supervisorId)
                          .setDimension(
                              DruidMetrics.DATASOURCE,
                              CollectionUtils.getOnlyElement(
                                  spec.getDataSources(),
                                  xs -> DruidException.defensive("Expected one dataSource, got[%s]", xs)
                              )
                          )
                          .setDimension(
                              DruidMetrics.STREAM,
                              this.supervisor.getIoConfig().getStream()
                          );
  }

  @SuppressWarnings("unchecked")
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
    final int taskCount;
    if (isScaleActionAllowed() && optimalTaskCount > currentTaskCount) {
      taskCount = optimalTaskCount;
      lastScaleActionTimeMillis = DateTimes.nowUtc().getMillis();
      log.info("Updating taskCount for supervisor[%s] from [%d] to [%d] (scale up).", supervisorId, currentTaskCount, taskCount);
    } else if (!config.isScaleDownOnTaskRolloverOnly()
               && isScaleActionAllowed()
               && optimalTaskCount < currentTaskCount
               && optimalTaskCount > 0) {
      taskCount = optimalTaskCount;
      lastScaleActionTimeMillis = DateTimes.nowUtc().getMillis();
      log.info("Updating taskCount for supervisor[%s] from [%d] to [%d] (scale down).", supervisorId, currentTaskCount, taskCount);
    } else {
      taskCount = -1;
      log.debug("No scaling required for supervisor [%s]", supervisorId);
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
    final Either<String, Boolean> result = validateMetricsForScaling(metrics);
    if (result.isError()) {
      log.debug("Valid metrics are not yet available for scaling supervisor[%s]", supervisorId);
      emitter.emit(
          getMetricBuilder()
              .setDimension(DruidMetrics.DESCRIPTION, result.error())
              .setMetric(INVALID_METRICS_COUNT, 1L)
      );
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
        config.shouldUseTaskCountBoundaries(),
        config.getHighLagThreshold()
    );

    if (validTaskCounts.length == 0) {
      log.warn("No valid task counts after applying constraints for supervisor[%s]", supervisorId);
      return -1;
    }

    // Start with the current task count as optimal
    int optimalTaskCount = currentTaskCount;
    CostResult optimalCost = costFunction.computeCost(metrics, currentTaskCount, config);

    log.info(
        "Computing optimal taskCount for supervisor[%s] with metrics:"
        + " currentTaskCount[%d], avgPartitionLag[%.1f], avgProcessingRate[%.1f],"
        + " pollIdleRatio[%.1f], lagWeight[%.1f], idleWeight[%.1f].",
        supervisorId,
        currentTaskCount,
        metrics.getAvgPartitionLag(),
        metrics.getAvgProcessingRate(),
        metrics.getPollIdleRatio(),
        config.getLagWeight(),
        config.getIdleWeight()
    );

    // Find the task count which reduces cost
    final CostResult[] costResults = new CostResult[validTaskCounts.length];
    for (int i = 0; i < validTaskCounts.length; ++i) {
      final int taskCount = validTaskCounts[i];
      CostResult costResult = costFunction.computeCost(metrics, taskCount, config);
      double cost = costResult.totalCost();

      costResults[i] = costResult;
      if (cost < optimalCost.totalCost()) {
        optimalTaskCount = taskCount;
        optimalCost = costResult;
      }
    }

    emitter.emit(getMetricBuilder().setMetric(OPTIMAL_TASK_COUNT_METRIC, (long) optimalTaskCount));
    emitter.emit(getMetricBuilder().setMetric(LAG_COST_METRIC, optimalCost.lagCost()));
    emitter.emit(getMetricBuilder().setMetric(IDLE_COST_METRIC, optimalCost.idleCost()));

    if (optimalTaskCount == currentTaskCount) {
      return -1;
    } else {
      log.info(
          "Optimal taskCount[%d] for supervisor[%s] has lowest cost[%.4f] out of the following candidates: %n%s",
          optimalTaskCount, supervisorId, optimalCost.totalCost(), constructCostTable(validTaskCounts, costResults)
      );
    }

    // Scale up is performed eagerly.
    return optimalTaskCount;
  }

  /**
   * Generates valid task counts based on partitions-per-task ratios.
   *
   * @return sorted list of valid task counts within bounds
   */
  @SuppressWarnings({"ReassignedVariable"})
  static int[] computeValidTaskCounts(
      int partitionCount,
      int currentTaskCount,
      double aggregateLag,
      int taskCountMin,
      int taskCountMax,
      boolean isTaskCountBoundariesEnabled,
      int highLagThreshold
  )
  {
    if (partitionCount <= 0 || currentTaskCount <= 0) {
      return new int[]{};
    }

    IntSet result = new IntArraySet();
    final int currentPartitionsPerTask = partitionCount / currentTaskCount;

    // Minimum partitions per task correspond to the maximum number of tasks (scale up) and vice versa.
    int minPartitionsPerTask = Math.min(1, partitionCount / taskCountMax);
    int maxPartitionsPerTask = Math.max(partitionCount, partitionCount / taskCountMin);

    if (isTaskCountBoundariesEnabled) {
      maxPartitionsPerTask = Math.min(
          partitionCount,
          currentPartitionsPerTask + MAX_DECREASE_IN_PARTITIONS_PER_TASK
      );

      int extraIncrease = 0;
      if (highLagThreshold > 0) {
        extraIncrease = computeExtraPPTIncrease(
            highLagThreshold,
            aggregateLag,
            partitionCount,
            currentTaskCount,
            taskCountMax
        );
      }
      int effectiveMaxIncrease = MAX_INCREASE_IN_PARTITIONS_PER_TASK + extraIncrease;
      minPartitionsPerTask = Math.max(minPartitionsPerTask, currentPartitionsPerTask - effectiveMaxIncrease);
    }

    for (int partitionsPerTask = maxPartitionsPerTask; partitionsPerTask >= minPartitionsPerTask
                                                       && partitionsPerTask != 0; partitionsPerTask--) {
      final int taskCount = (partitionCount + partitionsPerTask - 1) / partitionsPerTask;
      if (taskCount >= taskCountMin && taskCount <= taskCountMax) {
        result.add(taskCount);
      }
    }
    return result.toIntArray();
  }

  /**
   * Computes extra allowed increase in partitions-per-task in scenarios when the average per-partition lag
   * is above the configured threshold.
   * <p>
   * This uses a logarithmic formula for consistent absolute growth:
   * {@code deltaTasks = K * ln(lagSeverity)}
   * where {@code K = (partitionCount / 6.4) / sqrt(currentTaskCount)}
   * <p>
   * This ensures that small taskCount's get a massive relative boost,
   * while large taskCount's receive more measured, stable increases.
   */
  static int computeExtraPPTIncrease(
      double lagThreshold,
      double aggregateLag,
      int partitionCount,
      int currentTaskCount,
      int taskCountMax
  )
  {
    if (partitionCount <= 0 || taskCountMax <= 0 || currentTaskCount <= 0) {
      return 0;
    }

    final double lagPerPartition = aggregateLag / partitionCount;
    if (lagPerPartition < lagThreshold) {
      return 0;
    }

    final double lagSeverity = lagPerPartition / lagThreshold;

    // Logarithmic growth: ln(lagSeverity) is positive when lagSeverity > 1
    // First multoplier decreases with sqrt(currentTaskCount): aggressive when small, conservative when large
    final double deltaTasks = (partitionCount / K_PARTITION_DIVISOR) / Math.sqrt(currentTaskCount) * Math.log(
        lagSeverity);

    final double targetTaskCount = Math.min(taskCountMax, (double) currentTaskCount + deltaTasks);

    // Compute precise PPT reduction to avoid early integer truncation artifacts
    final double currentPPT = (double) partitionCount / currentTaskCount;
    final double targetPPT = (double) partitionCount / targetTaskCount;

    return Math.max(0, (int) Math.floor(currentPPT - targetPPT));
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
   * Extracts the average 15-minute, 5-minute, or 1-minute moving average processing rate
   * from task stats, depending on which is available, in this order.
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

  @Nullable
  CostMetrics collectMetrics()
  {
    if (spec.isSuspended()) {
      log.debug("Supervisor [%s] is suspended, skipping a metrics collection", supervisorId);
      return null;
    }

    final LagStats lagStats = supervisor.computeLagStats();
    final double avgPartitionLag;
    if (lagStats == null) {
      log.debug("Lag stats unavailable for supervisorId [%s], skipping collection", supervisorId);
      avgPartitionLag = -1;
    } else {
      avgPartitionLag = lagStats.getAvgLag();
    }

    final int currentTaskCount = supervisor.getIoConfig().getTaskCount();
    final int partitionCount = supervisor.getPartitionCount();

    final Map<String, Map<String, Object>> taskStats = supervisor.getStats();
    final double movingAvgRate = extractMovingAverage(taskStats);
    final double pollIdleRatio = extractPollIdleRatio(taskStats);

    return new CostMetrics(
        avgPartitionLag,
        currentTaskCount,
        partitionCount,
        pollIdleRatio,
        supervisor.getIoConfig().getTaskDuration().getStandardSeconds(),
        movingAvgRate
    );
  }

  private static String constructCostTable(int[] taskCounts, CostResult[] results)
  {
    final StringBuilder table = new StringBuilder();
    table.append("Task count ");
    for (int taskCount : taskCounts) {
      table.append(StringUtils.format("| %8d", taskCount));
    }
    table.append("\nLag cost   ");
    for (CostResult result : results) {
      table.append(StringUtils.format("| %8.1f", result.lagCost()));
    }
    table.append("\nIdle cost  ");
    for (CostResult result : results) {
      table.append(StringUtils.format("| %8.1f", result.idleCost()));
    }
    table.append("\nTotal cost ");
    for (CostResult result : results) {
      table.append(StringUtils.format("| %8.1f", result.totalCost()));
    }

    return table.toString();
  }

  /**
   * Checks if the given metrics are valid for auto-scaling. If they are not
   * valid, auto-scaling will be skipped until fresh metrics are available.
   *
   * @return Either an error or a success boolean.
   */
  private Either<String, Boolean> validateMetricsForScaling(CostMetrics metrics)
  {
    if (metrics == null) {
      return Either.error("No metrics collected");
    } else if (metrics.getAvgProcessingRate() < 0 || metrics.getPollIdleRatio() < 0) {
      return Either.error("Task metrics not available");
    } else if (metrics.getCurrentTaskCount() <= 0 || metrics.getPartitionCount() <= 0) {
      return Either.error("Supervisor metrics not available");
    } else if (metrics.getAvgPartitionLag() < 0) {
      return Either.error("Lag metrics not available");
    } else if (metrics.getAvgProcessingRate() < 1 && metrics.getAvgPartitionLag() > MAX_IDLENESS_PARTITION_LAG) {
      log.makeAlert(
          "Supervisor[%s] has high partition lag[%.0f] but processing rate is zero. Check if the tasks are stuck.",
          supervisorId, metrics.getAvgPartitionLag()
      );
      return Either.error("Lag is high but processing rate is zero");
    } else {
      return Either.value(true);
    }
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
