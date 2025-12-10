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

import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.LagStats;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.SupervisorTaskAutoScaler;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisor;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.DruidMetrics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Cost-based auto-scaler for seekable stream supervisors.
 * Uses a cost function combining lag and idle time metrics to determine optimal task counts.
 * Task counts are selected from predefined values (not arbitrary factors).
 * Scale-up happens incrementally, scale-down only during task rollover.
 */
public class CostBasedAutoScaler implements SupervisorTaskAutoScaler
{
  private static final EmittingLogger log = new EmittingLogger(CostBasedAutoScaler.class);

  private static final int SCALE_FACTOR_DISCRETE_DISTANCE = 2;
  public static final String OPTIMAL_TASK_COUNT_METRIC = "task/autoScaler/costBased/optimalTaskCount";

  private static final Map<Integer, int[]> FACTORS_CACHE = new LinkedHashMap<>();
  private static final int FACTORS_CACHE_MAX_SIZE = 10; // Enough for most scenarios, almost capacity * loadFactor.

  private final String supervisorId;
  private final SeekableStreamSupervisor supervisor;
  private final ServiceEmitter emitter;
  private final SupervisorSpec spec;
  private final CostBasedAutoScalerConfig config;
  private final ServiceMetricEvent.Builder metricBuilder;
  /**
   * Atomic reference to CostMetrics object. All operations must be performed
   * with sequentially consistent semantics (volatile reads/writes).
   * However, it may be fine-tuned with acquire/release semantics,
   * but requires careful reasoning about correctness.
   */
  private final AtomicReference<CostMetrics> currentMetrics;
  private final ScheduledExecutorService autoscalerExecutor;
  private final WeightedCostFunction costFunction;

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

    final String supervisorId = StringUtils.format("Supervisor-%s", supervisor);

    this.currentMetrics = new AtomicReference<>(null);
    this.costFunction = new WeightedCostFunction();

    this.autoscalerExecutor = Execs.scheduledSingleThreaded(StringUtils.encodeForFormat(supervisorId));
    this.metricBuilder = ServiceMetricEvent.builder()
                                           .setDimension(DruidMetrics.DATASOURCE, supervisorId)
                                           .setDimension(
                                               DruidMetrics.STREAM,
                                               this.supervisor.getIoConfig().getStream()
                                           );
  }

  @Override
  public void start()
  {
    Callable<Integer> scaleAction = () -> computeOptimalTaskCount(currentMetrics);
    Runnable onSuccessfulScale = () -> currentMetrics.set(null);

    autoscalerExecutor.scheduleAtFixedRate(
        this::collectMetrics,
        config.getMetricsCollectionIntervalMillis(),
        config.getMetricsCollectionIntervalMillis(),
        TimeUnit.MILLISECONDS
    );

    autoscalerExecutor.scheduleAtFixedRate(
        supervisor.buildDynamicAllocationTask(scaleAction, onSuccessfulScale, emitter),
        config.getScaleActionStartDelayMillis(),
        config.getScaleActionPeriodMillis(),
        TimeUnit.MILLISECONDS
    );

    log.info(
        "CostBasedAutoScaler started for dataSource [%s]: collecting metrics every [%d]ms, "
        + "evaluating scaling every [%d]ms",
        supervisorId,
        config.getMetricsCollectionIntervalMillis(),
        config.getScaleActionPeriodMillis()
    );
  }

  @Override
  public void stop()
  {
    autoscalerExecutor.shutdownNow();
    log.info("CostBasedAutoScaler stopped for dataSource [%s]", supervisorId);
  }

  @Override
  public void reset()
  {
    currentMetrics.set(null);
  }

  private void collectMetrics()
  {
    if (spec.isSuspended()) {
      log.debug("Supervisor [%s] is suspended, skipping a metrics collection", supervisorId);
      return;
    }

    final LagStats lagStats = supervisor.computeLagStats();
    if (lagStats == null) {
      log.debug("Lag stats unavailable for dataSource [%s], skipping collection", supervisorId);
      return;
    }

    final int currentTaskCount = supervisor.getIoConfig().getTaskCount();
    final int partitionCount = supervisor.getPartitionCount();
    final double pollIdleRatio = supervisor.getPollIdleRatioMetric();

    currentMetrics.set(
        new CostMetrics(
            lagStats.getAvgLag(),
            currentTaskCount,
            partitionCount,
            pollIdleRatio
        )
    );

    log.debug("Collected metrics for dataSource [%s]", supervisorId);
  }

  /**
   * @return optimal task count, or -1 if no scaling action needed
   */
  public int computeOptimalTaskCount(AtomicReference<CostMetrics> currentMetricsRef)
  {
    final CostMetrics metrics = currentMetricsRef.get();
    if (metrics == null) {
      log.debug("No metrics available yet for dataSource [%s]", supervisorId);
      return -1;
    }

    if (metrics.getPartitionCount() <= 0 || metrics.getCurrentTaskCount() <= 0) {
      return -1;
    }

    final int currentTaskCount = metrics.getCurrentTaskCount();
    final int[] validTaskCounts = FACTORS_CACHE.computeIfAbsent(metrics.getPartitionCount(), this::computeFactors);
    if (FACTORS_CACHE.size() > FACTORS_CACHE_MAX_SIZE) {
      int firstKey = FACTORS_CACHE.keySet().iterator().next();
      FACTORS_CACHE.remove(firstKey);
    }

    if (validTaskCounts.length == 0) {
      log.warn("No valid task counts after applying constraints for dataSource [%s]", supervisorId);
      return -1;
    }

    // Update bounds with observed lag BEFORE optimization loop
    // This ensures normalization uses historical observed values, not predicted values
    costFunction.updateLagBounds(metrics.getAvgPartitionLag());

    int optimalTaskCount = -1;
    double optimalCost = Double.POSITIVE_INFINITY;


    final int bestTaskCountIndex = Arrays.binarySearch(validTaskCounts, currentTaskCount);
    for (int i = bestTaskCountIndex - SCALE_FACTOR_DISCRETE_DISTANCE;
         i <= bestTaskCountIndex + SCALE_FACTOR_DISCRETE_DISTANCE; i++) {
      // Range check.
      if (i < 0 || i >= validTaskCounts.length) {
        continue;
      }
      int taskCount = validTaskCounts[i];
      if (taskCount < config.getTaskCountMin()) {
        continue;
      } else if (taskCount > config.getTaskCountMax()) {
        break;
      }
      double cost = costFunction.computeCost(metrics, taskCount, config);
      log.debug("Proposed task count: %d, Cost: %.4f", taskCount, cost);
      if (cost < optimalCost) {
        optimalTaskCount = taskCount;
        optimalCost = cost;
      }
    }

    emitter.emit(metricBuilder.setMetric(OPTIMAL_TASK_COUNT_METRIC, (long) optimalTaskCount));

    log.info(
        "Cost-based scaling evaluation for dataSource [%s]: current=%d, optimal=%d, cost=%.4f, "
        + "avgPartitionLag=%.2f, pollIdleRatio=%.3f",
        supervisorId,
        metrics.getCurrentTaskCount(),
        optimalTaskCount,
        optimalCost,
        metrics.getAvgPartitionLag(),
        metrics.getPollIdleRatio()
    );

    if (optimalTaskCount > currentTaskCount) {
      return optimalTaskCount;
    } else if (optimalTaskCount < currentTaskCount) {
      supervisor.getIoConfig().setTaskCount(optimalTaskCount);
    }
    return -1;
  }

  /**
   * Generates valid task counts based on partitions-per-task ratios.
   * This enables gradual scaling and avoids large jumps.
   *
   * @return sorted list of valid task counts within bounds
   */
  int[] computeFactors(int partitionCount)
  {
    if (partitionCount <= 0) {
      return new int[]{};
    }

    List<Integer> result = new ArrayList<>();

    for (int partitionsPerTask = partitionCount; partitionsPerTask >= 1; partitionsPerTask--) {
      int taskCount = (partitionCount + partitionsPerTask - 1) / partitionsPerTask;
      if (result.isEmpty() || result.get(result.size() - 1) != taskCount) {
        result.add(taskCount);
      }
    }
    return result.stream().mapToInt(Integer::intValue).toArray();
  }

  public CostBasedAutoScalerConfig getConfig()
  {
    return config;
  }
}
