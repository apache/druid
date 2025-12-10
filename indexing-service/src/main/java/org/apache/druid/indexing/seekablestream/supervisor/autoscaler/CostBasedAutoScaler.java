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
import java.util.Collections;
import java.util.HashMap;
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
  private static final String OPTIMAL_TASK_COUNT_METRIC = "task/autoScaler/costBased/optimalTaskCount";

  private static final Map<Integer, List<Integer>> FACTORS_CACHE = new HashMap<>();

  private final String dataSource;
  /**
   * Atomic reference to CostMetrics object. All operations must be performed
   * with sequentially consistent semantics (volatile reads/writes).
   * However, it may be fine-tuned with acquire/release semantics,
   * but requires careful reasoning about correctness.
   */
  private final AtomicReference<CostMetrics> currentMetrics;
  private final ScheduledExecutorService metricsCollectionExec;
  private final ScheduledExecutorService scalingDecisionExec;
  private final SupervisorSpec spec;
  private final SeekableStreamSupervisor supervisor;
  private final CostBasedAutoScalerConfig config;
  private final ServiceEmitter emitter;
  private final ServiceMetricEvent.Builder metricBuilder;
  private final WeightedCostFunction costFunction;

  public CostBasedAutoScaler(
      SeekableStreamSupervisor supervisor,
      String dataSource,
      CostBasedAutoScalerConfig config,
      SupervisorSpec spec,
      ServiceEmitter emitter
  )
  {
    this.dataSource = dataSource;
    this.config = config;
    this.spec = spec;
    this.supervisor = supervisor;
    this.emitter = emitter;

    final String supervisorId = StringUtils.format("Supervisor-%s", dataSource);

    this.currentMetrics = new AtomicReference<>(null);
    this.costFunction = new WeightedCostFunction();

    this.metricsCollectionExec = Execs.scheduledSingleThreaded(
        StringUtils.encodeForFormat(supervisorId) + "-CostBasedMetrics-%d"
    );
    this.scalingDecisionExec = Execs.scheduledSingleThreaded(
        StringUtils.encodeForFormat(supervisorId) + "-CostBasedScaling-%d"
    );

    this.metricBuilder = ServiceMetricEvent.builder()
                                           .setDimension(DruidMetrics.DATASOURCE, dataSource)
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

    metricsCollectionExec.scheduleAtFixedRate(
        collectMetrics(),
        config.getMetricsCollectionIntervalMillis(),
        config.getMetricsCollectionIntervalMillis(),
        TimeUnit.MILLISECONDS
    );

    scalingDecisionExec.scheduleAtFixedRate(
        supervisor.buildDynamicAllocationTask(scaleAction, onSuccessfulScale, emitter),
        config.getScaleActionStartDelayMillis(),
        config.getScaleActionPeriodMillis(),
        TimeUnit.MILLISECONDS
    );

    log.info(
        "CostBasedAutoScaler started for dataSource [%s]: collecting metrics every [%d]ms, "
        + "evaluating scaling every [%d]ms",
        dataSource,
        config.getMetricsCollectionIntervalMillis(),
        config.getScaleActionPeriodMillis()
    );
  }

  @Override
  public void stop()
  {
    scalingDecisionExec.shutdownNow();
    metricsCollectionExec.shutdownNow();
    log.info("CostBasedAutoScaler stopped for dataSource [%s]", dataSource);
  }

  @Override
  public void reset()
  {
    currentMetrics.set(null);
  }

  private Runnable collectMetrics()
  {
    return () -> {
      if (spec.isSuspended()) {
        log.debug("Supervisor [%s] is suspended, skipping metrics collection", dataSource);
        return;
      }

      final LagStats lagStats = supervisor.computeLagStats();
      if (lagStats == null) {
        log.debug("Lag stats unavailable for dataSource [%s], skipping collection", dataSource);
        return;
      }

      final int currentTaskCount = supervisor.getActiveTaskGroupsCount();
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

      log.debug("Collected metrics for dataSource [%s]", dataSource);
    };
  }

  /**
   * @return optimal task count, or -1 if no scaling action needed
   */
  public int computeOptimalTaskCount(AtomicReference<CostMetrics> currentMetricsRef)
  {
    final CostMetrics currentMetrics = currentMetricsRef.get();
    if (currentMetrics == null) {
      log.debug("No metrics available yet for dataSource [%s]", dataSource);
      return -1;
    }

    if (currentMetrics.getPartitionCount() <= 0 || currentMetrics.getCurrentTaskCount() <= 0) {
      return -1;
    }

    final int currentTaskCount = currentMetrics.getCurrentTaskCount();
    final List<Integer> validTaskCounts = FACTORS_CACHE.computeIfAbsent(
        currentMetrics.getPartitionCount(),
        this::computeFactors
    );

    if (validTaskCounts.isEmpty()) {
      log.warn("No valid task counts after applying constraints for dataSource [%s]", dataSource);
      return -1;
    }

    // Update bounds with observed lag BEFORE optimization loop
    // This ensures normalization uses historical observed values, not predicted values
    costFunction.updateLagBounds(currentMetrics.getAvgPartitionLag());

    int optimalTaskCount = -1;
    double optimalCost = Double.POSITIVE_INFINITY;

    // TODO: what if somehow it is not in the validTaskCounts list?
    final int bestTaskCountIndex = validTaskCounts.indexOf(currentTaskCount);
    for (int i = bestTaskCountIndex - SCALE_FACTOR_DISCRETE_DISTANCE;
         i <= bestTaskCountIndex + SCALE_FACTOR_DISCRETE_DISTANCE; i++) {
      // Range check.
      if (i < 0 || i >= validTaskCounts.size()) {
        continue;
      }
      int taskCount = validTaskCounts.get(i);
      if (taskCount < config.getTaskCountMin()) {
        continue;
      } else if (taskCount > config.getTaskCountMax()) {
        break;
      }
      double cost = costFunction.computeCost(currentMetrics, taskCount, config);
      log.debug("Proposed task count: %d, Cost: %.4f", taskCount, cost);
      if (cost < optimalCost) {
        optimalTaskCount = taskCount;
        optimalCost = cost;
      }
    }

    emitter.emit(ServiceMetricEvent.builder().setMetric(OPTIMAL_TASK_COUNT_METRIC, (long) optimalTaskCount));

    log.info(
        "Cost-based scaling evaluation for dataSource [%s]: current=%d, optimal=%d, cost=%.4f, "
        + "avgPartitionLag=%.2f, pollIdleRatio=%.3f, validOptions=%d",
        dataSource,
        currentMetrics.getCurrentTaskCount(),
        optimalTaskCount,
        optimalCost,
        currentMetrics.getAvgPartitionLag(),
        currentMetrics.getPollIdleRatio(),
        validTaskCounts.size()
    );

    if (optimalTaskCount > currentTaskCount) {
      log.info(
          "Scale UP dataSource [%s] from %d to %d tasks",
          dataSource,
          currentTaskCount,
          optimalTaskCount
      );
      return optimalTaskCount;
    } else if (optimalTaskCount < currentTaskCount) {
      log.info(
          "Scale DOWN dataSource [%s] from %d to %d tasks (during rollover only)",
          dataSource,
          currentTaskCount,
          optimalTaskCount
      );
      config.setTaskCountStart(optimalTaskCount);
      return -1;
    } else {
      log.debug("No scaling action needed for dataSource [%s], staying at %d tasks", dataSource, optimalTaskCount);
      return -1;
    }
  }

  /**
   * Generates valid task counts based on partitions-per-task ratios.
   * This enables gradual scaling and avoids large jumps.
   *
   * @return sorted list of valid task counts within bounds
   */
  List<Integer> computeFactors(int partitionCount)
  {
    if (partitionCount <= 0) {
      return Collections.emptyList();
    }

    List<Integer> result = new ArrayList<>();

    for (int partitionsPerTask = partitionCount; partitionsPerTask >= 1; partitionsPerTask--) {
      int taskCount = (partitionCount + partitionsPerTask - 1) / partitionsPerTask;
      if (result.isEmpty() || result.get(result.size() - 1) != taskCount) {
        result.add(taskCount);
      }
    }

    return result;
  }

  public CostBasedAutoScalerConfig getConfig()
  {
    return config;
  }
}
