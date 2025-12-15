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
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Cost-based auto-scaler for seekable stream supervisors.
 * Uses a cost function combining lag and idle time metrics to determine optimal task counts.
 * Task counts are selected from pre-calculated values (not arbitrary factors).
 * Scale-up and scale-down are both performed proactively.
 * Future versions may perform scale-down on task rollover only.
 */
public class CostBasedAutoScaler implements SupervisorTaskAutoScaler
{
  private static final EmittingLogger log = new EmittingLogger(CostBasedAutoScaler.class);

  private static final int MAX_INCREASE_IN_PARTITIONS_PER_TASK = 2;
  private static final int MIN_INCREASE_IN_PARTITIONS_PER_TASK = MAX_INCREASE_IN_PARTITIONS_PER_TASK * 2;
  public static final String OPTIMAL_TASK_COUNT_METRIC = "task/autoScaler/costBased/optimalTaskCount";

  private final String supervisorId;
  private final SeekableStreamSupervisor supervisor;
  private final ServiceEmitter emitter;
  private final SupervisorSpec spec;
  private final CostBasedAutoScalerConfig config;
  private final ServiceMetricEvent.Builder metricBuilder;
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

    this.costFunction = new WeightedCostFunction();

    this.autoscalerExecutor = Execs.scheduledSingleThreaded("CostBasedAutoScaler-" + StringUtils.encodeForFormat(spec.getId()));
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
    Callable<Integer> scaleAction = () -> computeOptimalTaskCount(this.collectMetrics());
    Runnable onSuccessfulScale = () -> {
    };

    autoscalerExecutor.scheduleAtFixedRate(
        supervisor.buildDynamicAllocationTask(scaleAction, onSuccessfulScale, emitter),
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
    final double pollIdleRatio = supervisor.getPollIdleRatioMetric();

    return new CostMetrics(lagStats.getAvgLag(), currentTaskCount, partitionCount, pollIdleRatio);
  }

  /**
   * Computes the optimal task count based on current metrics.
   * <p>
   * Returns -1 (no scaling needed) in the following cases:
   * <ul>
   *   <li>Metrics are not available</li>
   *   <li>The current idle ratio is in the ideal range [0.2, 0.6] - optimal utilization achieved</li>
   *   <li>Optimal task count equals current task count</li>
   * </ul>
   *
   * @return optimal task count for scale-up, or -1 if no scaling action needed
   */
  public int computeOptimalTaskCount(CostMetrics metrics)
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

    final int[] validTaskCounts = CostBasedAutoScaler.computeValidTaskCounts(partitionCount, currentTaskCount);

    if (validTaskCounts.length == 0) {
      log.warn("No valid task counts after applying constraints for supervisorId [%s]", supervisorId);
      return -1;
    }

    // If idle is already in the ideal range [0.2, 0.6], optimal utilization has been achieved.
    // No scaling is needed - maintain stability by staying at current task count.
    final double currentIdleRatio = metrics.getPollIdleRatio();
    if (currentIdleRatio >= 0 && WeightedCostFunction.isIdleInIdealRange(currentIdleRatio)) {
      log.info(
          "Idle ratio [%.3f] is in ideal range for supervisorId [%s], no scaling needed",
          currentIdleRatio,
          supervisorId
      );
      return -1;
    }

    int optimalTaskCount = -1;
    double optimalCost = Double.POSITIVE_INFINITY;

    for (int taskCount : validTaskCounts) {
      double cost = costFunction.computeCost(metrics, taskCount, config);
      log.debug("Proposed task count: %d, Cost: %.4f", taskCount, cost);
      if (cost < optimalCost) {
        optimalTaskCount = taskCount;
        optimalCost = cost;
      }
    }

    emitter.emit(metricBuilder.setMetric(OPTIMAL_TASK_COUNT_METRIC, (long) optimalTaskCount));

    log.debug(
        "Cost-based scaling evaluation for supervisorId [%s]: current=%d, optimal=%d, cost=%.4f, "
        + "avgPartitionLag=%.2f, pollIdleRatio=%.3f",
        supervisorId,
        metrics.getCurrentTaskCount(),
        optimalTaskCount,
        optimalCost,
        metrics.getAvgPartitionLag(),
        metrics.getPollIdleRatio()
    );

    if (optimalTaskCount == currentTaskCount) {
      return -1;
    }
    // Perform both scale-up and scale-down proactively
    // Future versions may perform scale-down on task rollover only
    return optimalTaskCount;
  }

  /**
   * Generates valid task counts based on partitions-per-task ratios.
   * This enables gradual scaling and avoids large jumps.
   * Limits the range of task counts considered to avoid excessive computation.
   *
   * @return sorted list of valid task counts within bounds
   */
  static int[] computeValidTaskCounts(int partitionCount, int currentTaskCount)
  {
    if (partitionCount <= 0) {
      return new int[]{};
    }

    List<Integer> result = new ArrayList<>();
    final int currentPartitionsPerTask = partitionCount / currentTaskCount;
    // Minimum partitions per task corresponds to maximum number of tasks (scale up) and vice versa.
    final int minPartitionsPerTask = Math.max(1, currentPartitionsPerTask - MAX_INCREASE_IN_PARTITIONS_PER_TASK);
    final int maxPartitionsPerTask = Math.min(
        partitionCount,
        Math.max(
            minPartitionsPerTask,
            currentPartitionsPerTask + MIN_INCREASE_IN_PARTITIONS_PER_TASK
        )
    );

    for (int partitionsPerTask = maxPartitionsPerTask; partitionsPerTask >= minPartitionsPerTask; partitionsPerTask--) {
      final int taskCount = (partitionCount + partitionsPerTask - 1) / partitionsPerTask;
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
