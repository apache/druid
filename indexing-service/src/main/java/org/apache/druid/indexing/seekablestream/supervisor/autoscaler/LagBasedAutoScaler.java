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

import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.AggregateFunction;
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
import java.util.OptionalDouble;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class LagBasedAutoScaler implements SupervisorTaskAutoScaler
{
  private static final EmittingLogger log = new EmittingLogger(LagBasedAutoScaler.class);
  private final String dataSource;
  private final CircularFifoQueue<Long> lagMetricsQueue;
  private final ScheduledExecutorService lagComputationExec;
  private final ScheduledExecutorService allocationExec;
  private final SupervisorSpec spec;
  private final SeekableStreamSupervisor supervisor;
  private final LagBasedAutoScalerConfig lagBasedAutoScalerConfig;
  private final ServiceEmitter emitter;
  private final ServiceMetricEvent.Builder metricBuilder;

  private static final ReentrantLock LOCK = new ReentrantLock(true);

  public LagBasedAutoScaler(
      SeekableStreamSupervisor supervisor,
      String dataSource,
      LagBasedAutoScalerConfig autoScalerConfig,
      SupervisorSpec spec,
      ServiceEmitter emitter
  )
  {
    this.lagBasedAutoScalerConfig = autoScalerConfig;
    final String supervisorId = StringUtils.format("Supervisor-%s", dataSource);
    this.dataSource = dataSource;
    final int slots = (int) (lagBasedAutoScalerConfig.getLagCollectionRangeMillis() / lagBasedAutoScalerConfig
        .getLagCollectionIntervalMillis()) + 1;
    this.lagMetricsQueue = new CircularFifoQueue<>(slots);
    this.allocationExec = Execs.scheduledSingleThreaded(StringUtils.encodeForFormat(supervisorId) + "-Allocation-%d");
    this.lagComputationExec = Execs.scheduledSingleThreaded(StringUtils.encodeForFormat(supervisorId)
                                                            + "-Computation-%d");
    this.spec = spec;
    this.supervisor = supervisor;
    this.emitter = emitter;
    metricBuilder = ServiceMetricEvent.builder()
                                      .setDimension(DruidMetrics.DATASOURCE, dataSource)
                                      .setDimension(DruidMetrics.STREAM, this.supervisor.getIoConfig().getStream());
  }

  @Override
  public void start()
  {
    Callable<Integer> scaleAction = () -> {
      LOCK.lock();
      int desiredTaskCount = -1;
      try {
        desiredTaskCount = computeDesiredTaskCountUnified(new ArrayList<>(lagMetricsQueue));
      }
      catch (Exception ex) {
        log.warn(ex, "Exception while computing desired task count for [%s]", dataSource);
      }
      finally {
        LOCK.unlock();
      }
      return desiredTaskCount;
    };

    Runnable onSuccessfulScale = () -> {
      LOCK.lock();
      try {
        lagMetricsQueue.clear();
      }
      catch (Exception ex) {
        log.warn(ex, "Exception while clearing lags for [%s]", dataSource);
      }
      finally {
        LOCK.unlock();
      }
    };

    lagComputationExec.scheduleAtFixedRate(
        computeAndCollectLag(),
        lagBasedAutoScalerConfig.getScaleActionStartDelayMillis(), // wait for tasks to start up
        lagBasedAutoScalerConfig.getLagCollectionIntervalMillis(),
        TimeUnit.MILLISECONDS
    );
    allocationExec.scheduleAtFixedRate(
        supervisor.buildDynamicAllocationTask(scaleAction, onSuccessfulScale, emitter),
        lagBasedAutoScalerConfig.getScaleActionStartDelayMillis() + lagBasedAutoScalerConfig
            .getLagCollectionRangeMillis(),
        lagBasedAutoScalerConfig.getScaleActionPeriodMillis(),
        TimeUnit.MILLISECONDS
    );
    log.info(
        "LagBasedAutoScaler will collect lag every [%d] millis and will keep at most [%d] data points for the last [%d] millis for dataSource [%s]",
        lagBasedAutoScalerConfig.getLagCollectionIntervalMillis(),
        lagMetricsQueue.maxSize(),
        lagBasedAutoScalerConfig.getLagCollectionRangeMillis(),
        dataSource
    );
  }

  @Override
  public void stop()
  {
    allocationExec.shutdownNow();
    lagComputationExec.shutdownNow();
  }

  @Override
  public void reset()
  {
    // clear queue for kafka lags
    if (lagMetricsQueue != null) {
      try {
        LOCK.lock();
        lagMetricsQueue.clear();
      }
      catch (Exception e) {
        log.warn(e, "Error,when clear queue in rest action");
      }
      finally {
        LOCK.unlock();
      }
    }
  }

  /**
   * This method computes current consumer lag. Gets the total lag of all partitions and fill in the lagMetricsQueue
   *
   * @return a Runnbale object to compute and collect lag.
   */
  private Runnable computeAndCollectLag()
  {
    return () -> {
      LOCK.lock();
      try {
        if (!spec.isSuspended()) {
          LagStats lagStats = supervisor.computeLagStats();

          if (lagStats != null) {
            AggregateFunction aggregate = lagBasedAutoScalerConfig.getLagAggregate() == null ?
                                          lagStats.getAggregateForScaling() :
                                          lagBasedAutoScalerConfig.getLagAggregate();
            long lag = lagStats.getMetric(aggregate);
            lagMetricsQueue.offer(lag > 0 ? lag : 0L);
          } else {
            lagMetricsQueue.offer(0L);
          }
          log.debug("Current lags for dataSource[%s] are [%s].", dataSource, lagMetricsQueue);
        } else {
          log.debug("Supervisor[%s] is suspended, skipping lag collection", dataSource);
        }
      }
      catch (Exception e) {
        log.error(e, "Error while collecting lags");
      }
      finally {
        LOCK.unlock();
      }
    };
  }

  /**
   * This method determines whether to do scale actions based on collected lag points.
   * Current algorithm of scale is simple:
   * First of all, compute the proportion of lag points higher/lower than scaleOutThreshold/scaleInThreshold, getting scaleOutThreshold/scaleInThreshold.
   * Secondly, compare scaleOutThreshold/scaleInThreshold with triggerScaleOutFractionThreshold/triggerScaleInFractionThreshold. P.S. Scale out action has higher priority than scale in action.
   * Finaly, if scaleOutThreshold/scaleInThreshold is higher than triggerScaleOutFractionThreshold/triggerScaleInFractionThreshold, scale out/in action would be triggered.
   *
   * @param lags the lag metrics of Stream(Kafka/Kinesis)
   * @return Integer. target number of tasksCount, -1 means skip scale action.
   */
  private int computeDesiredTaskCountUnified(List<Long> lags)
  {
    log.info("Computing desired task count for [%s], based on following lags : [%s]", dataSource, lags);
    final int currentActiveTaskCount = supervisor.getActiveTaskGroupsCount();
    final int partitionCount = supervisor.getPartitionCount();
    if (partitionCount <= 0) {
      log.warn("Partition number for [%s] <= 0 ? how can it be?", dataSource);
      return -1;
    }

    final int desiredActiveTaskCount;
    if (lagBasedAutoScalerConfig.getUseProportionalScaler()) {
      desiredActiveTaskCount = computeDesiredTaskCountProportional(lags, currentActiveTaskCount);
    } else {
      desiredActiveTaskCount = computeDesiredTaskCountThreshold(lags, currentActiveTaskCount);
    }

    return applyMinMaxChecks(desiredActiveTaskCount, currentActiveTaskCount, partitionCount);
  }

  private int computeDesiredTaskCountThreshold(List<Long> lags, int currentActiveTaskCount)
  {
    int beyond = 0;
    int within = 0;
    int metricsCount = lags.size();
    for (Long lag : lags) {
      if (lag >= lagBasedAutoScalerConfig.getScaleOutThreshold()) {
        beyond++;
      }
      if (lag <= lagBasedAutoScalerConfig.getScaleInThreshold()) {
        within++;
      }
    }
    double beyondProportion = beyond * 1.0 / metricsCount;
    double withinProportion = within * 1.0 / metricsCount;

    log.debug(
        "Calculated beyondProportion is [%s] and withinProportion is [%s] for dataSource [%s].", beyondProportion,
        withinProportion, dataSource
    );

    if (beyondProportion >= lagBasedAutoScalerConfig.getTriggerScaleOutFractionThreshold()) {
      return currentActiveTaskCount + lagBasedAutoScalerConfig.getScaleOutStep();
    }

    if (withinProportion >= lagBasedAutoScalerConfig.getTriggerScaleInFractionThreshold()) {
      return currentActiveTaskCount - lagBasedAutoScalerConfig.getScaleInStep();
    }

    return currentActiveTaskCount;
  }

  private int computeDesiredTaskCountProportional(List<Long> lags, int currentActiveTaskCount)
  {
    final OptionalDouble avgLagAggregate = lags.stream().mapToLong(Long::longValue).average();
    if (avgLagAggregate.isEmpty()) {
      log.warn("Found no lag aggregate samples for [%s]", dataSource);
      return currentActiveTaskCount;
    }
    log.info("Computed avg aggregate lag for [%s] as [%s]", dataSource, avgLagAggregate);

    final int scaleOutTaskCount = proportionalTaskCountHelper(
        currentActiveTaskCount,
        lagBasedAutoScalerConfig.getScaleOutThreshold(),
        avgLagAggregate.getAsDouble(),
        lagBasedAutoScalerConfig.getTriggerScaleOutFractionThreshold()
    );
    final int scaleInTaskCount = proportionalTaskCountHelper(
        currentActiveTaskCount,
        lagBasedAutoScalerConfig.getScaleInThreshold(),
        avgLagAggregate.getAsDouble(),
        lagBasedAutoScalerConfig.getTriggerScaleInFractionThreshold()
    );
    log.info(
        "Computed scaleOutTaskCount=[%d], scaleInTaskCount=[%d] for datasource=[%s]",
        scaleOutTaskCount,
        scaleInTaskCount,
        dataSource
    );

    if (scaleOutTaskCount > currentActiveTaskCount) {
      return currentActiveTaskCount + Math.min(
          lagBasedAutoScalerConfig.getScaleOutStep(),
          scaleOutTaskCount - currentActiveTaskCount
      );
    } else if (scaleInTaskCount < currentActiveTaskCount) {
      return currentActiveTaskCount - Math.min(
          lagBasedAutoScalerConfig.getScaleInStep(),
          currentActiveTaskCount - scaleInTaskCount
      );
    }

    return currentActiveTaskCount;
  }

  private int applyMinMaxChecks(int desiredActiveTaskCount, int currentActiveTaskCount, int partitionCount)
  {
    final int actualTaskCountMax = Math.min(lagBasedAutoScalerConfig.getTaskCountMax(), partitionCount);
    final int actualTaskCountMin = Math.min(lagBasedAutoScalerConfig.getTaskCountMin(), partitionCount);

    if (desiredActiveTaskCount == currentActiveTaskCount) {
      log.debug("No change in task count for dataSource [%s].", dataSource);
      return -1;
    }

    if (currentActiveTaskCount == actualTaskCountMax && currentActiveTaskCount < desiredActiveTaskCount) {
      emitSkipMetric("Already at max task count", desiredActiveTaskCount);
      return -1;
    } else if (currentActiveTaskCount == actualTaskCountMin && currentActiveTaskCount > desiredActiveTaskCount) {
      emitSkipMetric("Already at min task count", desiredActiveTaskCount);
      return -1;
    }

    desiredActiveTaskCount = Math.min(desiredActiveTaskCount, actualTaskCountMax);
    desiredActiveTaskCount = Math.max(desiredActiveTaskCount, actualTaskCountMin);
    return desiredActiveTaskCount;
  }

  private void emitSkipMetric(final String reason, int taskCount)
  {
    emitter.emit(metricBuilder
                     .setDimension(SeekableStreamSupervisor.AUTOSCALER_SKIP_REASON_DIMENSION, reason)
                     .setMetric(SeekableStreamSupervisor.AUTOSCALER_REQUIRED_TASKS_METRIC, taskCount));
  }

  private static int proportionalTaskCountHelper(
      int currentActiveTaskCount,
      long targetAggregate,
      double actualAggregate,
      double tolerance
  )
  {
    final int newActiveTaskCount = (int) Math.ceil(currentActiveTaskCount * actualAggregate / targetAggregate);
    // ignore requests within a specific absolute pct threshold from current task count
    if (Math.abs(((double) newActiveTaskCount / currentActiveTaskCount) - 1.0) > tolerance) {
      log.info("newActiveTaskCount [%d] larger than threshold [%f]", newActiveTaskCount, tolerance);
      return newActiveTaskCount;
    }
    return currentActiveTaskCount;
  }

  public LagBasedAutoScalerConfig getAutoScalerConfig()
  {
    return lagBasedAutoScalerConfig;
  }
}
