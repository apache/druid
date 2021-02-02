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
import org.apache.druid.indexing.overlord.supervisor.Supervisor;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.LagStats;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.SupervisorTaskAutoscaler;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisor;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.emitter.EmittingLogger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class DefaultAutoScaler implements SupervisorTaskAutoscaler
{
  private static final EmittingLogger log = new EmittingLogger(DefaultAutoScaler.class);
  private final String dataSource;
  private final long metricsCollectionIntervalMillis;
  private final long metricsCollectionRangeMillis;
  private final CircularFifoQueue<Long> lagMetricsQueue;
  private final long dynamicCheckStartDelayMillis;
  private final long dynamicCheckPeriod;
  private final long scaleOutThreshold;
  private final long scaleInThreshold;
  private final double triggerScaleOutThresholdFrequency;
  private final double triggerScaleInThresholdFrequency;
  private final int taskCountMax;
  private final int taskCountMin;
  private final int scaleInStep;
  private final int scaleOutStep;
  private final ScheduledExecutorService lagComputationExec;
  private final ScheduledExecutorService allocationExec;
  private final SupervisorSpec spec;
  private final SeekableStreamSupervisor supervisor;

  private static ReentrantLock lock = new ReentrantLock(true);


  public DefaultAutoScaler(Supervisor supervisor, String dataSource, Map<String, Object> autoscalerConfig, SupervisorSpec spec)
  {
    String supervisorId = StringUtils.format("KafkaSupervisor-%s", dataSource);
    this.dataSource = dataSource;
    this.metricsCollectionIntervalMillis = Long.parseLong(String.valueOf(autoscalerConfig.getOrDefault("metricsCollectionIntervalMillis", 30000)));
    this.metricsCollectionRangeMillis = Long.parseLong(String.valueOf(autoscalerConfig.getOrDefault("metricsCollectionRangeMillis", 600000)));
    int slots = (int) (metricsCollectionRangeMillis / metricsCollectionIntervalMillis) + 1;
    log.debug(" The interval of metrics collection is [%s], [%s] timeRange will collect [%s] data points for dataSource [%s].", metricsCollectionIntervalMillis, metricsCollectionRangeMillis, slots, dataSource);
    this.lagMetricsQueue = new CircularFifoQueue<>(slots);
    this.dynamicCheckStartDelayMillis = Long.parseLong(String.valueOf(autoscalerConfig.getOrDefault("dynamicCheckStartDelayMillis", 300000)));
    this.dynamicCheckPeriod = Long.parseLong(String.valueOf(autoscalerConfig.getOrDefault("dynamicCheckPeriod", 60000)));
    this.scaleOutThreshold = Long.parseLong(String.valueOf(autoscalerConfig.getOrDefault("scaleOutThreshold", 6000000)));
    this.scaleInThreshold = Long.parseLong(String.valueOf(autoscalerConfig.getOrDefault("scaleInThreshold", 1000000)));
    this.triggerScaleOutThresholdFrequency = Double.parseDouble(String.valueOf(autoscalerConfig.getOrDefault("triggerScaleOutThresholdFrequency", 0.3)));
    this.triggerScaleInThresholdFrequency = Double.parseDouble(String.valueOf(autoscalerConfig.getOrDefault("triggerScaleInThresholdFrequency", 0.9)));
    this.taskCountMax = Integer.parseInt(String.valueOf(autoscalerConfig.getOrDefault("taskCountMax", SeekableStreamSupervisor.TASK_COUNT_MAX)));
    this.taskCountMin = Integer.parseInt(String.valueOf(autoscalerConfig.getOrDefault("taskCountMin", SeekableStreamSupervisor.TASK_COUNT_MIN)));
    this.scaleInStep = Integer.parseInt(String.valueOf(autoscalerConfig.getOrDefault("scaleInStep", 1)));
    this.scaleOutStep = Integer.parseInt(String.valueOf(autoscalerConfig.getOrDefault("scaleOutStep", 2)));
    this.allocationExec = Execs.scheduledSingleThreaded(StringUtils.encodeForFormat(supervisorId) + "-Allocation-%d");
    this.lagComputationExec = Execs.scheduledSingleThreaded(StringUtils.encodeForFormat(supervisorId) + "-Computation-%d");
    this.spec = spec;
    this.supervisor = (SeekableStreamSupervisor) supervisor;
  }

  @Override
  public void start()
  {
    Callable<Integer> scaleAction = new Callable<Integer>() {
        @Override
        public Integer call()
        {
          lock.lock();
          int desireTaskCount = -1;
          try {
            desireTaskCount = computeDesireTaskCount(new ArrayList<>(lagMetricsQueue));

            if (desireTaskCount != -1) {
              lagMetricsQueue.clear();
            }
          }
          catch (Exception ex) {
            log.warn(ex, "Exception when computeDesireTaskCount [%s]", dataSource);
          }
          finally {
            lock.unlock();
          }
          return desireTaskCount;
        }
    };

    log.info("enableTaskAutoscaler for datasource [%s]", dataSource);
    log.debug("Collect and compute lags at fixed rate of [%s] for dataSource[%s].", metricsCollectionIntervalMillis, dataSource);
    lagComputationExec.scheduleAtFixedRate(
            collectAndComputeLags(),
            dynamicCheckStartDelayMillis, // wait for tasks to start up
            metricsCollectionIntervalMillis,
            TimeUnit.MILLISECONDS
    );
    log.debug("allocate task at fixed rate of [%s], dataSource [%s].", dynamicCheckPeriod, dataSource);
    allocationExec.scheduleAtFixedRate(
            supervisor.buildDynamicAllocationTask(scaleAction),
            dynamicCheckStartDelayMillis + metricsCollectionRangeMillis,
            dynamicCheckPeriod,
            TimeUnit.MILLISECONDS
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
        lock.lock();
        lagMetricsQueue.clear();
      }
      catch (Exception e) {
        log.warn(e, "Error,when clear queue in rest action");
      }
      finally {
        lock.unlock();
      }
    }
  }

    /**
   * This method compute current consume lags. Get the total lags of all partition and fill in lagMetricsQueue
   * @return a Runnbale object to do collect and compute action.
   */
  private Runnable collectAndComputeLags()
  {
    return new Runnable() {
        @Override
        public void run()
        {
          lock.lock();
          try {
            if (!spec.isSuspended()) {
              LagStats lagStats = supervisor.computeLagStats();
              if (lagStats == null) {
                lagMetricsQueue.offer(0L);
              } else {
                long totalLags = lagStats.getTotalLag();
                lagMetricsQueue.offer(totalLags > 0 ? totalLags : 0L);
              }

              log.debug("Current lag metric points [%s] for dataSource [%s].", new ArrayList<>(lagMetricsQueue), dataSource);
            } else {
              log.debug("[%s] supervisor is suspended, skip to collect kafka lags", dataSource);
            }
          }
          catch (Exception e) {
            log.warn(e, "Error, When collect lags");
          }
          finally {
            lock.unlock();
          }
        }
    };
  }

    /**
     * This method determines whether to do scale actions based on collected lag points.
     * Current algorithm of scale is simple:
     *    First of all, compute the proportion of lag points higher/lower than scaleOutThreshold/scaleInThreshold, getting scaleOutThreshold/scaleInThreshold.
     *    Secondly, compare scaleOutThreshold/scaleInThreshold with triggerScaleOutThresholdFrequency/triggerScaleInThresholdFrequency. P.S. Scale out action has higher priority than scale in action.
     *    Finaly, if scaleOutThreshold/scaleInThreshold is higher than triggerScaleOutThresholdFrequency/triggerScaleInThresholdFrequency, scale out/in action would be triggered.
     * @param lags the lag metrics of Stream(Kafka/Kinesis)
     * @return Boolean flag, do scale action successfully or not. If true , it will take at least 'minTriggerDynamicFrequency' before next 'dynamicAllocatie'.
     *         If false, it will do 'dynamicAllocate' again after 'dynamicCheckPeriod'.
     *
     * @return Integer. target number of tasksCount, -1 means skip scale action.
     */
  private Integer computeDesireTaskCount(List<Long> lags)
  {
    // if supervisor is not suspended, ensure required tasks are running
    // if suspended, ensure tasks have been requested to gracefully stop
    log.info("[%s] supervisor is running, start to check dynamic allocate task logic. Current collected lags : [%s]", dataSource, lags);
    int beyond = 0;
    int within = 0;
    int metricsCount = lags.size();
    for (Long lag : lags) {
      if (lag >= scaleOutThreshold) {
        beyond++;
      }
      if (lag <= scaleInThreshold) {
        within++;
      }
    }
    double beyondProportion = beyond * 1.0 / metricsCount;
    double withinProportion = within * 1.0 / metricsCount;
    log.debug("triggerScaleOutThresholdFrequency is [%s] and triggerScaleInThresholdFrequency is [%s] for dataSource [%s].", triggerScaleOutThresholdFrequency, triggerScaleInThresholdFrequency, dataSource);
    log.info("beyondProportion is [%s] and withinProportion is [%s] for dataSource [%s].", beyondProportion, withinProportion, dataSource);

    int currentActiveTaskCount = supervisor.getActiveTaskGroupsCount();
    if (currentActiveTaskCount < 0) {
      log.info("CurrentActiveTaskCount is lower than 0 ??? skip [%s].", dataSource);
      return -1;
    }
    int desireActiveTaskCount;

    if (beyondProportion >= triggerScaleOutThresholdFrequency) {
        // Do Scale out
      int taskCount = currentActiveTaskCount + scaleOutStep;
      if (currentActiveTaskCount == taskCountMax) {
        log.info("CurrentActiveTaskCount reach task count Max limit, skip to scale out tasks for dataSource [%s].", dataSource);
        return -1;
      } else {
        desireActiveTaskCount = Math.min(taskCount, taskCountMax);
      }

      return desireActiveTaskCount;
    }

    if (withinProportion >= triggerScaleInThresholdFrequency) {
      // Do Scale in
      int taskCount = currentActiveTaskCount - scaleInStep;
      if (currentActiveTaskCount == taskCountMin) {
        log.info("CurrentActiveTaskCount reach task count Min limit, skip to scale in tasks for dataSource [%s].", dataSource);
        return -1;
      } else {
        desireActiveTaskCount = Math.max(taskCount, taskCountMin);
      }
      log.debug("Start to scale in tasks, current active task number [%s] and desire task number is [%s] for dataSource [%s].", currentActiveTaskCount, desireActiveTaskCount, dataSource);
      return desireActiveTaskCount;
    }

    return -1;
  }
}
