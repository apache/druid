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
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class DefaultAutoScaler implements SupervisorTaskAutoscaler
{
  private static final EmittingLogger log = new EmittingLogger(DefaultAutoScaler.class);
  private final String dataSource;
  private final CircularFifoQueue<Long> lagMetricsQueue;
  private final ScheduledExecutorService lagComputationExec;
  private final ScheduledExecutorService allocationExec;
  private final SupervisorSpec spec;
  private final SeekableStreamSupervisor supervisor;
  private final DefaultAutoScalerConfig defaultAutoScalerConfig;

  private static ReentrantLock lock = new ReentrantLock(true);


  public DefaultAutoScaler(Supervisor supervisor, String dataSource, AutoScalerConfig autoScalerConfig, SupervisorSpec spec)
  {
    this.defaultAutoScalerConfig = (DefaultAutoScalerConfig) autoScalerConfig;
    String supervisorId = StringUtils.format("KafkaSupervisor-%s", dataSource);
    this.dataSource = dataSource;
    int slots = (int) (defaultAutoScalerConfig.getMetricsCollectionRangeMillis() / defaultAutoScalerConfig.getMetricsCollectionIntervalMillis()) + 1;
    log.debug(" The interval of metrics collection is [%s], [%s] timeRange will collect [%s] data points for dataSource [%s].", defaultAutoScalerConfig.getMetricsCollectionIntervalMillis(), defaultAutoScalerConfig.getMetricsCollectionRangeMillis(), slots, dataSource);
    this.lagMetricsQueue = new CircularFifoQueue<>(slots);
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
    log.debug("Collect and compute lags at fixed rate of [%s] for dataSource[%s].", defaultAutoScalerConfig.getMetricsCollectionIntervalMillis(), dataSource);
    lagComputationExec.scheduleAtFixedRate(
            collectAndComputeLags(),
            defaultAutoScalerConfig.getDynamicCheckStartDelayMillis(), // wait for tasks to start up
            defaultAutoScalerConfig.getMetricsCollectionIntervalMillis(),
            TimeUnit.MILLISECONDS
    );
    log.debug("allocate task at fixed rate of [%s], dataSource [%s].", defaultAutoScalerConfig.getDynamicCheckPeriod(), dataSource);
    allocationExec.scheduleAtFixedRate(
            supervisor.buildDynamicAllocationTask(scaleAction),
            defaultAutoScalerConfig.getDynamicCheckStartDelayMillis() + defaultAutoScalerConfig.getMetricsCollectionRangeMillis(),
            defaultAutoScalerConfig.getDynamicCheckPeriod(),
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
      if (lag >= defaultAutoScalerConfig.getScaleOutThreshold()) {
        beyond++;
      }
      if (lag <= defaultAutoScalerConfig.getScaleInThreshold()) {
        within++;
      }
    }
    double beyondProportion = beyond * 1.0 / metricsCount;
    double withinProportion = within * 1.0 / metricsCount;
    log.debug("triggerScaleOutThresholdFrequency is [%s] and triggerScaleInThresholdFrequency is [%s] for dataSource [%s].", defaultAutoScalerConfig.getTriggerScaleOutThresholdFrequency(), defaultAutoScalerConfig.getTriggerScaleInThresholdFrequency(), dataSource);
    log.info("beyondProportion is [%s] and withinProportion is [%s] for dataSource [%s].", beyondProportion, withinProportion, dataSource);

    int currentActiveTaskCount = supervisor.getActiveTaskGroupsCount();
    if (currentActiveTaskCount < 0) {
      log.info("CurrentActiveTaskCount is lower than 0 ??? skip [%s].", dataSource);
      return -1;
    }
    int desiredActiveTaskCount;

    if (beyondProportion >= defaultAutoScalerConfig.getTriggerScaleOutThresholdFrequency()) {
        // Do Scale out
      int taskCount = currentActiveTaskCount + defaultAutoScalerConfig.getScaleOutStep();
      if (currentActiveTaskCount == defaultAutoScalerConfig.getTaskCountMax()) {
        log.info("CurrentActiveTaskCount reach task count Max limit, skip to scale out tasks for dataSource [%s].", dataSource);
        return -1;
      } else {
        desiredActiveTaskCount = Math.min(taskCount, defaultAutoScalerConfig.getTaskCountMax());
      }

      return desiredActiveTaskCount;
    }

    if (withinProportion >= defaultAutoScalerConfig.getTriggerScaleInThresholdFrequency()) {
      // Do Scale in
      int taskCount = currentActiveTaskCount - defaultAutoScalerConfig.getScaleInStep();
      if (currentActiveTaskCount == defaultAutoScalerConfig.getTaskCountMin()) {
        log.info("CurrentActiveTaskCount reach task count Min limit, skip to scale in tasks for dataSource [%s].", dataSource);
        return -1;
      } else {
        desiredActiveTaskCount = Math.max(taskCount, defaultAutoScalerConfig.getTaskCountMin());
      }
      log.debug("Start to scale in tasks, current active task number [%s] and desire task number is [%s] for dataSource [%s].", currentActiveTaskCount, desiredActiveTaskCount, dataSource);
      return desiredActiveTaskCount;
    }

    return -1;
  }

  public DefaultAutoScalerConfig getAutoScalerConfig()
  {
    return defaultAutoScalerConfig;
  }
}
