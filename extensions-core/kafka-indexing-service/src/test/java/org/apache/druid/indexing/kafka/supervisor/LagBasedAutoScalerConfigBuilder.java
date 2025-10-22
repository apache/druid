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

package org.apache.druid.indexing.kafka.supervisor;

import org.apache.druid.indexing.overlord.supervisor.autoscaler.AggregateFunction;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.LagBasedAutoScalerConfig;

import javax.annotation.Nullable;

/**
 * Builder for {@link LagBasedAutoScalerConfig} used in tests.
 */
public class LagBasedAutoScalerConfigBuilder
{
  private long lagCollectionIntervalMillis;
  private long lagCollectionRangeMillis;
  private long scaleActionStartDelayMillis;
  private long scaleActionPeriodMillis;
  private long scaleOutThreshold;
  private long scaleInThreshold;
  private double triggerScaleOutFractionThreshold;
  private double triggerScaleInFractionThreshold;
  private int taskCountMax;
  private int taskCountMin;
  private Integer taskCountStart;
  private int scaleInStep;
  private int scaleOutStep;
  private boolean enableTaskAutoScaler;
  private long minTriggerScaleActionFrequencyMillis;
  private AggregateFunction lagAggregate;
  private double stopTaskCountRatio;

  public LagBasedAutoScalerConfigBuilder withLagCollectionIntervalMillis(long lagCollectionIntervalMillis)
  {
    this.lagCollectionIntervalMillis = lagCollectionIntervalMillis;
    return this;
  }

  public LagBasedAutoScalerConfigBuilder withLagCollectionRangeMillis(long lagCollectionRangeMillis)
  {
    this.lagCollectionRangeMillis = lagCollectionRangeMillis;
    return this;
  }

  public LagBasedAutoScalerConfigBuilder withScaleActionStartDelayMillis(long scaleActionStartDelayMillis)
  {
    this.scaleActionStartDelayMillis = scaleActionStartDelayMillis;
    return this;
  }

  public LagBasedAutoScalerConfigBuilder withScaleActionPeriodMillis(long scaleActionPeriodMillis)
  {
    this.scaleActionPeriodMillis = scaleActionPeriodMillis;
    return this;
  }

  public LagBasedAutoScalerConfigBuilder withScaleOutThreshold(long scaleOutThreshold)
  {
    this.scaleOutThreshold = scaleOutThreshold;
    return this;
  }

  public LagBasedAutoScalerConfigBuilder withScaleInThreshold(long scaleInThreshold)
  {
    this.scaleInThreshold = scaleInThreshold;
    return this;
  }

  public LagBasedAutoScalerConfigBuilder withTriggerScaleOutFractionThreshold(double triggerScaleOutFractionThreshold)
  {
    this.triggerScaleOutFractionThreshold = triggerScaleOutFractionThreshold;
    return this;
  }

  public LagBasedAutoScalerConfigBuilder withTriggerScaleInFractionThreshold(double triggerScaleInFractionThreshold)
  {
    this.triggerScaleInFractionThreshold = triggerScaleInFractionThreshold;
    return this;
  }

  public LagBasedAutoScalerConfigBuilder withTaskCountMax(int taskCountMax)
  {
    this.taskCountMax = taskCountMax;
    return this;
  }

  public LagBasedAutoScalerConfigBuilder withTaskCountMin(int taskCountMin)
  {
    this.taskCountMin = taskCountMin;
    return this;
  }

  public LagBasedAutoScalerConfigBuilder withTaskCountStart(@Nullable Integer taskCountStart)
  {
    this.taskCountStart = taskCountStart;
    return this;
  }

  public LagBasedAutoScalerConfigBuilder withScaleInStep(int scaleInStep)
  {
    this.scaleInStep = scaleInStep;
    return this;
  }

  public LagBasedAutoScalerConfigBuilder withScaleOutStep(int scaleOutStep)
  {
    this.scaleOutStep = scaleOutStep;
    return this;
  }

  public LagBasedAutoScalerConfigBuilder withEnableTaskAutoScaler(boolean enableTaskAutoScaler)
  {
    this.enableTaskAutoScaler = enableTaskAutoScaler;
    return this;
  }

  public LagBasedAutoScalerConfigBuilder withMinTriggerScaleActionFrequencyMillis(long minTriggerScaleActionFrequencyMillis)
  {
    this.minTriggerScaleActionFrequencyMillis = minTriggerScaleActionFrequencyMillis;
    return this;
  }

  public LagBasedAutoScalerConfigBuilder withLagAggregate(AggregateFunction lagAggregate)
  {
    this.lagAggregate = lagAggregate;
    return this;
  }

  public LagBasedAutoScalerConfigBuilder withStopTaskCountRatio(double stopTaskCountRatio)
  {
    this.stopTaskCountRatio = stopTaskCountRatio;
    return this;
  }

  public LagBasedAutoScalerConfig build()
  {
    return new LagBasedAutoScalerConfig(
        lagCollectionIntervalMillis,
        lagCollectionRangeMillis,
        scaleActionStartDelayMillis,
        scaleActionPeriodMillis,
        scaleOutThreshold,
        scaleInThreshold,
        triggerScaleOutFractionThreshold,
        triggerScaleInFractionThreshold,
        taskCountMax,
        taskCountStart,
        taskCountMin,
        scaleInStep,
        scaleOutStep,
        enableTaskAutoScaler,
        minTriggerScaleActionFrequencyMillis,
        lagAggregate,
        stopTaskCountRatio
    );
  }
}
