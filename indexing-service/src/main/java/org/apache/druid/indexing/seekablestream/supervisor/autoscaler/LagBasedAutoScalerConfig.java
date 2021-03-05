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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexing.overlord.supervisor.Supervisor;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.SupervisorTaskAutoScaler;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisor;

import javax.annotation.Nullable;

public class LagBasedAutoScalerConfig implements AutoScalerConfig
{
  private final long lagCollectionIntervalMillis;
  private final long lagCollectionRangeMillis;
  private final long scaleActionStartDelayMillis;
  private final long scaleActionPeriodMillis;
  private final long scaleOutThreshold;
  private final long scaleInThreshold;
  private final double triggerScaleOutFractionThreshold;
  private final double triggerScaleInFractionThreshold;
  private int taskCountMax;
  private int taskCountMin;
  private final int scaleInStep;
  private final int scaleOutStep;
  private final boolean enableTaskAutoScaler;
  private final long minTriggerScaleActionFrequencyMillis;

  @JsonCreator
  public LagBasedAutoScalerConfig(
          @Nullable @JsonProperty("lagCollectionIntervalMillis") Long lagCollectionIntervalMillis,
          @Nullable @JsonProperty("lagCollectionRangeMillis") Long lagCollectionRangeMillis,
          @Nullable @JsonProperty("scaleActionStartDelayMillis") Long scaleActionStartDelayMillis,
          @Nullable @JsonProperty("scaleActionPeriodMillis") Long scaleActionPeriodMillis,
          @Nullable @JsonProperty("scaleOutThreshold") Long scaleOutThreshold,
          @Nullable @JsonProperty("scaleInThreshold") Long scaleInThreshold,
          @Nullable @JsonProperty("triggerScaleOutFractionThreshold") Double triggerScaleOutFractionThreshold,
          @Nullable @JsonProperty("triggerScaleInFractionThreshold") Double triggerScaleInFractionThreshold,
          @JsonProperty("taskCountMax") Integer taskCountMax,
          @JsonProperty("taskCountMin") Integer taskCountMin,
          @Nullable @JsonProperty("scaleInStep") Integer scaleInStep,
          @Nullable @JsonProperty("scaleOutStep") Integer scaleOutStep,
          @Nullable @JsonProperty("enableTaskAutoScaler") Boolean enableTaskAutoScaler,
          @Nullable @JsonProperty("minTriggerScaleActionFrequencyMillis") Long minTriggerScaleActionFrequencyMillis
  )
  {
    this.enableTaskAutoScaler = enableTaskAutoScaler != null ? enableTaskAutoScaler : false;
    this.lagCollectionIntervalMillis = lagCollectionIntervalMillis != null ? lagCollectionIntervalMillis : 30000;
    this.lagCollectionRangeMillis = lagCollectionRangeMillis != null ? lagCollectionRangeMillis : 600000;
    this.scaleActionStartDelayMillis = scaleActionStartDelayMillis != null ? scaleActionStartDelayMillis : 300000;
    this.scaleActionPeriodMillis = scaleActionPeriodMillis != null ? scaleActionPeriodMillis : 60000;
    this.scaleOutThreshold = scaleOutThreshold != null ? scaleOutThreshold : 6000000;
    this.scaleInThreshold = scaleInThreshold != null ? scaleInThreshold : 1000000;
    this.triggerScaleOutFractionThreshold = triggerScaleOutFractionThreshold != null ? triggerScaleOutFractionThreshold : 0.3;
    this.triggerScaleInFractionThreshold = triggerScaleInFractionThreshold != null ? triggerScaleInFractionThreshold : 0.9;

    // Only do taskCountMax and taskCountMin check when autoscaler is enabled. So that users left autoConfig empty{} will not throw any exception and autoscaler is disabled.
    // If autoscaler is disabled, no matter what configs are set, they are not used.
    if (this.enableTaskAutoScaler) {
      if (taskCountMax == null || taskCountMin == null) {
        throw new RuntimeException("taskCountMax or taskCountMin can't be null!");
      } else if (taskCountMax < taskCountMin) {
        throw new RuntimeException("taskCountMax can't lower than taskCountMin!");
      }
      this.taskCountMax = taskCountMax;
      this.taskCountMin = taskCountMin;
    }

    this.scaleInStep = scaleInStep != null ? scaleInStep : 1;
    this.scaleOutStep = scaleOutStep != null ? scaleOutStep : 2;
    this.minTriggerScaleActionFrequencyMillis = minTriggerScaleActionFrequencyMillis
        != null ? minTriggerScaleActionFrequencyMillis : 600000;
  }

  @JsonProperty
  public long getLagCollectionIntervalMillis()
  {
    return lagCollectionIntervalMillis;
  }

  @JsonProperty
  public long getLagCollectionRangeMillis()
  {
    return lagCollectionRangeMillis;
  }

  @JsonProperty
  public long getScaleActionStartDelayMillis()
  {
    return scaleActionStartDelayMillis;
  }

  @JsonProperty
  public long getScaleActionPeriodMillis()
  {
    return scaleActionPeriodMillis;
  }

  @JsonProperty
  public long getScaleOutThreshold()
  {
    return scaleOutThreshold;
  }

  @JsonProperty
  public long getScaleInThreshold()
  {
    return scaleInThreshold;
  }

  @JsonProperty
  public double getTriggerScaleOutFractionThreshold()
  {
    return triggerScaleOutFractionThreshold;
  }

  @JsonProperty
  public double getTriggerScaleInFractionThreshold()
  {
    return triggerScaleInFractionThreshold;
  }

  @Override
  @JsonProperty
  public int getTaskCountMax()
  {
    return taskCountMax;
  }

  @Override
  @JsonProperty
  public int getTaskCountMin()
  {
    return taskCountMin;
  }

  @Override
  public SupervisorTaskAutoScaler createAutoScaler(Supervisor supervisor, SupervisorSpec spec)
  {
    return new LagBasedAutoScaler((SeekableStreamSupervisor) supervisor, spec.getId(), this, spec);
  }

  @JsonProperty
  public int getScaleInStep()
  {
    return scaleInStep;
  }

  @JsonProperty
  public int getScaleOutStep()
  {
    return scaleOutStep;
  }

  @Override
  @JsonProperty
  public boolean getEnableTaskAutoScaler()
  {
    return enableTaskAutoScaler;
  }

  @Override
  @JsonProperty
  public long getMinTriggerScaleActionFrequencyMillis()
  {
    return minTriggerScaleActionFrequencyMillis;
  }

  @Override
  public String toString()
  {
    return "autoScalerConfig{" +
            "enableTaskAutoScaler=" + enableTaskAutoScaler +
            ", taskCountMax=" + taskCountMax +
            ", taskCountMin=" + taskCountMin +
            ", minTriggerScaleActionFrequencyMillis=" + minTriggerScaleActionFrequencyMillis +
            ", lagCollectionIntervalMillis=" + lagCollectionIntervalMillis +
            ", lagCollectionIntervalMillis=" + lagCollectionIntervalMillis +
            ", scaleOutThreshold=" + scaleOutThreshold +
            ", triggerScaleOutFractionThreshold=" + triggerScaleOutFractionThreshold +
            ", scaleInThreshold=" + scaleInThreshold +
            ", triggerScaleInFractionThreshold=" + triggerScaleInFractionThreshold +
            ", scaleActionStartDelayMillis=" + scaleActionStartDelayMillis +
            ", scaleActionPeriodMillis=" + scaleActionPeriodMillis +
            ", scaleInStep=" + scaleInStep +
            ", scaleOutStep=" + scaleOutStep +
            '}';
  }
}
