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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.common.config.Configs;
import org.apache.druid.indexing.overlord.supervisor.Supervisor;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.SupervisorTaskAutoScaler;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisor;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Configuration for cost-based auto-scaling of seekable stream supervisor tasks.
 * Uses a cost function combining lag and idle time metrics to determine optimal task counts.
 * Task counts are selected from a bounded range derived from the current partitions-per-task (PPT)
 * ratio, not strictly from factors/divisors of the partition count. This bounded PPT window enables
 * gradual scaling while avoiding large jumps and still allowing non-divisor task counts when needed.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CostBasedAutoScalerConfig implements AutoScalerConfig
{
  static final long DEFAULT_SCALE_ACTION_PERIOD_MILLIS = 10 * 60 * 1000; // 10 minutes
  static final long DEFAULT_MIN_TRIGGER_SCALE_ACTION_FREQUENCY_MILLIS = 5 * 60 * 1000; // 5 minutes
  static final double DEFAULT_LAG_WEIGHT = 0.25;
  static final double DEFAULT_IDLE_WEIGHT = 0.75;
  static final double DEFAULT_PROCESSING_RATE = 1000.0; // 1000 records/sec per task as default
  static final Duration DEFAULT_MIN_SCALE_DELAY = Duration.millis(DEFAULT_SCALE_ACTION_PERIOD_MILLIS * 3);

  private final boolean enableTaskAutoScaler;
  private final int taskCountMax;
  private final int taskCountMin;
  private final Integer taskCountStart;
  private final long minTriggerScaleActionFrequencyMillis;
  private final Double stopTaskCountRatio;
  private final long scaleActionPeriodMillis;

  private final double lagWeight;
  private final double idleWeight;
  private final double defaultProcessingRate;
  /**
   * Per-partition lag threshold allowing to activate a burst scaleup to eliminate high lag.
   */
  private final int highLagThreshold;
  /**
   * Represents the minimum duration between successful scale actions.
   * A higher value implies a more conservative scaling behavior, ensuring that tasks
   * are not scaled too frequently during workload fluctuations.
   */
  private final Duration minScaleDownDelay;
  /**
   * Indicates whether task scaling down is limited to periods during task rollovers only.
   * If set to {@code false}, allows scaling down during normal task run time.
   */
  private final boolean scaleDownDuringTaskRolloverOnly;

  @JsonCreator
  public CostBasedAutoScalerConfig(
      @JsonProperty("taskCountMax") Integer taskCountMax,
      @JsonProperty("taskCountMin") Integer taskCountMin,
      @Nullable @JsonProperty("enableTaskAutoScaler") Boolean enableTaskAutoScaler,
      @Nullable @JsonProperty("taskCountStart") Integer taskCountStart,
      @Nullable @JsonProperty("minTriggerScaleActionFrequencyMillis") Long minTriggerScaleActionFrequencyMillis,
      @Nullable @JsonProperty("stopTaskCountRatio") Double stopTaskCountRatio,
      @Nullable @JsonProperty("scaleActionPeriodMillis") Long scaleActionPeriodMillis,
      @Nullable @JsonProperty("lagWeight") Double lagWeight,
      @Nullable @JsonProperty("idleWeight") Double idleWeight,
      @Nullable @JsonProperty("defaultProcessingRate") Double defaultProcessingRate,
      @Nullable @JsonProperty("highLagThreshold") Integer highLagThreshold,
      @Nullable @JsonProperty("minScaleDownDelay") Duration minScaleDownDelay,
      @Nullable @JsonProperty("scaleDownDuringTaskRolloverOnly") Boolean scaleDownDuringTaskRolloverOnly
  )
  {
    this.enableTaskAutoScaler = enableTaskAutoScaler != null ? enableTaskAutoScaler : false;

    // Timing configuration with defaults
    this.scaleActionPeriodMillis = scaleActionPeriodMillis != null
                                   ? scaleActionPeriodMillis
                                   : DEFAULT_SCALE_ACTION_PERIOD_MILLIS;
    this.minTriggerScaleActionFrequencyMillis = Configs.valueOrDefault(
        minTriggerScaleActionFrequencyMillis,
        DEFAULT_MIN_TRIGGER_SCALE_ACTION_FREQUENCY_MILLIS
    );

    // Cost function weights with defaults
    this.lagWeight = Configs.valueOrDefault(lagWeight, DEFAULT_LAG_WEIGHT);
    this.idleWeight = Configs.valueOrDefault(idleWeight, DEFAULT_IDLE_WEIGHT);
    this.defaultProcessingRate = Configs.valueOrDefault(defaultProcessingRate, DEFAULT_PROCESSING_RATE);
    this.highLagThreshold = Configs.valueOrDefault(
        highLagThreshold,
        CostBasedAutoScaler.EXTRA_SCALING_LAG_PER_PARTITION_THRESHOLD
    );
    this.minScaleDownDelay = Configs.valueOrDefault(minScaleDownDelay, DEFAULT_MIN_SCALE_DELAY);
    this.scaleDownDuringTaskRolloverOnly = Configs.valueOrDefault(scaleDownDuringTaskRolloverOnly, false);

    if (this.enableTaskAutoScaler) {
      Preconditions.checkNotNull(taskCountMax, "taskCountMax is required when enableTaskAutoScaler is true");
      Preconditions.checkNotNull(taskCountMin, "taskCountMin is required when enableTaskAutoScaler is true");
      Preconditions.checkArgument(taskCountMax >= taskCountMin, "taskCountMax must be >= taskCountMin");
      Preconditions.checkArgument(
          taskCountStart == null || (taskCountStart >= taskCountMin && taskCountStart <= taskCountMax),
          "taskCountMin <= taskCountStart <= taskCountMax"
      );
      this.taskCountMax = taskCountMax;
      this.taskCountMin = taskCountMin;
    } else {
      this.taskCountMax = Configs.valueOrDefault(taskCountMax, 0);
      this.taskCountMin = Configs.valueOrDefault(taskCountMin, 0);
    }
    this.taskCountStart = taskCountStart;

    Preconditions.checkArgument(
        stopTaskCountRatio == null || (stopTaskCountRatio > 0.0 && stopTaskCountRatio <= 1.0),
        "0.0 < stopTaskCountRatio <= 1.0"
    );
    this.stopTaskCountRatio = stopTaskCountRatio;

    Preconditions.checkArgument(this.lagWeight >= 0, "lagWeight must be >= 0");
    Preconditions.checkArgument(this.idleWeight >= 0, "idleWeight must be >= 0");
    Preconditions.checkArgument(this.defaultProcessingRate > 0, "defaultProcessingRate must be > 0");
    Preconditions.checkArgument(this.minScaleDownDelay.getMillis() >= 0, "minScaleDownDelay must be >= 0");
  }

  /**
   * Creates a new Builder for constructing CostBasedAutoScalerConfig instances.
   */
  public static Builder builder()
  {
    return new Builder();
  }

  @Override
  @JsonProperty
  public boolean getEnableTaskAutoScaler()
  {
    return enableTaskAutoScaler;
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
  @JsonProperty
  @Nullable
  public Integer getTaskCountStart()
  {
    return taskCountStart;
  }

  @Override
  @JsonProperty
  public long getMinTriggerScaleActionFrequencyMillis()
  {
    return minTriggerScaleActionFrequencyMillis;
  }

  @Override
  @JsonProperty
  @Nullable
  public Double getStopTaskCountRatio()
  {
    return stopTaskCountRatio;
  }

  @JsonProperty
  public long getScaleActionPeriodMillis()
  {
    return scaleActionPeriodMillis;
  }

  @JsonProperty
  public double getLagWeight()
  {
    return lagWeight;
  }

  @JsonProperty
  public double getIdleWeight()
  {
    return idleWeight;
  }

  @JsonProperty
  public double getDefaultProcessingRate()
  {
    return defaultProcessingRate;
  }

  @JsonProperty
  public Duration getMinScaleDownDelay()
  {
    return minScaleDownDelay;
  }

  @JsonProperty("scaleDownDuringTaskRolloverOnly")
  public boolean isScaleDownOnTaskRolloverOnly()
  {
    return scaleDownDuringTaskRolloverOnly;
  }

  @JsonProperty("highLagThreshold")
  public int getHighLagThreshold()
  {
    return highLagThreshold;
  }

  @Override
  public SupervisorTaskAutoScaler createAutoScaler(Supervisor supervisor, SupervisorSpec spec, ServiceEmitter emitter)
  {
    return new CostBasedAutoScaler((SeekableStreamSupervisor) supervisor, this, spec, emitter);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    CostBasedAutoScalerConfig that = (CostBasedAutoScalerConfig) o;

    return enableTaskAutoScaler == that.enableTaskAutoScaler
           && taskCountMax == that.taskCountMax
           && taskCountMin == that.taskCountMin
           && minTriggerScaleActionFrequencyMillis == that.minTriggerScaleActionFrequencyMillis
           && scaleActionPeriodMillis == that.scaleActionPeriodMillis
           && Double.compare(that.lagWeight, lagWeight) == 0
           && Double.compare(that.idleWeight, idleWeight) == 0
           && Double.compare(that.defaultProcessingRate, defaultProcessingRate) == 0
           && Objects.equals(minScaleDownDelay, that.minScaleDownDelay)
           && scaleDownDuringTaskRolloverOnly == that.scaleDownDuringTaskRolloverOnly
           && Objects.equals(taskCountStart, that.taskCountStart)
           && Objects.equals(stopTaskCountRatio, that.stopTaskCountRatio);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        enableTaskAutoScaler,
        taskCountMax,
        taskCountMin,
        taskCountStart,
        minTriggerScaleActionFrequencyMillis,
        stopTaskCountRatio,
        scaleActionPeriodMillis,
        lagWeight,
        idleWeight,
        defaultProcessingRate,
        minScaleDownDelay,
        scaleDownDuringTaskRolloverOnly
    );
  }

  @Override
  public String toString()
  {
    return "CostBasedAutoScalerConfig{" +
           "enableTaskAutoScaler=" + enableTaskAutoScaler +
           ", taskCountMax=" + taskCountMax +
           ", taskCountMin=" + taskCountMin +
           ", taskCountStart=" + taskCountStart +
           ", minTriggerScaleActionFrequencyMillis=" + minTriggerScaleActionFrequencyMillis +
           ", stopTaskCountRatio=" + stopTaskCountRatio +
           ", scaleActionPeriodMillis=" + scaleActionPeriodMillis +
           ", lagWeight=" + lagWeight +
           ", idleWeight=" + idleWeight +
           ", defaultProcessingRate=" + defaultProcessingRate +
           ", highLagThreshold=" + highLagThreshold +
           ", minScaleDownDelay=" + minScaleDownDelay +
           ", scaleDownDuringTaskRolloverOnly=" + scaleDownDuringTaskRolloverOnly +
           '}';
  }

  /**
   * Builder for CostBasedAutoScalerConfig.
   * Provides a fluent API for constructing configuration instances.
   */
  public static class Builder
  {
    private Integer taskCountMax;
    private Integer taskCountMin;
    private Boolean enableTaskAutoScaler = true;
    private Integer taskCountStart;
    private Long minTriggerScaleActionFrequencyMillis;
    private Double stopTaskCountRatio;
    private Long scaleActionPeriodMillis;
    private Double lagWeight;
    private Double idleWeight;
    private Double defaultProcessingRate;
    private Integer highLagThreshold;
    private Duration minScaleDownDelay;
    private Boolean scaleDownDuringTaskRolloverOnly;

    private Builder()
    {
    }

    public Builder taskCountMax(int taskCountMax)
    {
      this.taskCountMax = taskCountMax;
      return this;
    }

    public Builder taskCountMin(int taskCountMin)
    {
      this.taskCountMin = taskCountMin;
      return this;
    }

    public Builder enableTaskAutoScaler(boolean enableTaskAutoScaler)
    {
      this.enableTaskAutoScaler = enableTaskAutoScaler;
      return this;
    }

    public Builder taskCountStart(Integer taskCountStart)
    {
      this.taskCountStart = taskCountStart;
      return this;
    }

    public Builder minTriggerScaleActionFrequencyMillis(long minTriggerScaleActionFrequencyMillis)
    {
      this.minTriggerScaleActionFrequencyMillis = minTriggerScaleActionFrequencyMillis;
      return this;
    }

    public Builder stopTaskCountRatio(Double stopTaskCountRatio)
    {
      this.stopTaskCountRatio = stopTaskCountRatio;
      return this;
    }

    public Builder scaleActionPeriodMillis(long scaleActionPeriodMillis)
    {
      this.scaleActionPeriodMillis = scaleActionPeriodMillis;
      return this;
    }

    public Builder lagWeight(double lagWeight)
    {
      this.lagWeight = lagWeight;
      return this;
    }

    public Builder idleWeight(double idleWeight)
    {
      this.idleWeight = idleWeight;
      return this;
    }

    public Builder defaultProcessingRate(double defaultProcessingRate)
    {
      this.defaultProcessingRate = defaultProcessingRate;
      return this;
    }

    public Builder minScaleDownDelay(Duration minScaleDownDelay)
    {
      this.minScaleDownDelay = minScaleDownDelay;
      return this;
    }

    public Builder scaleDownDuringTaskRolloverOnly(boolean scaleDownDuringTaskRolloverOnly)
    {
      this.scaleDownDuringTaskRolloverOnly = scaleDownDuringTaskRolloverOnly;
      return this;
    }

    public Builder highLagThreshold(int highLagThreshold)
    {
      this.highLagThreshold = highLagThreshold;
      return this;
    }

    public CostBasedAutoScalerConfig build()
    {
      return new CostBasedAutoScalerConfig(
          taskCountMax,
          taskCountMin,
          enableTaskAutoScaler,
          taskCountStart,
          minTriggerScaleActionFrequencyMillis,
          stopTaskCountRatio,
          scaleActionPeriodMillis,
          lagWeight,
          idleWeight,
          defaultProcessingRate,
          highLagThreshold,
          minScaleDownDelay,
          scaleDownDuringTaskRolloverOnly
      );
    }
  }
}
