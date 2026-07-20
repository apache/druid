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
 * Candidate task counts are derived from possible partitions-per-task ratios and are not limited
 * to factors/divisors of the partition count. Optional scale-up and scale-down boundaries control
 * how much of that candidate set is evaluated around the current task count.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CostBasedAutoScalerConfig implements AutoScalerConfig
{
  static final double DEFAULT_LAG_WEIGHT = 0.4;
  static final double DEFAULT_IDLE_WEIGHT = 0.6;
  static final Duration DEFAULT_MIN_SCALE_UP_DELAY = Duration.standardMinutes(10);
  static final Duration DEFAULT_MIN_SCALE_DOWN_DELAY = Duration.standardMinutes(30);

  private final boolean enableTaskAutoScaler;
  private final int taskCountMax;
  private final int taskCountMin;
  private final Integer taskCountStart;
  private final Double stopTaskCountRatio;
  private final long scaleActionPeriodMillis;

  private final double lagWeight;
  private final double idleWeight;
  private final double optimalTaskIdleRatio;
  private final boolean useTaskCountBoundariesOnScaleUp;
  private final boolean useTaskCountBoundariesOnScaleDown;
  private final Duration minScaleUpDelay;
  private final Duration minScaleDownDelay;
  private final boolean scaleDownDuringTaskRolloverOnly;
  private final boolean usePollIdleRatio;
  private final int minCostDropPercentForScaling;

  /**
   * Creates a new CostBasedAutoScalerConfig instance.
   */
  @JsonCreator
  public CostBasedAutoScalerConfig(
      @JsonProperty("taskCountMax") Integer taskCountMax,
      @JsonProperty("taskCountMin") Integer taskCountMin,
      @Nullable @JsonProperty("enableTaskAutoScaler") Boolean enableTaskAutoScaler,
      @Nullable @JsonProperty("taskCountStart") Integer taskCountStart,
      @Nullable @JsonProperty("stopTaskCountRatio") Double stopTaskCountRatio,
      @Nullable @JsonProperty("scaleActionPeriodMillis") Long scaleActionPeriodMillis,
      @Nullable @JsonProperty("lagWeight") Double lagWeight,
      @Nullable @JsonProperty("idleWeight") Double idleWeight,
      @Nullable @JsonProperty("optimalTaskIdleRatio") Double optimalTaskIdleRatio,
      @Nullable @JsonProperty("useTaskCountBoundariesOnScaleUp") Boolean useTaskCountBoundariesOnScaleUp,
      @Nullable @JsonProperty("useTaskCountBoundariesOnScaleDown") Boolean useTaskCountBoundariesOnScaleDown,
      @Nullable @JsonProperty("minScaleUpDelay") Duration minScaleUpDelay,
      @Nullable @JsonProperty("minScaleDownDelay") Duration minScaleDownDelay,
      @Nullable @JsonProperty("scaleDownDuringTaskRolloverOnly") Boolean scaleDownDuringTaskRolloverOnly,
      @Nullable @JsonProperty("usePollIdleRatio") Boolean usePollIdleRatio,
      @Nullable @JsonProperty("minCostDropPercentForScaling") Integer minCostDropPercentForScaling
  )
  {
    this.enableTaskAutoScaler = Configs.valueOrDefault(enableTaskAutoScaler, false);
    this.scaleActionPeriodMillis = Configs.valueOrDefault(scaleActionPeriodMillis, DEFAULT_MIN_SCALE_UP_DELAY.getMillis());

    this.lagWeight = Configs.valueOrDefault(lagWeight, DEFAULT_LAG_WEIGHT);
    this.idleWeight = Configs.valueOrDefault(idleWeight, DEFAULT_IDLE_WEIGHT);
    this.optimalTaskIdleRatio = Configs.valueOrDefault(
        optimalTaskIdleRatio,
        WeightedCostFunction.OPTIMAL_TASK_IDLE_RATIO
    );
    this.useTaskCountBoundariesOnScaleUp = Configs.valueOrDefault(useTaskCountBoundariesOnScaleUp, false);
    this.useTaskCountBoundariesOnScaleDown = Configs.valueOrDefault(useTaskCountBoundariesOnScaleDown, true);
    this.minScaleUpDelay = Configs.valueOrDefault(minScaleUpDelay, DEFAULT_MIN_SCALE_UP_DELAY);
    this.minScaleDownDelay = Configs.valueOrDefault(minScaleDownDelay, DEFAULT_MIN_SCALE_DOWN_DELAY);
    this.scaleDownDuringTaskRolloverOnly = Configs.valueOrDefault(scaleDownDuringTaskRolloverOnly, false);
    this.usePollIdleRatio = Configs.valueOrDefault(usePollIdleRatio, true);
    this.minCostDropPercentForScaling = Configs.valueOrDefault(minCostDropPercentForScaling, 0);

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
    Preconditions.checkArgument(
        this.optimalTaskIdleRatio > 0.0 && this.optimalTaskIdleRatio < 1.0,
        "optimalTaskIdleRatio must be > 0 and < 1"
    );
    Preconditions.checkArgument(
        this.minScaleUpDelay.getMillis() >= 0,
        "minScaleUpDelay must be a duration >= 0 millis"
    );
    Preconditions.checkArgument(
        this.minScaleDownDelay.getMillis() >= 0,
        "minScaleDownDelay must be a duration >= 0 millis"
    );
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

  @Deprecated
  @Override
  @JsonProperty
  public long getMinTriggerScaleActionFrequencyMillis()
  {
    return -1;
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

  /**
   * Target idle ratio representing the optimal operating point for the U-shaped idle cost
   * in {@link WeightedCostFunction}.
   */
  @JsonProperty
  public double getOptimalTaskIdleRatio()
  {
    return optimalTaskIdleRatio;
  }

  /**
   * If true, the auto-scaler evaluates a small number of candidate task counts
   * (dictated by {@link CostBasedAutoScaler#BOUNDARY_LIMIT_IN_PARTITIONS_PER_TASK})
   * above the current count, which prevents large scale-up jumps.
   */
  @JsonProperty
  public boolean isUseTaskCountBoundariesOnScaleUp()
  {
    return useTaskCountBoundariesOnScaleUp;
  }

  /**
   * If true, the auto-scaler evaluates a small number of candidate task counts
   * (dictated by {@link CostBasedAutoScaler#BOUNDARY_LIMIT_IN_PARTITIONS_PER_TASK})
   * below the current count, which prevents large scale-down drops.
   */
  @JsonProperty
  public boolean isUseTaskCountBoundariesOnScaleDown()
  {
    return useTaskCountBoundariesOnScaleDown;
  }

  /**
   * Returns the minimum delay before a scale-up action is allowed after any previous scale action.
   */
  @Override
  @JsonProperty
  public Duration getMinScaleUpDelay()
  {
    return minScaleUpDelay;
  }

  /**
   * Returns the minimum delay before a scale-down action is allowed after any previous scale action.
   */
  @Override
  @JsonProperty
  public Duration getMinScaleDownDelay()
  {
    return minScaleDownDelay;
  }

  /**
   * Indicates whether scale-down actions are deferred to task rollover.
   * If set to {@code false}, scale-down can happen during regular scale-action checks.
   */
  @JsonProperty("scaleDownDuringTaskRolloverOnly")
  public boolean isScaleDownOnTaskRolloverOnly()
  {
    return scaleDownDuringTaskRolloverOnly;
  }

  /**
   * If true (the default), idle cost is derived from the poll idle ratio reported by tasks.
   * If false, idle cost is derived from processing-rate utilization instead.
   */
  @JsonProperty
  public boolean isUsePollIdleRatio()
  {
    return usePollIdleRatio;
  }

  /**
   * Minimum percentage drop from current cost that is required by the auto-scaler
   * to choose a new task count.
   */
  @JsonProperty
  public int getMinCostDropPercentForScaling()
  {
    return minCostDropPercentForScaling;
  }

  @Override
  public SupervisorTaskAutoScaler createAutoScaler(Supervisor supervisor, SupervisorSpec spec, ServiceEmitter emitter)
  {
    return new CostBasedAutoScaler((SeekableStreamSupervisor<?, ?, ?>) supervisor, this, spec, emitter);
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
           && scaleActionPeriodMillis == that.scaleActionPeriodMillis
           && Double.compare(that.lagWeight, lagWeight) == 0
           && Double.compare(that.idleWeight, idleWeight) == 0
           && Double.compare(that.optimalTaskIdleRatio, optimalTaskIdleRatio) == 0
           && useTaskCountBoundariesOnScaleUp == that.useTaskCountBoundariesOnScaleUp
           && useTaskCountBoundariesOnScaleDown == that.useTaskCountBoundariesOnScaleDown
           && Objects.equals(minScaleUpDelay, that.minScaleUpDelay)
           && Objects.equals(minScaleDownDelay, that.minScaleDownDelay)
           && scaleDownDuringTaskRolloverOnly == that.scaleDownDuringTaskRolloverOnly
           && usePollIdleRatio == that.usePollIdleRatio
           && minCostDropPercentForScaling == that.minCostDropPercentForScaling
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
        stopTaskCountRatio,
        scaleActionPeriodMillis,
        lagWeight,
        idleWeight,
        optimalTaskIdleRatio,
        useTaskCountBoundariesOnScaleUp,
        useTaskCountBoundariesOnScaleDown,
        minScaleUpDelay,
        minScaleDownDelay,
        scaleDownDuringTaskRolloverOnly,
        usePollIdleRatio,
        minCostDropPercentForScaling
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
           ", stopTaskCountRatio=" + stopTaskCountRatio +
           ", scaleActionPeriodMillis=" + scaleActionPeriodMillis +
           ", lagWeight=" + lagWeight +
           ", idleWeight=" + idleWeight +
           ", optimalTaskIdleRatio=" + optimalTaskIdleRatio +
           ", useTaskCountBoundariesOnScaleUp=" + useTaskCountBoundariesOnScaleUp +
           ", useTaskCountBoundariesOnScaleDown=" + useTaskCountBoundariesOnScaleDown +
           ", minScaleUpDelay=" + minScaleUpDelay +
           ", minScaleDownDelay=" + minScaleDownDelay +
           ", scaleDownDuringTaskRolloverOnly=" + scaleDownDuringTaskRolloverOnly +
           ", usePollIdleRatio=" + usePollIdleRatio +
           ", minCostDropPercentForScaling=" + minCostDropPercentForScaling +
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
    private Double stopTaskCountRatio;
    private Long scaleActionPeriodMillis;
    private Double lagWeight;
    private Double idleWeight;
    private Double optimalTaskIdleRatio;
    private Boolean useTaskCountBoundariesOnScaleUp;
    private Boolean useTaskCountBoundariesOnScaleDown;
    private Duration minScaleUpDelay;
    private Duration minScaleDownDelay;
    private Boolean scaleDownDuringTaskRolloverOnly;
    private Boolean usePollIdleRatio;
    private Integer minCostDropPercentForScaling;

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

    public Builder optimalTaskIdleRatio(double optimalTaskIdleRatio)
    {
      this.optimalTaskIdleRatio = optimalTaskIdleRatio;
      return this;
    }

    public Builder minScaleUpDelay(Duration minScaleUpDelay)
    {
      this.minScaleUpDelay = minScaleUpDelay;
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

    public Builder usePollIdleRatio(boolean usePollIdleRatio)
    {
      this.usePollIdleRatio = usePollIdleRatio;
      return this;
    }

    public Builder useTaskCountBoundariesOnScaleUp(boolean useTaskCountBoundariesOnScaleUp)
    {
      this.useTaskCountBoundariesOnScaleUp = useTaskCountBoundariesOnScaleUp;
      return this;
    }

    public Builder useTaskCountBoundariesOnScaleDown(boolean useTaskCountBoundariesOnScaleDown)
    {
      this.useTaskCountBoundariesOnScaleDown = useTaskCountBoundariesOnScaleDown;
      return this;
    }

    public Builder minCostDropPercentForScaling(int minCostDropPercentForScaling)
    {
      this.minCostDropPercentForScaling = minCostDropPercentForScaling;
      return this;
    }

    public CostBasedAutoScalerConfig build()
    {
      return new CostBasedAutoScalerConfig(
          taskCountMax,
          taskCountMin,
          enableTaskAutoScaler,
          taskCountStart,
          stopTaskCountRatio,
          scaleActionPeriodMillis,
          lagWeight,
          idleWeight,
          optimalTaskIdleRatio,
          useTaskCountBoundariesOnScaleUp,
          useTaskCountBoundariesOnScaleDown,
          minScaleUpDelay,
          minScaleDownDelay,
          scaleDownDuringTaskRolloverOnly,
          usePollIdleRatio,
          minCostDropPercentForScaling
      );
    }
  }
}
