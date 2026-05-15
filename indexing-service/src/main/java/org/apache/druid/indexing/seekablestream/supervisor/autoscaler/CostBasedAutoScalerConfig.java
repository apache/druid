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
import org.apache.druid.java.util.emitter.EmittingLogger;
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
  private static final EmittingLogger LOG = new EmittingLogger(CostBasedAutoScalerConfig.class);

  static final long DEFAULT_SCALE_ACTION_PERIOD_MILLIS = 10 * 60 * 1000; // 10 minutes
  static final double DEFAULT_LAG_WEIGHT = 0.4;
  static final double DEFAULT_IDLE_WEIGHT = 0.6;
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
  private final boolean useTaskCountBoundariesOnScaleUp;
  private final boolean useTaskCountBoundariesOnScaleDown;
  private final Duration minScaleUpDelay;
  private final Duration minScaleDownDelay;
  private final boolean scaleDownDuringTaskRolloverOnly;

  /**
   * Creates a new CostBasedAutoScalerConfig instance.
   * <p>
   * Note: useTaskCountBoundaries and highLagThreshold are kept for backward compatibility,
   * but effectively they are removed.
   */
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
      @Nullable @JsonProperty("useTaskCountBoundaries") Boolean useTaskCountBoundaries,
      @Nullable @JsonProperty("highLagThreshold") Integer highLagThreshold,
      @Nullable @JsonProperty("useTaskCountBoundariesOnScaleUp") Boolean useTaskCountBoundariesOnScaleUp,
      @Nullable @JsonProperty("useTaskCountBoundariesOnScaleDown") Boolean useTaskCountBoundariesOnScaleDown,
      @Nullable @JsonProperty("minScaleUpDelay") Duration minScaleUpDelay,
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
        DEFAULT_SCALE_ACTION_PERIOD_MILLIS
    );

    // Cost function weights with defaults
    this.lagWeight = Configs.valueOrDefault(lagWeight, DEFAULT_LAG_WEIGHT);
    this.idleWeight = Configs.valueOrDefault(idleWeight, DEFAULT_IDLE_WEIGHT);
    this.useTaskCountBoundariesOnScaleUp = Configs.valueOrDefault(useTaskCountBoundariesOnScaleUp, false);
    this.useTaskCountBoundariesOnScaleDown = Configs.valueOrDefault(useTaskCountBoundariesOnScaleDown, true);
    this.minScaleUpDelay = Configs.valueOrDefault(
        minScaleUpDelay,
        Duration.millis(this.minTriggerScaleActionFrequencyMillis)
    );
    this.minScaleDownDelay = Configs.valueOrDefault(minScaleDownDelay, DEFAULT_MIN_SCALE_DELAY);
    this.scaleDownDuringTaskRolloverOnly = Configs.valueOrDefault(scaleDownDuringTaskRolloverOnly, false);

    if (this.enableTaskAutoScaler) {
      if (useTaskCountBoundaries != null) {
        LOG.warn("useTaskCountBoundaries is removed, "
                 + "use useTaskCountBoundariesOnScaleUp and useTaskCountBoundariesOnScaleDown instead");
      }
      if (highLagThreshold != null) {
        LOG.warn("highLagThreshold is removed, the autoscaler behavior is good enough just with cost function");
      }

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

  /**
   * Enables or disables the task-count evaluation window when considering scale-up candidates.
   * If enabled, the optimizer only evaluates a small number of candidate task counts above the current count,
   * which prevents large scale-up jumps.
   */
  @JsonProperty("useTaskCountBoundariesOnScaleUp")
  public boolean shouldUseTaskCountBoundariesOnScaleUp()
  {
    return useTaskCountBoundariesOnScaleUp;
  }

  /**
   * Enables or disables the task-count evaluation window when considering scale-down candidates.
   * If enabled, the optimizer only evaluates a small number of candidate task counts below the current count,
   * which prevents large scale-down drops.
   */
  @JsonProperty("useTaskCountBoundariesOnScaleDown")
  public boolean shouldUseTaskCountBoundariesOnScaleDown()
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
           && useTaskCountBoundariesOnScaleUp == that.useTaskCountBoundariesOnScaleUp
           && useTaskCountBoundariesOnScaleDown == that.useTaskCountBoundariesOnScaleDown
           && Objects.equals(minScaleUpDelay, that.minScaleUpDelay)
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
        useTaskCountBoundariesOnScaleUp,
        useTaskCountBoundariesOnScaleDown,
        minScaleUpDelay,
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
           ", useTaskCountBoundariesOnScaleUp=" + useTaskCountBoundariesOnScaleUp +
           ", useTaskCountBoundariesOnScaleDown=" + useTaskCountBoundariesOnScaleDown +
           ", minScaleUpDelay=" + minScaleUpDelay +
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
    private Boolean useTaskCountBoundariesOnScaleUp;
    private Boolean useTaskCountBoundariesOnScaleDown;
    private Duration minScaleUpDelay;
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
          null,
          null,
          useTaskCountBoundariesOnScaleUp,
          useTaskCountBoundariesOnScaleDown,
          minScaleUpDelay,
          minScaleDownDelay,
          scaleDownDuringTaskRolloverOnly
      );
    }
  }
}
