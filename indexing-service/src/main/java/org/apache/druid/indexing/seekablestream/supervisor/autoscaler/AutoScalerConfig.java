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

import com.fasterxml.jackson.annotation.JsonProperty;

public class AutoScalerConfig
{
  @JsonProperty("metricsCollectionIntervalMillis")
  private long metricsCollectionIntervalMillis = 30000;
  @JsonProperty("metricsCollectionRangeMillis")
  private long metricsCollectionRangeMillis = 600000;
  @JsonProperty("dynamicCheckStartDelayMillis")
  private long dynamicCheckStartDelayMillis = 300000;
  @JsonProperty("dynamicCheckPeriod")
  private long dynamicCheckPeriod = 60000;
  @JsonProperty("scaleOutThreshold")
  private long scaleOutThreshold = 6000000;
  @JsonProperty("scaleInThreshold")
  private long scaleInThreshold = 1000000;
  @JsonProperty("triggerScaleOutThresholdFrequency")
  private double triggerScaleOutThresholdFrequency = 0.3;
  @JsonProperty("triggerScaleInThresholdFrequency")
  private double triggerScaleInThresholdFrequency = 0.9;
  @JsonProperty("taskCountMax")
  private int taskCountMax = 4;
  @JsonProperty("taskCountMin")
  private int taskCountMin = 1;
  @JsonProperty("scaleInStep")
  private int scaleInStep = 1;
  @JsonProperty("scaleOutStep")
  private int scaleOutStep = 2;
  @JsonProperty("enableTaskAutoscaler")
  private boolean enableTaskAutoscaler = true;
  @JsonProperty("autoScalerStrategy")
  private String autoScalerStrategy = "default";
  @JsonProperty("minTriggerDynamicFrequencyMillis")
  private long minTriggerDynamicFrequencyMillis = 600000;



  @JsonProperty
  public long getMetricsCollectionIntervalMillis()
  {
    return metricsCollectionIntervalMillis;
  }

  @JsonProperty
  public long getMetricsCollectionRangeMillis()
  {
    return metricsCollectionRangeMillis;
  }

  @JsonProperty
  public long getDynamicCheckStartDelayMillis()
  {
    return dynamicCheckStartDelayMillis;
  }

  @JsonProperty
  public long getDynamicCheckPeriod()
  {
    return dynamicCheckPeriod;
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
  public double getTriggerScaleOutThresholdFrequency()
  {
    return triggerScaleOutThresholdFrequency;
  }

  @JsonProperty
  public double getTriggerScaleInThresholdFrequency()
  {
    return triggerScaleInThresholdFrequency;
  }

  @JsonProperty
  public int getTaskCountMax()
  {
    return taskCountMax;
  }

  @JsonProperty
  public int getTaskCountMin()
  {
    return taskCountMin;
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

  @JsonProperty
  public boolean getEnableTaskAutoscaler()
  {
    return enableTaskAutoscaler;
  }

  @JsonProperty
  public String getAutoScalerStrategy()
  {
    return autoScalerStrategy;
  }

  @JsonProperty
  public long getMinTriggerDynamicFrequencyMillis()
  {
    return minTriggerDynamicFrequencyMillis;
  }

}
