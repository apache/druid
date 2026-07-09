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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.guice.annotations.UnstableApi;
import org.apache.druid.indexing.overlord.supervisor.Supervisor;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.SupervisorTaskAutoScaler;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.joda.time.Duration;

@UnstableApi
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "autoScalerStrategy", defaultImpl = LagBasedAutoScalerConfig.class)
@JsonSubTypes(value = {
    @Type(name = "lagBased", value = LagBasedAutoScalerConfig.class),
    @Type(name = "costBased", value = CostBasedAutoScalerConfig.class)
})
public interface AutoScalerConfig
{
  boolean getEnableTaskAutoScaler();

  /**
   * @deprecated Use {@link #getMinScaleUpDelay()} and {@link #getMinScaleDownDelay()} instead.
   * This field is retained for backward compatibility and will be removed in a future version.
   */
  @Deprecated
  long getMinTriggerScaleActionFrequencyMillis();

  /**
   * Minimum time that must elapse after any scale action before a scale-up is permitted.
   * If not explicitly configured, implementations fall back to
   * {@link #getMinTriggerScaleActionFrequencyMillis()} for backward compatibility.
   */
  Duration getMinScaleUpDelay();

  /**
   * Minimum time that must elapse after any scale action before a scale-down is permitted.
   * If not explicitly configured, implementations fall back to
   * {@link #getMinTriggerScaleActionFrequencyMillis()} for backward compatibility.
   */
  Duration getMinScaleDownDelay();

  int getTaskCountMax();
  int getTaskCountMin();
  Integer getTaskCountStart();
  Double getStopTaskCountRatio();
  SupervisorTaskAutoScaler createAutoScaler(Supervisor supervisor, SupervisorSpec spec, ServiceEmitter emitter);
}

