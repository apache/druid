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

package org.apache.druid.indexing.overlord.setup;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexing.overlord.autoscaling.AutoScaler;
import org.apache.druid.indexing.worker.config.WorkerConfig;

import java.util.List;
import java.util.Objects;

public class CategoriedWorkerBehaviorConfig implements WorkerBehaviorConfig
{
  // Use the same category constant as for worker category to match default workers and autoscalers
  public static final String DEFAULT_AUTOSCALER_CATEGORY = WorkerConfig.DEFAULT_CATEGORY;

  private final WorkerSelectStrategy selectStrategy;
  private final List<AutoScaler> autoScalers;

  @JsonCreator
  public CategoriedWorkerBehaviorConfig(
      @JsonProperty("selectStrategy") WorkerSelectStrategy selectStrategy,
      @JsonProperty("autoScalers") List<AutoScaler> autoScalers
  )
  {
    this.selectStrategy = selectStrategy;
    this.autoScalers = autoScalers;
  }

  @Override
  @JsonProperty
  public WorkerSelectStrategy getSelectStrategy()
  {
    return selectStrategy;
  }

  @JsonProperty
  public List<AutoScaler> getAutoScalers()
  {
    return autoScalers;
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
    CategoriedWorkerBehaviorConfig that = (CategoriedWorkerBehaviorConfig) o;
    return Objects.equals(selectStrategy, that.selectStrategy) &&
           Objects.equals(autoScalers, that.autoScalers);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(selectStrategy, autoScalers);
  }

  @Override
  public String toString()
  {
    return "WorkerConfiguration{" +
           "selectStrategy=" + selectStrategy +
           ", autoScalers=" + autoScalers +
           '}';
  }
}
