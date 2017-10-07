/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.overlord.setup;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.indexing.overlord.autoscaling.AutoScaler;
import io.druid.indexing.overlord.autoscaling.NoopAutoScaler;

/**
 */
public class DefaultWorkerBehaviorConfig implements WorkerBehaviorConfig
{
  private static final AutoScaler DEFAULT_AUTOSCALER = new NoopAutoScaler();

  public static DefaultWorkerBehaviorConfig defaultConfig()
  {
    return new DefaultWorkerBehaviorConfig(DEFAULT_STRATEGY, DEFAULT_AUTOSCALER);
  }

  private final WorkerSelectStrategy selectStrategy;
  private final AutoScaler autoScaler;

  @JsonCreator
  public DefaultWorkerBehaviorConfig(
      @JsonProperty("selectStrategy") WorkerSelectStrategy selectStrategy,
      @JsonProperty("autoScaler") AutoScaler autoScaler
  )
  {
    this.selectStrategy = selectStrategy;
    this.autoScaler = autoScaler;
  }

  @Override
  @JsonProperty
  public WorkerSelectStrategy getSelectStrategy()
  {
    return selectStrategy;
  }

  @JsonProperty
  public AutoScaler<?> getAutoScaler()
  {
    return autoScaler;
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

    DefaultWorkerBehaviorConfig that = (DefaultWorkerBehaviorConfig) o;

    if (autoScaler != null ? !autoScaler.equals(that.autoScaler) : that.autoScaler != null) {
      return false;
    }
    if (selectStrategy != null ? !selectStrategy.equals(that.selectStrategy) : that.selectStrategy != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = selectStrategy != null ? selectStrategy.hashCode() : 0;
    result = 31 * result + (autoScaler != null ? autoScaler.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "WorkerConfiguration{" +
           "selectStrategy=" + selectStrategy +
           ", autoScaler=" + autoScaler +
           '}';
  }
}
