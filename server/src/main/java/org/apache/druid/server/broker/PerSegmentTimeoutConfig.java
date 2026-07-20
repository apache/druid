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

package org.apache.druid.server.broker;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.common.config.Configs;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Per-datasource configuration for per-segment timeout on the broker.
 * Used within {@link BrokerDynamicConfig#getPerSegmentTimeoutConfig()}.
 */
public class PerSegmentTimeoutConfig
{
  private final long perSegmentTimeoutMs;
  private final boolean monitorOnly;

  @JsonCreator
  public PerSegmentTimeoutConfig(
      @JsonProperty("perSegmentTimeoutMs") long perSegmentTimeoutMs,
      @JsonProperty("monitorOnly") @Nullable Boolean monitorOnly
  )
  {
    Preconditions.checkArgument(perSegmentTimeoutMs > 0, "perSegmentTimeoutMs must be > 0, got [%s]", perSegmentTimeoutMs);
    this.perSegmentTimeoutMs = perSegmentTimeoutMs;
    this.monitorOnly = Configs.valueOrDefault(monitorOnly, false);
  }

  @JsonProperty
  public long getPerSegmentTimeoutMs()
  {
    return perSegmentTimeoutMs;
  }

  @JsonProperty
  public boolean isMonitorOnly()
  {
    return monitorOnly;
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
    PerSegmentTimeoutConfig that = (PerSegmentTimeoutConfig) o;
    return perSegmentTimeoutMs == that.perSegmentTimeoutMs
           && monitorOnly == that.monitorOnly;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(perSegmentTimeoutMs, monitorOnly);
  }

  @Override
  public String toString()
  {
    return "PerSegmentTimeoutConfig{" +
           "perSegmentTimeoutMs=" + perSegmentTimeoutMs +
           ", monitorOnly=" + monitorOnly +
           '}';
  }
}
