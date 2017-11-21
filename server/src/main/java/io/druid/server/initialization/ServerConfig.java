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

package io.druid.server.initialization;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.Period;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.Objects;

/**
 */
public class ServerConfig
{
  @JsonProperty
  @Min(1)
  private int numThreads = Math.max(10, (Runtime.getRuntime().availableProcessors() * 17) / 16 + 2) + 30;

  @JsonProperty
  @Min(1)
  private int queueSize = Integer.MAX_VALUE;

  @JsonProperty
  private boolean enableRequestLimit = false;

  @JsonProperty
  @NotNull
  private Period maxIdleTime = new Period("PT5m");

  @JsonProperty
  @Min(0)
  private long defaultQueryTimeout = 300_000; // 5 minutes

  @JsonProperty
  @Min(1)
  private long maxScatterGatherBytes = Long.MAX_VALUE;

  @JsonProperty
  @Min(1)
  private long maxQueryTimeout = Long.MAX_VALUE;

  public int getNumThreads()
  {
    return numThreads;
  }

  public int getQueueSize()
  {
    return queueSize;
  }

  public boolean isEnableRequestLimit()
  {
    return enableRequestLimit;
  }

  public Period getMaxIdleTime()
  {
    return maxIdleTime;
  }

  public long getDefaultQueryTimeout()
  {
    return defaultQueryTimeout;
  }

  public long getMaxScatterGatherBytes()
  {
    return maxScatterGatherBytes;
  }

  public long getMaxQueryTimeout()
  {
    return maxQueryTimeout;
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
    ServerConfig that = (ServerConfig) o;
    return numThreads == that.numThreads &&
           queueSize == that.queueSize &&
           enableRequestLimit == that.enableRequestLimit &&
           defaultQueryTimeout == that.defaultQueryTimeout &&
           maxScatterGatherBytes == that.maxScatterGatherBytes &&
           Objects.equals(maxIdleTime, that.maxIdleTime) &&
           maxQueryTimeout == that.maxQueryTimeout;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        numThreads,
        queueSize,
        enableRequestLimit,
        maxIdleTime,
        defaultQueryTimeout,
        maxScatterGatherBytes,
        maxQueryTimeout
    );
  }
}
