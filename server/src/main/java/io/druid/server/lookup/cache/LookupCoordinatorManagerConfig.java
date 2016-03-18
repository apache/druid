/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.server.lookup.cache;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.Duration;

import javax.validation.constraints.Min;

public class LookupCoordinatorManagerConfig
{
  public static final Duration DEFAULT_HOST_DELETE_TIMEOUT = Duration.millis(1_000L);
  public static final Duration DEFAULT_HOST_UPDATE_TIMEOUT = Duration.millis(10_000L);
  public static final Duration DEFAULT_DELETE_ALL_TIMEOUT = Duration.millis(10_000L);
  public static final Duration DEFAULT_UPDATE_ALL_TIMEOUT = Duration.millis(60_000L);
  @JsonProperty
  private Duration hostDeleteTimeout = null;
  @JsonProperty
  private Duration hostUpdateTimeout = null;
  @JsonProperty
  private Duration deleteAllTimeout = null;
  @JsonProperty
  private Duration updateAllTimeout = null;
  @JsonProperty
  @Min(1)
  private int threadPoolSize = 10;
  @JsonProperty
  @Min(1)
  private long period = 30_000L;

  public Duration getHostDeleteTimeout()
  {
    return hostDeleteTimeout == null ? DEFAULT_HOST_DELETE_TIMEOUT : hostDeleteTimeout;
  }

  public void setHostDeleteTimeout(Duration hostDeleteTimeout)
  {
    this.hostDeleteTimeout = hostDeleteTimeout;
  }

  public Duration getHostUpdateTimeout()
  {
    return hostUpdateTimeout == null ? DEFAULT_HOST_UPDATE_TIMEOUT : hostUpdateTimeout;
  }

  public void setHostUpdateTimeout(Duration hostUpdateTimeout)
  {
    this.hostUpdateTimeout = hostUpdateTimeout;
  }

  public Duration getDeleteAllTimeout()
  {
    return deleteAllTimeout == null ? DEFAULT_DELETE_ALL_TIMEOUT : deleteAllTimeout;
  }

  public void setDeleteAllTimeout(Duration deleteAllTimeout)
  {
    this.deleteAllTimeout = deleteAllTimeout;
  }

  public Duration getUpdateAllTimeout()
  {
    return updateAllTimeout == null ? DEFAULT_UPDATE_ALL_TIMEOUT : updateAllTimeout;
  }

  public void setUpdateAllTimeout(Duration updateAllTimeout)
  {
    this.updateAllTimeout = updateAllTimeout;
  }

  public int getThreadPoolSize()
  {
    return threadPoolSize;
  }

  public void setThreadPoolSize(int threadPoolSize)
  {
    this.threadPoolSize = threadPoolSize;
  }

  public long getPeriod()
  {
    return period;
  }

  public void setPeriod(long period)
  {
    this.period = period;
  }
}
