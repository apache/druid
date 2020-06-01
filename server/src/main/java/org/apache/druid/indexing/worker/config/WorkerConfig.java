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

package org.apache.druid.indexing.worker.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.server.DruidNode;
import org.apache.druid.utils.JvmUtils;
import org.joda.time.Period;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 */
public class WorkerConfig
{
  public static final String DEFAULT_CATEGORY = "_default_worker_category";

  @JsonProperty
  private String ip = DruidNode.getDefaultHost();

  @JsonProperty
  private String version = "0";

  @JsonProperty
  @Min(1)
  private int capacity = Math.max(1, JvmUtils.getRuntimeInfo().getAvailableProcessors() - 1);

  @JsonProperty
  @NotNull
  private String category = DEFAULT_CATEGORY;

  private long intermediaryPartitionDiscoveryPeriodSec = 60L;

  @JsonProperty
  private long intermediaryPartitionCleanupPeriodSec = 300L;

  @JsonProperty
  private Period intermediaryPartitionTimeout = new Period("P1D");

  @JsonProperty
  private final long globalIngestionHeapLimitBytes = Runtime.getRuntime().maxMemory() / 6;

  @JsonProperty
  private final int numConcurrentMerges = Math.max(1, capacity / 2);

  public String getIp()
  {
    return ip;
  }

  public String getVersion()
  {
    return version;
  }

  public int getCapacity()
  {
    return capacity;
  }

  public String getCategory()
  {
    return category;
  }

  public long getIntermediaryPartitionDiscoveryPeriodSec()
  {
    return intermediaryPartitionDiscoveryPeriodSec;
  }

  public long getIntermediaryPartitionCleanupPeriodSec()
  {
    return intermediaryPartitionCleanupPeriodSec;
  }

  public Period getIntermediaryPartitionTimeout()
  {
    return intermediaryPartitionTimeout;
  }

  public long getGlobalIngestionHeapLimitBytes()
  {
    return globalIngestionHeapLimitBytes;
  }

  public int getNumConcurrentMerges()
  {
    return numConcurrentMerges;
  }
}
