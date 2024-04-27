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
import java.util.List;

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
  private long baseTaskDirSize = Long.MAX_VALUE;

  @JsonProperty
  private List<String> baseTaskDirs = null;

  @JsonProperty
  @NotNull
  private String category = DEFAULT_CATEGORY;

  private long intermediaryPartitionDiscoveryPeriodSec = 60L;

  @JsonProperty
  private long intermediaryPartitionCleanupPeriodSec = 300L;

  @JsonProperty
  private Period intermediaryPartitionTimeout = new Period("PT5M");

  @JsonProperty
  private long globalIngestionHeapLimitBytes = Runtime.getRuntime().maxMemory() / 6;

  @JsonProperty
  private int numConcurrentMerges = Math.max(1, capacity / 2);

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

  public WorkerConfig setCapacity(int capacity)
  {
    this.capacity = capacity;
    return this;
  }

  public long getBaseTaskDirSize()
  {
    return baseTaskDirSize;
  }

  public List<String> getBaseTaskDirs()
  {
    return baseTaskDirs;
  }

  @NotNull
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

  public Builder cloneBuilder()
  {
    return new Builder(this);
  }

  public static class Builder
  {
    private String ip;
    private String version;
    private int capacity;
    private long baseTaskDirSize;
    private List<String> baseTaskDirs;
    private String category;
    private long intermediaryPartitionDiscoveryPeriodSec;
    private long intermediaryPartitionCleanupPeriodSec;
    private Period intermediaryPartitionTimeout;
    private long globalIngestionHeapLimitBytes;
    private int numConcurrentMerges;

    private Builder(WorkerConfig input)
    {
      this.ip = input.ip;
      this.version = input.version;
      this.capacity = input.capacity;
      this.baseTaskDirSize = input.baseTaskDirSize;
      this.baseTaskDirs = input.baseTaskDirs;
      this.category = input.category;
      this.intermediaryPartitionDiscoveryPeriodSec = input.intermediaryPartitionDiscoveryPeriodSec;
      this.intermediaryPartitionCleanupPeriodSec = input.intermediaryPartitionCleanupPeriodSec;
      this.intermediaryPartitionTimeout = input.intermediaryPartitionTimeout;
      this.globalIngestionHeapLimitBytes = input.globalIngestionHeapLimitBytes;
      this.numConcurrentMerges = input.numConcurrentMerges;
    }

    public Builder setIp(String ip)
    {
      this.ip = ip;
      return this;
    }

    public Builder setVersion(String version)
    {
      this.version = version;
      return this;
    }

    public Builder setCapacity(int capacity)
    {
      this.capacity = capacity;
      return this;
    }

    public Builder setBaseTaskDirSize(long baseTaskDirSize)
    {
      this.baseTaskDirSize = baseTaskDirSize;
      return this;
    }

    public Builder setBaseTaskDirs(List<String> baseTaskDirs)
    {
      this.baseTaskDirs = baseTaskDirs;
      return this;
    }

    public Builder setCategory(String category)
    {
      this.category = category;
      return this;
    }

    public Builder setIntermediaryPartitionDiscoveryPeriodSec(long intermediaryPartitionDiscoveryPeriodSec)
    {
      this.intermediaryPartitionDiscoveryPeriodSec = intermediaryPartitionDiscoveryPeriodSec;
      return this;
    }

    public Builder setIntermediaryPartitionCleanupPeriodSec(long intermediaryPartitionCleanupPeriodSec)
    {
      this.intermediaryPartitionCleanupPeriodSec = intermediaryPartitionCleanupPeriodSec;
      return this;
    }

    public Builder setIntermediaryPartitionTimeout(Period intermediaryPartitionTimeout)
    {
      this.intermediaryPartitionTimeout = intermediaryPartitionTimeout;
      return this;
    }

    public Builder setGlobalIngestionHeapLimitBytes(long globalIngestionHeapLimitBytes)
    {
      this.globalIngestionHeapLimitBytes = globalIngestionHeapLimitBytes;
      return this;
    }

    public Builder setNumConcurrentMerges(int numConcurrentMerges)
    {
      this.numConcurrentMerges = numConcurrentMerges;
      return this;
    }

    public WorkerConfig build()
    {
      final WorkerConfig retVal = new WorkerConfig();
      retVal.ip = this.ip;
      retVal.version = this.version;
      retVal.capacity = this.capacity;
      retVal.baseTaskDirSize = this.baseTaskDirSize;
      retVal.baseTaskDirs = this.baseTaskDirs;
      retVal.category = this.category;
      retVal.intermediaryPartitionDiscoveryPeriodSec = this.intermediaryPartitionDiscoveryPeriodSec;
      retVal.intermediaryPartitionCleanupPeriodSec = this.intermediaryPartitionCleanupPeriodSec;
      retVal.intermediaryPartitionTimeout = this.intermediaryPartitionTimeout;
      retVal.globalIngestionHeapLimitBytes = this.globalIngestionHeapLimitBytes;
      retVal.numConcurrentMerges = this.numConcurrentMerges;
      return retVal;
    }
  }
}
