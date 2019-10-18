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

package org.apache.druid.client.indexing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig.UserCompactTuningConfig;

import javax.annotation.Nullable;
import java.util.Objects;

public class ClientCompactQueryTuningConfig
{
  @Nullable
  private final Integer maxRowsPerSegment;
  @Nullable
  private final Long maxBytesInMemory;
  @Nullable
  private final Integer maxRowsInMemory;
  @Nullable
  private final Long maxTotalRows;
  @Nullable
  private final SplitHintSpec splitHintSpec;
  @Nullable
  private final IndexSpec indexSpec;
  @Nullable
  private final Integer maxPendingPersists;
  @Nullable
  private final Long pushTimeout;
  @Nullable
  private final Integer maxNumConcurrentSubTasks;

  public static ClientCompactQueryTuningConfig from(
      @Nullable UserCompactTuningConfig userCompactionTaskQueryTuningConfig,
      @Nullable Integer maxRowsPerSegment
  )
  {
    if (userCompactionTaskQueryTuningConfig == null) {
      return new ClientCompactQueryTuningConfig(
          maxRowsPerSegment,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null
      );
    } else {
      return new ClientCompactQueryTuningConfig(
          maxRowsPerSegment,
          userCompactionTaskQueryTuningConfig.getMaxRowsInMemory(),
          userCompactionTaskQueryTuningConfig.getMaxBytesInMemory(),
          userCompactionTaskQueryTuningConfig.getMaxTotalRows(),
          userCompactionTaskQueryTuningConfig.getSplitHintSpec(),
          userCompactionTaskQueryTuningConfig.getIndexSpec(),
          userCompactionTaskQueryTuningConfig.getMaxPendingPersists(),
          userCompactionTaskQueryTuningConfig.getPushTimeout(),
          userCompactionTaskQueryTuningConfig.getMaxNumConcurrentSubTasks()
      );
    }
  }

  @JsonCreator
  public ClientCompactQueryTuningConfig(
      @JsonProperty("maxRowsPerSegment") @Nullable Integer maxRowsPerSegment,
      @JsonProperty("maxRowsInMemory") @Nullable Integer maxRowsInMemory,
      @JsonProperty("maxBytesInMemory") @Nullable Long maxBytesInMemory,
      @JsonProperty("maxTotalRows") @Nullable Long maxTotalRows,
      @JsonProperty("splitHintSpec") @Nullable SplitHintSpec splitHintSpec,
      @JsonProperty("indexSpec") @Nullable IndexSpec indexSpec,
      @JsonProperty("maxPendingPersists") @Nullable Integer maxPendingPersists,
      @JsonProperty("pushTimeout") @Nullable Long pushTimeout,
      @JsonProperty("maxNumConcurrentSubTasks") @Nullable Integer maxNumConcurrentSubTasks
  )
  {
    this.maxRowsPerSegment = maxRowsPerSegment;
    this.maxBytesInMemory = maxBytesInMemory;
    this.maxRowsInMemory = maxRowsInMemory;
    this.maxTotalRows = maxTotalRows;
    this.splitHintSpec = splitHintSpec;
    this.indexSpec = indexSpec;
    this.maxPendingPersists = maxPendingPersists;
    this.pushTimeout = pushTimeout;
    this.maxNumConcurrentSubTasks = maxNumConcurrentSubTasks;
  }

  @JsonProperty
  public String getType()
  {
    return "index_parallel";
  }

  @JsonProperty
  @Nullable
  public Integer getMaxRowsPerSegment()
  {
    return maxRowsPerSegment;
  }

  @JsonProperty
  @Nullable
  public Integer getMaxRowsInMemory()
  {
    return maxRowsInMemory;
  }

  @JsonProperty
  @Nullable
  public Long getMaxBytesInMemory()
  {
    return maxBytesInMemory;
  }

  @JsonProperty
  @Nullable
  public Long getMaxTotalRows()
  {
    return maxTotalRows;
  }

  @Nullable
  @JsonProperty
  public SplitHintSpec getSplitHintSpec()
  {
    return splitHintSpec;
  }

  public long getMaxTotalRowsOr(long defaultMaxTotalRows)
  {
    return maxTotalRows == null ? defaultMaxTotalRows : maxTotalRows;
  }

  @JsonProperty
  @Nullable
  public IndexSpec getIndexSpec()
  {
    return indexSpec;
  }

  @JsonProperty
  @Nullable
  public Integer getMaxPendingPersists()
  {
    return maxPendingPersists;
  }

  @JsonProperty
  @Nullable
  public Long getPushTimeout()
  {
    return pushTimeout;
  }

  @JsonProperty
  @Nullable
  public Integer getMaxNumConcurrentSubTasks()
  {
    return maxNumConcurrentSubTasks;
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
    ClientCompactQueryTuningConfig that = (ClientCompactQueryTuningConfig) o;
    return Objects.equals(maxRowsPerSegment, that.maxRowsPerSegment) &&
           Objects.equals(maxBytesInMemory, that.maxBytesInMemory) &&
           Objects.equals(maxRowsInMemory, that.maxRowsInMemory) &&
           Objects.equals(maxTotalRows, that.maxTotalRows) &&
           Objects.equals(indexSpec, that.indexSpec) &&
           Objects.equals(maxPendingPersists, that.maxPendingPersists) &&
           Objects.equals(pushTimeout, that.pushTimeout) &&
           Objects.equals(maxNumConcurrentSubTasks, that.maxNumConcurrentSubTasks);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        maxRowsPerSegment,
        maxBytesInMemory,
        maxRowsInMemory,
        maxTotalRows,
        indexSpec,
        maxPendingPersists,
        pushTimeout,
        maxNumConcurrentSubTasks
    );
  }

  @Override
  public String toString()
  {
    return "ClientCompactQueryTuningConfig{" +
           "maxRowsPerSegment=" + maxRowsPerSegment +
           ", maxBytesInMemory=" + maxBytesInMemory +
           ", maxRowsInMemory=" + maxRowsInMemory +
           ", maxTotalRows=" + maxTotalRows +
           ", indexSpec=" + indexSpec +
           ", maxPendingPersists=" + maxPendingPersists +
           ", pushTimeout=" + pushTimeout +
           ", maxNumConcurrentSubTasks=" + maxNumConcurrentSubTasks +
           '}';
  }
}
