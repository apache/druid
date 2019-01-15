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
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig.UserCompactTuningConfig;

import javax.annotation.Nullable;
import java.util.Objects;

public class ClientCompactQueryTuningConfig
{
  // These default values should be synchronized with those of IndexTuningConfig
  private static final int DEFAULT_MAX_ROWS_IN_MEMORY = 75_000;
  private static final int DEFAULT_MAX_TOTAL_ROWS = 20_000_000;
  private static final IndexSpec DEFAULT_INDEX_SPEC = new IndexSpec();
  private static final int DEFAULT_MAX_PENDING_PERSISTS = 0;
  private static final long DEFAULT_PUSH_TIMEOUT = 0;

  @Nullable
  private final Integer maxRowsPerSegment;
  private final int maxRowsInMemory;
  private final int maxTotalRows;
  private final IndexSpec indexSpec;
  private final int maxPendingPersists;
  private final long pushTimeout;

  public static ClientCompactQueryTuningConfig from(
      @Nullable UserCompactTuningConfig userCompactTuningConfig,
      @Nullable Integer maxRowsPerSegment
  )
  {
    return new ClientCompactQueryTuningConfig(
        maxRowsPerSegment,
        userCompactTuningConfig == null ? null : userCompactTuningConfig.getMaxRowsInMemory(),
        userCompactTuningConfig == null ? null : userCompactTuningConfig.getMaxTotalRows(),
        userCompactTuningConfig == null ? null : userCompactTuningConfig.getIndexSpec(),
        userCompactTuningConfig == null ? null : userCompactTuningConfig.getMaxPendingPersists(),
        userCompactTuningConfig == null ? null : userCompactTuningConfig.getPushTimeout()
    );
  }

  @JsonCreator
  public ClientCompactQueryTuningConfig(
      @JsonProperty("maxRowsPerSegment") @Nullable Integer maxRowsPerSegment,
      @JsonProperty("maxRowsInMemory") @Nullable Integer maxRowsInMemory,
      @JsonProperty("maxTotalRows") @Nullable Integer maxTotalRows,
      @JsonProperty("indexSpec") @Nullable IndexSpec indexSpec,
      @JsonProperty("maxPendingPersists") @Nullable Integer maxPendingPersists,
      @JsonProperty("pushTimeout") @Nullable Long pushTimeout
  )
  {
    this.maxRowsPerSegment = maxRowsPerSegment;
    this.maxRowsInMemory = maxRowsInMemory == null ? DEFAULT_MAX_ROWS_IN_MEMORY : maxRowsInMemory;
    this.maxTotalRows = maxTotalRows == null ? DEFAULT_MAX_TOTAL_ROWS : maxTotalRows;
    this.indexSpec = indexSpec == null ? DEFAULT_INDEX_SPEC : indexSpec;
    this.maxPendingPersists = maxPendingPersists == null ? DEFAULT_MAX_PENDING_PERSISTS : maxPendingPersists;
    this.pushTimeout = pushTimeout == null ? DEFAULT_PUSH_TIMEOUT : pushTimeout;
  }

  @JsonProperty
  public String getType()
  {
    return "index";
  }

  @JsonProperty
  @Nullable
  public Integer getMaxRowsPerSegment()
  {
    return maxRowsPerSegment;
  }

  @JsonProperty
  public int getMaxRowsInMemory()
  {
    return maxRowsInMemory;
  }

  @JsonProperty
  public int getMaxTotalRows()
  {
    return maxTotalRows;
  }

  @JsonProperty
  public IndexSpec getIndexSpec()
  {
    return indexSpec;
  }

  @JsonProperty
  public int getMaxPendingPersists()
  {
    return maxPendingPersists;
  }

  @JsonProperty
  public long getPushTimeout()
  {
    return pushTimeout;
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
    return maxRowsInMemory == that.maxRowsInMemory &&
           maxTotalRows == that.maxTotalRows &&
           maxPendingPersists == that.maxPendingPersists &&
           pushTimeout == that.pushTimeout &&
           Objects.equals(maxRowsPerSegment, that.maxRowsPerSegment) &&
           Objects.equals(indexSpec, that.indexSpec);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        maxRowsPerSegment,
        maxRowsInMemory,
        maxTotalRows,
        indexSpec,
        maxPendingPersists,
        pushTimeout
    );
  }

  @Override
  public String toString()
  {
    return "ClientCompactQueryTuningConfig{" +
           "maxRowsPerSegment=" + maxRowsPerSegment +
           ", maxRowsInMemory=" + maxRowsInMemory +
           ", maxTotalRows=" + maxTotalRows +
           ", indexSpec=" + indexSpec +
           ", maxPendingPersists=" + maxPendingPersists +
           ", pushTimeout=" + pushTimeout +
           '}';
  }
}
