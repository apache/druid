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
  @Nullable
  private final Integer maxRowsPerSegment;
  @Nullable
  private final Integer maxRowsInMemory;
  @Nullable
  private final Integer maxTotalRows;
  @Nullable
  private final IndexSpec indexSpec;
  @Nullable
  private final Integer maxPendingPersists;
  @Nullable
  private final Long pushTimeout;

  public static ClientCompactQueryTuningConfig from(
      @Nullable UserCompactTuningConfig userCompactionTaskQueryTuningConfig,
      @Nullable Integer maxRowsPerSegment
  )
  {
    return new ClientCompactQueryTuningConfig(
        maxRowsPerSegment,
        userCompactionTaskQueryTuningConfig == null ? null : userCompactionTaskQueryTuningConfig.getMaxRowsInMemory(),
        userCompactionTaskQueryTuningConfig == null ? null : userCompactionTaskQueryTuningConfig.getMaxTotalRows(),
        userCompactionTaskQueryTuningConfig == null ? null : userCompactionTaskQueryTuningConfig.getIndexSpec(),
        userCompactionTaskQueryTuningConfig == null ? null : userCompactionTaskQueryTuningConfig.getMaxPendingPersists(),
        userCompactionTaskQueryTuningConfig == null ? null : userCompactionTaskQueryTuningConfig.getPushTimeout()
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
    this.maxRowsInMemory = maxRowsInMemory;
    this.maxTotalRows = maxTotalRows;
    this.indexSpec = indexSpec;
    this.maxPendingPersists = maxPendingPersists;
    this.pushTimeout = pushTimeout;
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
  @Nullable
  public Integer getMaxRowsInMemory()
  {
    return maxRowsInMemory;
  }

  @JsonProperty
  @Nullable
  public Integer getMaxTotalRows()
  {
    return maxTotalRows;
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
           Objects.equals(maxRowsInMemory, that.maxRowsInMemory) &&
           Objects.equals(maxTotalRows, that.maxTotalRows) &&
           Objects.equals(indexSpec, that.indexSpec) &&
           Objects.equals(maxPendingPersists, that.maxPendingPersists) &&
           Objects.equals(pushTimeout, that.pushTimeout);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(maxRowsPerSegment, maxRowsInMemory, maxTotalRows, indexSpec, maxPendingPersists, pushTimeout);
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{" +
           "maxRowsPerSegment=" + maxRowsPerSegment +
           ", maxRowsInMemory=" + maxRowsInMemory +
           ", maxTotalRows=" + maxTotalRows +
           ", indexSpec=" + indexSpec +
           ", maxPendingPersists=" + maxPendingPersists +
           ", pushTimeout=" + pushTimeout +
           '}';
  }
}
