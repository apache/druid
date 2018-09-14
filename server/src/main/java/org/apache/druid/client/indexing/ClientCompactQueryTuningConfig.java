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

import javax.annotation.Nullable;
import java.util.Objects;

public class ClientCompactQueryTuningConfig
{
  // These default values should be synchronized with those of IndexTuningConfig
  private static final int DEFAULT_MAX_ROWS_IN_MEMORY = 75_000;
  private static final int DEFAULT_MAX_TOTAL_ROWS = 20_000_000;
  private static final IndexSpec DEFAULT_INDEX_SPEC = new IndexSpec();
  private static final int DEFAULT_MAX_PENDING_PERSISTS = 0;
  private static final long DEFAULT_PUBLISH_TIMEOUT = 0;

  private final int maxRowsInMemory;
  private final int maxTotalRows;
  private final IndexSpec indexSpec;
  private final int maxPendingPersists;
  private final long publishTimeout;

  @JsonCreator
  public ClientCompactQueryTuningConfig(
      @JsonProperty("maxRowsInMemory") @Nullable Integer maxRowsInMemory,
      @JsonProperty("maxTotalRows") @Nullable Integer maxTotalRows,
      @JsonProperty("indexSpec") @Nullable IndexSpec indexSpec,
      @JsonProperty("maxPendingPersists") @Nullable Integer maxPendingPersists,
      @JsonProperty("publishTimeout") @Nullable Long publishTimeout
  )
  {
    this.maxRowsInMemory = maxRowsInMemory == null ? DEFAULT_MAX_ROWS_IN_MEMORY : maxRowsInMemory;
    this.maxTotalRows = maxTotalRows == null ? DEFAULT_MAX_TOTAL_ROWS : maxTotalRows;
    this.indexSpec = indexSpec == null ? DEFAULT_INDEX_SPEC : indexSpec;
    this.maxPendingPersists = maxPendingPersists == null ? DEFAULT_MAX_PENDING_PERSISTS : maxPendingPersists;
    this.publishTimeout = publishTimeout == null ? DEFAULT_PUBLISH_TIMEOUT : publishTimeout;
  }

  @JsonProperty
  public String getType()
  {
    return "index";
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
  public long getPublishTimeout()
  {
    return publishTimeout;
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

    final ClientCompactQueryTuningConfig that = (ClientCompactQueryTuningConfig) o;

    if (maxRowsInMemory != that.maxRowsInMemory) {
      return false;
    }

    if (maxTotalRows != that.maxTotalRows) {
      return false;
    }

    if (!indexSpec.equals(that.indexSpec)) {
      return false;
    }

    if (maxPendingPersists != that.maxPendingPersists) {
      return false;
    }

    return publishTimeout == that.publishTimeout;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(maxRowsInMemory, maxTotalRows, indexSpec, maxPendingPersists, publishTimeout);
  }

  @Override
  public String toString()
  {
    return "ClientCompactQueryTuningConfig{" +
           "maxRowsInMemory='" + maxRowsInMemory +
           ", maxTotalRows='" + maxTotalRows +
           ", indexSpec='" + indexSpec +
           ", maxPendingPersists='" + maxPendingPersists +
           ", publishTimeout='" + publishTimeout +
           "}";
  }
}
