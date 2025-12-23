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

package org.apache.druid.segment.incremental;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.common.config.Configs;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class RowIngestionMetersTotals
{
  private final long processed;
  private final long processedBytes;
  private final long processedWithError;
  private final long thrownAway;
  private final Map<String, Long> thrownAwayByReason;
  private final long unparseable;

  @JsonCreator
  public RowIngestionMetersTotals(
      @JsonProperty("processed") long processed,
      @JsonProperty("processedBytes") long processedBytes,
      @JsonProperty("processedWithError") long processedWithError,
      @JsonProperty("thrownAway") long thrownAway,
      @JsonProperty("thrownAwayByReason") @Nullable Map<String, Long> thrownAwayByReason,
      @JsonProperty("unparseable") long unparseable
  )
  {
    this(
        processed,
        processedBytes,
        processedWithError,
        Configs.valueOrDefault(thrownAwayByReason, getBackwardsCompatibleThrownAwayByReason(thrownAway)),
        unparseable
    );
  }

  public RowIngestionMetersTotals(
      long processed,
      long processedBytes,
      long processedWithError,
      long thrownAway,
      long unparseable
  )
  {
    this(
        processed,
        processedBytes,
        processedWithError,
        getBackwardsCompatibleThrownAwayByReason(thrownAway),
        unparseable
    );
  }

  public RowIngestionMetersTotals(
      long processed,
      long processedBytes,
      long processedWithError,
      Map<String, Long> thrownAwayByReason,
      long unparseable
  )
  {
    this.processed = processed;
    this.processedBytes = processedBytes;
    this.processedWithError = processedWithError;
    this.thrownAway = thrownAwayByReason.values().stream().reduce(0L, Long::sum);
    this.thrownAwayByReason = thrownAwayByReason;
    this.unparseable = unparseable;
  }

  @JsonProperty
  public long getProcessed()
  {
    return processed;
  }

  @JsonProperty
  public long getProcessedBytes()
  {
    return processedBytes;
  }

  @JsonProperty
  public long getProcessedWithError()
  {
    return processedWithError;
  }

  @JsonProperty
  public long getThrownAway()
  {
    return thrownAway;
  }

  @JsonProperty
  public Map<String, Long> getThrownAwayByReason()
  {
    return thrownAwayByReason;
  }

  @JsonProperty
  public long getUnparseable()
  {
    return unparseable;
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
    RowIngestionMetersTotals that = (RowIngestionMetersTotals) o;
    return processed == that.processed
           && processedBytes == that.processedBytes
           && processedWithError == that.processedWithError
           && thrownAway == that.thrownAway
           && thrownAwayByReason.equals(that.thrownAwayByReason)
           && unparseable == that.unparseable;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(processed, processedBytes, processedWithError, thrownAway, thrownAwayByReason, unparseable);
  }

  @Override
  public String toString()
  {
    return "RowIngestionMetersTotals{" +
           "processed=" + processed +
           ", processedBytes=" + processedBytes +
           ", processedWithError=" + processedWithError +
           ", thrownAway=" + thrownAway +
           ", thrownAwayByReason=" + thrownAwayByReason +
           ", unparseable=" + unparseable +
           '}';
  }

  /**
   * For backwards compatibility, key by {@link InputRowFilterResult} in case of lack of thrownAwayByReason input during rolling Druid upgrades.
   * This can occur when tasks running on older Druid versions return ingest statistic payloads to an overlord running on a newer Druid version.
   */
  private static Map<String, Long> getBackwardsCompatibleThrownAwayByReason(long thrownAway)
  {
    Map<String, Long> results = new HashMap<>();
    if (thrownAway > 0) {
      results.put(InputRowFilterResult.UNKNOWN.getReason(), thrownAway);
    }
    return results;
  }
}
