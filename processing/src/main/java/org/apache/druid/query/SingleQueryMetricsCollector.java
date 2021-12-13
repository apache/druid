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

package org.apache.druid.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.apache.druid.java.util.common.IAE;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.Objects;

@NotThreadSafe
public class SingleQueryMetricsCollector
{
  private String subQueryId;
  private int segments;
  private int segmentsProcessed;
  private int segmentsVectorProcessed;
  private long segmentRows;
  private long preFilteredRows;
  private long nodeRows;
  private long resultRows;
  private long cpuNanos;
  private int threads;
  private long queryStart;
  private long queryMs;

  private final LongSet threadAccumulator;

  public static SingleQueryMetricsCollector newCollector()
  {
    return new SingleQueryMetricsCollector(null, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
  }

  @JsonCreator
  public SingleQueryMetricsCollector(
      @JsonProperty("subQueryId") @Nullable String subQueryId,
      @JsonProperty("segments") int segments,
      @JsonProperty("segmentsProcessed") int segmentsProcessed,
      @JsonProperty("segmentsVectorProcessed") int segmentsVectorProcessed,
      @JsonProperty("segmentRows") long segmentRows,
      @JsonProperty("preFilteredRows") long preFilteredRows,
      @JsonProperty("nodeRows") long nodeRows,
      @JsonProperty("resultRows") long resultRows,
      @JsonProperty("cpuNanos") long cpuNanos,
      @JsonProperty("threads") int threads,
      @JsonProperty("queryStart") long queryStart,
      @JsonProperty("queryMs") long queryMs
  )
  {
    this.subQueryId = subQueryId;
    this.threadAccumulator = new LongOpenHashSet();
    this.segments = segments;
    this.segmentsProcessed = segmentsProcessed;
    this.segmentsVectorProcessed = segmentsVectorProcessed;
    this.segmentRows = segmentRows;
    this.preFilteredRows = preFilteredRows;
    this.nodeRows = nodeRows;
    this.resultRows = resultRows;
    this.cpuNanos = cpuNanos;
    this.threads = threads;
    this.queryStart = queryStart;
    this.queryMs = queryMs;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getSubQueryId()
  {
    return subQueryId;
  }

  @JsonProperty
  public int getSegments()
  {
    return segments;
  }

  @JsonProperty
  public int getSegmentsProcessed()
  {
    return segmentsProcessed;
  }

  @JsonProperty
  public int getSegmentsVectorProcessed()
  {
    return segmentsVectorProcessed;
  }

  @JsonProperty
  public long getSegmentRows()
  {
    return segmentRows;
  }

  @JsonProperty
  public long getNodeRows()
  {
    return nodeRows;
  }

  @JsonProperty
  public long getPreFilteredRows()
  {
    return preFilteredRows;
  }

  @JsonProperty
  public long getResultRows()
  {
    return resultRows;
  }

  @JsonProperty
  public long getCpuNanos()
  {
    return cpuNanos;
  }

  // Comment about how this works with snapshots and serde etc. it's a little tricky/weird
  @JsonProperty("threads")
  public int getThreadsSnapshot()
  {
    return threads + threadAccumulator.size();
  }

  public int getThreads()
  {
    return threads;
  }

  @JsonProperty
  public long getQueryStart()
  {
    return queryStart;
  }

  @JsonProperty
  public long getQueryMs()
  {
    return queryMs;
  }

  public SingleQueryMetricsCollector setSubQueryId(@Nullable final String subQueryId)
  {
    this.subQueryId = subQueryId;
    return this;
  }

  public SingleQueryMetricsCollector addCurrentThread()
  {
    threadAccumulator.add(Thread.currentThread().getId());
    return this;
  }

  public SingleQueryMetricsCollector addSegment()
  {
    this.segments++;
    return this;
  }

  public SingleQueryMetricsCollector addSegmentProcessed()
  {
    this.segmentsProcessed++;
    return this;
  }

  public SingleQueryMetricsCollector addSegmentVectorProcessed()
  {
    this.segmentsVectorProcessed++;
    return this;
  }

  public SingleQueryMetricsCollector addSegmentRows(final long numRows)
  {
    this.segmentRows += numRows;
    return this;
  }

  public SingleQueryMetricsCollector addPreFilteredRows(final long numRows)
  {
    this.preFilteredRows += numRows;
    return this;
  }

  public SingleQueryMetricsCollector addNodeRows(final long numRows)
  {
    this.nodeRows += numRows;
    return this;
  }

  public SingleQueryMetricsCollector setResultRows(final long resultRows)
  {
    this.resultRows = resultRows;
    return this;
  }

  public SingleQueryMetricsCollector addCpuNanos(final long timeNs)
  {
    this.cpuNanos += timeNs;
    return this;
  }

  public SingleQueryMetricsCollector setQueryStart(final long queryStart)
  {
    this.queryStart = queryStart;
    return this;
  }

  public SingleQueryMetricsCollector setQueryMs(final long queryMs)
  {
    this.queryMs = queryMs;
    return this;
  }

  public SingleQueryMetricsCollector add(final SingleQueryMetricsCollector other)
  {
    if (!Objects.equals(subQueryId, other.subQueryId)) {
      // Sanity check
      throw new IAE("Cannot merge collectors for different queries");
    }

    this.segments += other.segments;
    this.segmentsProcessed += other.segmentsProcessed;
    this.segmentsVectorProcessed += other.segmentsVectorProcessed;
    this.segmentRows += other.segmentRows;
    this.preFilteredRows += other.preFilteredRows;
    this.nodeRows += other.nodeRows;
    this.resultRows = other.resultRows > 0 ? other.resultRows : resultRows;
    this.cpuNanos += other.cpuNanos;
    this.threads += other.threads;
    this.threadAccumulator.addAll(other.threadAccumulator);
    this.queryStart = other.queryStart > 0 ? other.queryStart : queryStart;
    this.queryMs = other.queryMs > 0 ? other.queryMs : queryMs;
    return this;
  }
}
