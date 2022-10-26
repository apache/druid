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

package org.apache.druid.msq.statistics;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;

/**
 * Class sent by worker to controller after reading input to generate partition boundries.
 */
public class WorkerAggregatedKeyStatistics
{
  private final SortedMap<Long, Set<Integer>> timeSegmentVsWorkerIdMap;

  private boolean hasMultipleValues;

  private double bytesRetained;

  @JsonCreator
  public WorkerAggregatedKeyStatistics(
      @JsonProperty("timeSegmentVsWorkerIdMap") final SortedMap<Long, Set<Integer>> timeChunks,
      @JsonProperty("hasMultipleValues") boolean hasMultipleValues,
      @JsonProperty("bytesRetained") double bytesRetained
  )
  {
    this.timeSegmentVsWorkerIdMap = timeChunks;
    this.hasMultipleValues = hasMultipleValues;
    this.bytesRetained = bytesRetained;
  }

  public void addAll(WorkerAggregatedKeyStatistics other)
  {
    for (Long timeChunk : other.timeSegmentVsWorkerIdMap.keySet()) {
      this.timeSegmentVsWorkerIdMap
          .computeIfAbsent(timeChunk, key -> new HashSet<>())
          .addAll(other.timeSegmentVsWorkerIdMap.get(timeChunk));
    }
    this.hasMultipleValues = this.hasMultipleValues || other.hasMultipleValues;
    this.bytesRetained += bytesRetained;
  }

  @JsonProperty("timeSegmentVsWorkerIdMap")
  public SortedMap<Long, Set<Integer>> getTimeSegmentVsWorkerIdMap()
  {
    return timeSegmentVsWorkerIdMap;
  }

  @JsonProperty("hasMultipleValues")
  public boolean isHasMultipleValues()
  {
    return hasMultipleValues;
  }

  @JsonProperty("bytesRetained")
  public double getBytesRetained()
  {
    return bytesRetained;
  }
}
