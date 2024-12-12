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

package org.apache.druid.indexer.report;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class KillTaskReport implements TaskReport
{
  public static final String REPORT_KEY = "killUnusedSegments";

  private final String taskId;
  private final Stats stats;

  @JsonCreator
  public KillTaskReport(
      @JsonProperty("taskId") String taskId,
      @JsonProperty("payload") Stats stats
  )
  {
    this.taskId = taskId;
    this.stats = stats;
  }

  @Override
  @JsonProperty
  public String getTaskId()
  {
    return taskId;
  }

  @Override
  public String getReportKey()
  {
    return REPORT_KEY;
  }

  @Override
  @JsonProperty
  public Object getPayload()
  {
    return stats;
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
    KillTaskReport that = (KillTaskReport) o;
    return Objects.equals(taskId, that.taskId) && Objects.equals(stats, that.stats);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(taskId, stats);
  }

  public static class Stats
  {
    private final int numSegmentsKilled;
    private final int numBatchesProcessed;

    @JsonCreator
    public Stats(
        @JsonProperty("numSegmentsKilled") int numSegmentsKilled,
        @JsonProperty("numBatchesProcessed") int numBatchesProcessed
    )
    {
      this.numSegmentsKilled = numSegmentsKilled;
      this.numBatchesProcessed = numBatchesProcessed;
    }

    @JsonProperty
    public int getNumSegmentsKilled()
    {
      return numSegmentsKilled;
    }

    @JsonProperty
    public int getNumBatchesProcessed()
    {
      return numBatchesProcessed;
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
      Stats that = (Stats) o;
      return numSegmentsKilled == that.numSegmentsKilled
             && numBatchesProcessed == that.numBatchesProcessed;
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(numSegmentsKilled, numBatchesProcessed);
    }

    @Override
    public String toString()
    {
      return "Stats{" +
             "numSegmentsKilled=" + numSegmentsKilled +
             ", numBatchesProcessed=" + numBatchesProcessed +
             '}';
    }
  }
}
