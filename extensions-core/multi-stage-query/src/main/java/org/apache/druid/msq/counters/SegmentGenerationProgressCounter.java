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

package org.apache.druid.msq.counters;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.errorprone.annotations.concurrent.GuardedBy;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Counters for segment generation phase. Created by {@link CounterTracker#segmentGenerationProgress()}.
 */
public class SegmentGenerationProgressCounter implements QueryCounter
{
  @GuardedBy("this")
  private long rowsProcessed = 0L;

  public void incrementRowProcessedCount()
  {
    synchronized (this) {
      this.rowsProcessed += 1;
    }
  }

  @Override
  @Nullable
  public QueryCounterSnapshot snapshot()
  {
    synchronized (this) {
      return new Snapshot(rowsProcessed);
    }
  }

  @JsonTypeName("segmentGenerationProgress")
  public static class Snapshot implements QueryCounterSnapshot
  {
    private final long rowsProcessed;

    @JsonCreator
    public Snapshot(@JsonProperty("rowsProcessed") final long rowsProcessed)
    {
      this.rowsProcessed = rowsProcessed;
    }

    @JsonProperty(value = "rowsProcessed")
    public long getRowsProcessed()
    {
      return rowsProcessed;
    }

    @Override
    public String toString()
    {
      return "Snapshot{" +
             "rowsProcessed=" + rowsProcessed +
             '}';
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
      Snapshot snapshot = (Snapshot) o;
      return rowsProcessed == snapshot.rowsProcessed;
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(rowsProcessed);
    }
  }
}
