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
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Counters for warnings. Created by {@link CounterTracker#warnings()}.
 */
@JsonTypeName("warnings")
public class WarningCounters implements QueryCounter
{
  private final ConcurrentHashMap<String, Long> warningCodeCounter = new ConcurrentHashMap<>();

  public void incrementWarningCount(String errorCode)
  {
    warningCodeCounter.compute(errorCode, (ignored, oldCount) -> oldCount == null ? 1 : oldCount + 1);
  }

  @Override
  @Nullable
  public Snapshot snapshot()
  {
    if (warningCodeCounter.isEmpty()) {
      return null;
    }

    final Map<String, Long> countCopy = ImmutableMap.copyOf(warningCodeCounter);
    return new Snapshot(countCopy);
  }

  @JsonTypeName("warnings")
  public static class Snapshot implements QueryCounterSnapshot
  {
    private final Map<String, Long> warningCountMap;

    @JsonCreator
    public Snapshot(Map<String, Long> warningCountMap)
    {
      this.warningCountMap = Preconditions.checkNotNull(warningCountMap, "warningCountMap");
    }

    @JsonValue
    public Map<String, Long> getWarningCountMap()
    {
      return warningCountMap;
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
      return Objects.equals(warningCountMap, snapshot.warningCountMap);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(warningCountMap);
    }
  }
}
