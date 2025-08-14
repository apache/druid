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

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class CpuCounters implements QueryCounter
{
  public static final String LABEL_MAIN = "main";
  public static final String LABEL_KEY_STATISTICS = "collectKeyStatistics";
  public static final String LABEL_MERGE_INPUT = "mergeInput";
  public static final String LABEL_HASH_PARTITION = "hashPartitionOutput";
  public static final String LABEL_MIX = "mixOutput";
  public static final String LABEL_SORT = "sortOutput";

  private final ConcurrentHashMap<String, CpuCounter> counters = new ConcurrentHashMap<>();

  public CpuCounter forName(final String name)
  {
    return counters.computeIfAbsent(name, k -> new CpuCounter());
  }

  @Nullable
  @Override
  public CpuCounters.Snapshot snapshot()
  {
    final Map<String, CpuCounter.Snapshot> snapshotMap = new HashMap<>();
    for (Map.Entry<String, CpuCounter> entry : counters.entrySet()) {
      snapshotMap.put(entry.getKey(), entry.getValue().snapshot());
    }
    return new Snapshot(snapshotMap);
  }

  @JsonTypeName("cpus")
  public static class Snapshot implements QueryCounterSnapshot
  {
    // String keys, not enum, so deserialization is forwards-compatible
    private final Map<String, CpuCounter.Snapshot> map;

    @JsonCreator
    public Snapshot(Map<String, CpuCounter.Snapshot> map)
    {
      this.map = Preconditions.checkNotNull(map, "map");
    }

    @JsonValue
    public Map<String, CpuCounter.Snapshot> getCountersMap()
    {
      return map;
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
      return Objects.equals(map, snapshot.map);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(map);
    }

    @Override
    public String toString()
    {
      return "CpuCounters.Snapshot{" +
             "map=" + map +
             '}';
    }
  }
}
