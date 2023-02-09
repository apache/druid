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
import com.google.common.collect.ImmutableSortedMap;

import java.util.Map;
import java.util.Objects;

/**
 * Named counter snapshots. Immutable. Often part of a {@link CounterSnapshotsTree}.
 */
public class CounterSnapshots
{
  private final Map<String, QueryCounterSnapshot> snapshotMap;

  @JsonCreator
  public CounterSnapshots(final Map<String, QueryCounterSnapshot> snapshotMap)
  {
    this.snapshotMap = ImmutableSortedMap.copyOf(snapshotMap, CounterNames.comparator());
  }

  public Map<String, QueryCounterSnapshot> getMap()
  {
    return snapshotMap;
  }

  public boolean isEmpty()
  {
    return snapshotMap.isEmpty();
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
    CounterSnapshots that = (CounterSnapshots) o;
    return Objects.equals(snapshotMap, that.snapshotMap);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(snapshotMap);
  }

  @Override
  public String toString()
  {
    return snapshotMap.toString();
  }
}
