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

import org.apache.druid.frame.processor.SuperSorterProgressTracker;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Class that tracks query counters for a particular worker in a particular stage.
 *
 * Counters are all tracked on a (stage, worker, counter) basis by the {@link #countersMap} object.
 *
 * Immutable {@link CounterSnapshots} snapshots can be created by {@link #snapshot()}.
 */
public class CounterTracker
{
  private final ConcurrentHashMap<String, QueryCounter> countersMap = new ConcurrentHashMap<>();

  public ChannelCounters channel(final String name)
  {
    return counter(name, ChannelCounters::new);
  }

  public SuperSorterProgressTracker sortProgress()
  {
    return counter(CounterNames.sortProgress(), SuperSorterProgressTrackerCounter::new).tracker();
  }

  public WarningCounters warnings()
  {
    return counter(CounterNames.warnings(), WarningCounters::new);
  }

  @SuppressWarnings("unchecked")
  public <T extends QueryCounter> T counter(final String counterName, final Supplier<T> newCounterFn)
  {
    return (T) countersMap.computeIfAbsent(counterName, ignored -> newCounterFn.get());
  }

  public CounterSnapshots snapshot()
  {
    final Map<String, QueryCounterSnapshot> m = new HashMap<>();

    for (final Map.Entry<String, QueryCounter> entry : countersMap.entrySet()) {
      final QueryCounterSnapshot counterSnapshot = entry.getValue().snapshot();
      if (counterSnapshot != null) {
        m.put(entry.getKey(), counterSnapshot);
      }
    }

    return new CounterSnapshots(m);
  }
}
