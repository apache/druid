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

import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.frame.processor.SuperSorterProgressTracker;
import org.apache.druid.frame.processor.manager.ProcessorManager;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.QueryContext;
import org.apache.druid.utils.JvmUtils;

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

  /**
   * See {@link MultiStageQueryContext#getIncludeAllCounters(QueryContext)}.
   */
  private final boolean includeAllCounters;

  public CounterTracker(boolean includeAllCounters)
  {
    this.includeAllCounters = includeAllCounters;
  }

  public ChannelCounters channel(final String name)
  {
    return counter(name, ChannelCounters::new);
  }

  /**
   * Returns a {@link CpuCounter} that can be used to accumulate CPU time under a particular label.
   */
  public CpuCounter cpu(final String name)
  {
    return counter(CounterNames.cpu(), CpuCounters::new).forName(name);
  }

  /**
   * Decorates a {@link FrameProcessor} such that it accumulates CPU time under a particular label.
   */
  public <T> FrameProcessor<T> trackCpu(final FrameProcessor<T> processor, final String name)
  {
    if (JvmUtils.isThreadCpuTimeEnabled()) {
      final CpuCounter counter = counter(CounterNames.cpu(), CpuCounters::new).forName(name);
      return new CpuTimeAccumulatingFrameProcessor<>(processor, counter);
    } else {
      return processor;
    }
  }

  /**
   * Decorates a {@link ProcessorManager} such that it accumulates CPU time under a particular label.
   */
  public <T, R> ProcessorManager<T, R> trackCpu(final ProcessorManager<T, R> processorManager, final String name)
  {
    if (JvmUtils.isThreadCpuTimeEnabled()) {
      final CpuCounter counter = counter(CounterNames.cpu(), CpuCounters::new).forName(name);
      return new CpuTimeAccumulatingProcessorManager<>(processorManager, counter);
    } else {
      return processorManager;
    }
  }

  public SuperSorterProgressTracker sortProgress()
  {
    return counter(CounterNames.sortProgress(), SuperSorterProgressTrackerCounter::new).tracker();
  }

  public SegmentGenerationProgressCounter segmentGenerationProgress()
  {
    return counter(CounterNames.getSegmentGenerationProgress(), SegmentGenerationProgressCounter::new);
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
      if (counterSnapshot != null && (includeAllCounters || isLegacyCounter(counterSnapshot))) {
        m.put(entry.getKey(), counterSnapshot);
      }
    }

    return new CounterSnapshots(m);
  }

  /**
   * Returns whether a counter is a "legacy counter" that can be snapshotted regardless of the value of
   * {@link MultiStageQueryContext#getIncludeAllCounters(QueryContext)}.
   */
  private static boolean isLegacyCounter(final QueryCounterSnapshot counterSnapshot)
  {
    return counterSnapshot instanceof ChannelCounters.Snapshot
        || counterSnapshot instanceof SuperSorterProgressTrackerCounter.Snapshot
        || counterSnapshot instanceof WarningCounters.Snapshot
        || counterSnapshot instanceof SegmentGenerationProgressCounter.Snapshot;
  }
}
