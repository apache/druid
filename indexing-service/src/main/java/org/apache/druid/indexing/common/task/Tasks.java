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

package org.apache.druid.indexing.common.task;

import org.apache.curator.shaded.com.google.common.base.Verify;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.duty.CompactSegments;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

public class Tasks
{
  public static final int DEFAULT_REALTIME_TASK_PRIORITY = 75;
  public static final int DEFAULT_BATCH_INDEX_TASK_PRIORITY = 50;
  public static final int DEFAULT_MERGE_TASK_PRIORITY = 25;

  static {
    Verify.verify(DEFAULT_MERGE_TASK_PRIORITY == DataSourceCompactionConfig.DEFAULT_COMPACTION_TASK_PRIORITY);
  }

  public static final int DEFAULT_TASK_PRIORITY = 0;
  public static final long DEFAULT_LOCK_TIMEOUT_MILLIS = TimeUnit.MINUTES.toMillis(5);
  public static final boolean DEFAULT_FORCE_TIME_CHUNK_LOCK = true;
  public static final boolean DEFAULT_STORE_COMPACTION_STATE = false;
  public static final boolean DEFAULT_USE_MAX_MEMORY_ESTIMATES = false;
  public static final TaskLockType DEFAULT_TASK_LOCK_TYPE = TaskLockType.EXCLUSIVE;
  public static final boolean DEFAULT_USE_CONCURRENT_LOCKS = false;

  public static final String PRIORITY_KEY = "priority";
  public static final String LOCK_TIMEOUT_KEY = "taskLockTimeout";
  public static final String FORCE_TIME_CHUNK_LOCK_KEY = "forceTimeChunkLock";
  public static final String STORE_EMPTY_COLUMNS_KEY = "storeEmptyColumns";
  public static final String USE_SHARED_LOCK = "useSharedLock";
  public static final String TASK_LOCK_TYPE = "taskLockType";
  public static final String USE_CONCURRENT_LOCKS = "useConcurrentLocks";

  /**
   * Context flag denoting if maximum possible values should be used to estimate
   * on-heap memory usage while indexing. Refer to OnHeapIncrementalIndex for
   * more details.
   *
   * The value of this flag is true by default which corresponds to the old method
   * of estimation.
   */
  public static final String USE_MAX_MEMORY_ESTIMATES = "useMaxMemoryEstimates";

  /**
   * This context is used in compaction. When it is set in the context, the segments created by the task
   * will fill 'lastCompactionState' in its metadata. This will be used to track what segments are compacted or not.
   * See {@link org.apache.druid.timeline.DataSegment} and {@link
   * org.apache.druid.server.coordinator.compact.NewestSegmentFirstIterator} for more details.
   */
  public static final String STORE_COMPACTION_STATE_KEY = "storeCompactionState";

  static {
    Verify.verify(STORE_COMPACTION_STATE_KEY.equals(CompactSegments.STORE_COMPACTION_STATE_KEY));
  }

  public static SortedSet<Interval> computeCondensedIntervals(SortedSet<Interval> intervals)
  {
    final SortedSet<Interval> condensedIntervals = new TreeSet<>(Comparators.intervalsByStartThenEnd());
    List<Interval> toBeAccumulated = new ArrayList<>();
    for (Interval interval : intervals) {
      if (toBeAccumulated.size() == 0) {
        toBeAccumulated.add(interval);
      } else {
        if (toBeAccumulated.get(toBeAccumulated.size() - 1).abuts(interval)) {
          toBeAccumulated.add(interval);
        } else {
          condensedIntervals.add(JodaUtils.umbrellaInterval(toBeAccumulated));
          toBeAccumulated.clear();
          toBeAccumulated.add(interval);
        }
      }
    }
    if (toBeAccumulated.size() > 0) {
      condensedIntervals.add(JodaUtils.umbrellaInterval(toBeAccumulated));
    }
    return condensedIntervals;
  }
}
