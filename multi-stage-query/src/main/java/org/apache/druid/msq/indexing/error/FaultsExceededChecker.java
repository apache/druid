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

package org.apache.druid.msq.indexing.error;

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.msq.counters.CounterNames;
import org.apache.druid.msq.counters.CounterSnapshots;
import org.apache.druid.msq.counters.CounterSnapshotsTree;
import org.apache.druid.msq.counters.WarningCounters;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Keeps a track of the warnings that have been so far and returns if any type has exceeded their designated limit
 * This class is thread safe
 */
public class FaultsExceededChecker
{
  final Map<String, Long> maxFaultsAllowedCount;

  public FaultsExceededChecker(final Map<String, Long> maxFaultsAllowedCount)
  {
    maxFaultsAllowedCount.forEach(
        (warning, count) ->
            Preconditions.checkArgument(
                count >= 0 || count == -1,
                StringUtils.format("Invalid limit of %d supplied for warnings of type %s. "
                                   + "Limit can be greater than or equal to -1.", count, warning)
            )
    );
    this.maxFaultsAllowedCount = maxFaultsAllowedCount;
  }

  /**
   * @param snapshotsTree WorkerCounters have the count of the warnings generated per worker
   *
   * @return An optional which is empty if the faults count in the present in the task counters don't exceed their
   * prescribed limit, else it contains the errorCode and the maximum allowed faults for that errorCode
   */
  public Optional<Pair<String, Long>> addFaultsAndCheckIfExceeded(CounterSnapshotsTree snapshotsTree)
  {
    final Map<Integer, Map<Integer, CounterSnapshots>> snapshotsMap = snapshotsTree.copyMap();

    Map<String, Long> allWarnings = new HashMap<>();
    for (Map.Entry<Integer, Map<Integer, CounterSnapshots>> stageEntry : snapshotsMap.entrySet()) {
      for (Map.Entry<Integer, CounterSnapshots> workerEntry : stageEntry.getValue().entrySet()) {
        final WarningCounters.Snapshot warningsSnapshot =
            (WarningCounters.Snapshot) workerEntry.getValue().getMap().get(CounterNames.warnings());

        if (warningsSnapshot != null) {
          for (Map.Entry<String, Long> entry : warningsSnapshot.getWarningCountMap().entrySet()) {
            allWarnings.compute(
                entry.getKey(),
                (ignored, value) -> value == null ? entry.getValue() : value + entry.getValue()
            );
          }
        }
      }
    }

    for (Map.Entry<String, Long> totalWarningCountEntry : allWarnings.entrySet()) {
      long limit = maxFaultsAllowedCount.getOrDefault(totalWarningCountEntry.getKey(), -1L);
      boolean passed = limit == -1 || totalWarningCountEntry.getValue() <= limit;
      if (!passed) {
        return Optional.of(Pair.of(totalWarningCountEntry.getKey(), limit));
      }
    }
    return Optional.empty();
  }

}
