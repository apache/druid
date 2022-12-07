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

import com.google.common.collect.ImmutableSortedMap;

import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;

/**
 * Class maintained by the controller to merge {@link PartialKeyStatisticsInformation} sent by the worker.
 */
public class CompleteKeyStatisticsInformation
{
  private final SortedMap<Long, Set<Integer>> timeSegmentVsWorkerMap;

  private boolean multipleValues;

  private double bytesRetained;

  public CompleteKeyStatisticsInformation(
      final SortedMap<Long, Set<Integer>> timeChunks,
      boolean multipleValues,
      double bytesRetained
  )
  {
    this.timeSegmentVsWorkerMap = timeChunks;
    this.multipleValues = multipleValues;
    this.bytesRetained = bytesRetained;
  }

  /**
   * Merges the {@link PartialKeyStatisticsInformation} into the complete key statistics information object.
   * {@link #timeSegmentVsWorkerMap} is updated in sorted order with the timechunks from
   * {@param partialKeyStatisticsInformation}, {@link #multipleValues} is set to true if
   * {@param partialKeyStatisticsInformation} contains multipleValues and the bytes retained by the partial sketch
   * is added to {@link #bytesRetained}.
   */
  public void mergePartialInformation(int workerNumber, PartialKeyStatisticsInformation partialKeyStatisticsInformation)
  {
    for (Long timeSegment : partialKeyStatisticsInformation.getTimeSegments()) {
      this.timeSegmentVsWorkerMap
          .computeIfAbsent(timeSegment, key -> new HashSet<>())
          .add(workerNumber);
    }
    this.multipleValues = this.multipleValues || partialKeyStatisticsInformation.hasMultipleValues();
    this.bytesRetained += bytesRetained;
  }

  public SortedMap<Long, Set<Integer>> getTimeSegmentVsWorkerMap()
  {
    return ImmutableSortedMap.copyOfSorted(timeSegmentVsWorkerMap);
  }

  public boolean hasMultipleValues()
  {
    return multipleValues;
  }

  public double getBytesRetained()
  {
    return bytesRetained;
  }
}
