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

package org.apache.druid.indexing.common;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.partition.PartitionChunk;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ReingestionTimelineUtils
{
  /**
   * @param timelineSegments A list of timeline objects, such as that returned by VersionedIntervalTimeline.lookup().
   * @param excludeDimensions Dimensions to be excluded
   * @return A list of all the unique dimension column names present in the segments within timelineSegments
   */
  public static List<String> getUniqueDimensions(
      List<TimelineObjectHolder<String, DataSegment>> timelineSegments,
      @Nullable Set<String> excludeDimensions
  )
  {
    final BiMap<String, Integer> uniqueDims = HashBiMap.create();

    // Here, we try to retain the order of dimensions as they were specified since the order of dimensions may be
    // optimized for performance.
    // Dimensions are extracted from the recent segments to olders because recent segments are likely to be queried more
    // frequently, and thus the performance should be optimized for recent ones rather than old ones.

    // timelineSegments are sorted in order of interval
    int index = 0;
    for (TimelineObjectHolder<String, DataSegment> timelineHolder : Lists.reverse(timelineSegments)) {
      for (PartitionChunk<DataSegment> chunk : timelineHolder.getObject()) {
        for (String dimension : chunk.getObject().getDimensions()) {
          if (!uniqueDims.containsKey(dimension) &&
              (excludeDimensions == null || !excludeDimensions.contains(dimension))) {
            uniqueDims.put(dimension, index++);
          }
        }
      }
    }

    final BiMap<Integer, String> orderedDims = uniqueDims.inverse();
    return IntStream.range(0, orderedDims.size())
                    .mapToObj(orderedDims::get)
                    .collect(Collectors.toList());
  }

  /**
   * @param timelineSegments A list of timeline objects, such as that returned by VersionedIntervalTimeline.lookup().
   * @return A list of all the unique metric column names present in the segments within timelineSegments
   */
  public static List<String> getUniqueMetrics(List<TimelineObjectHolder<String, DataSegment>> timelineSegments)
  {
    final BiMap<String, Integer> uniqueMetrics = HashBiMap.create();

    // Here, we try to retain the order of metrics as they were specified. Metrics are extracted from the recent
    // segments to olders.

    // timelineSegments are sorted in order of interval
    int[] index = {0};
    for (TimelineObjectHolder<String, DataSegment> timelineHolder : Lists.reverse(timelineSegments)) {
      for (PartitionChunk<DataSegment> chunk : timelineHolder.getObject()) {
        for (String metric : chunk.getObject().getMetrics()) {
          uniqueMetrics.computeIfAbsent(
              metric,
              k -> {
                return index[0]++;
              }
          );
        }
      }
    }

    final BiMap<Integer, String> orderedMetrics = uniqueMetrics.inverse();
    return IntStream.range(0, orderedMetrics.size())
                    .mapToObj(orderedMetrics::get)
                    .collect(Collectors.toList());
  }

  /**
   * Utility function to get dimensions that should be ingested. The preferred order is
   * - Explicit dimensions if they are provided.
   * - Custom dimensions are provided in the inputSpec.
   * - Calculate dimensions from the timeline but exclude any dimension exclusions.
   *
   * @param explicitDimensions sent as part of the re-ingestion InputSource.
   * @param dimensionsSpec from the provided ingestion spec.
   * @param timeLineSegments for the datasource that is being read.
   * @return
   */
  public static List<String> getDimensionsToReingest(
      @Nullable List<String> explicitDimensions,
      @NotNull DimensionsSpec dimensionsSpec,
      @NotNull List<TimelineObjectHolder<String, DataSegment>> timeLineSegments)
  {
    final List<String> dims;
    if (explicitDimensions != null) {
      dims = explicitDimensions;
    } else if (dimensionsSpec.hasCustomDimensions()) {
      dims = dimensionsSpec.getDimensionNames();
    } else {
      dims = ReingestionTimelineUtils.getUniqueDimensions(
          timeLineSegments,
          dimensionsSpec.getDimensionExclusions()
      );
    }
    return dims;
  }
}
