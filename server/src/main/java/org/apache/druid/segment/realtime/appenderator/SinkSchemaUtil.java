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

package org.apache.druid.segment.realtime.appenderator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.timeline.SegmentId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Utility to compute schema for all sinks in a streaming ingestion task, used in {@link StreamAppenderator}.
 */
class SinkSchemaUtil
{
  /**
   * Compute {@link SegmentSchemas} for the sinks.
   */
  @VisibleForTesting
  static Optional<SegmentSchemas> computeAbsoluteSchema(
      Map<SegmentIdWithShardSpec, Pair<Map<String, ColumnType>, Integer>> currentSinkSchema
  )
  {
    Map<Integer, SegmentSchemas.ColumnInformation> columnMapping = new HashMap<>();
    List<SegmentSchemas.SegmentSchema> sinkSchemas = new ArrayList<>();

    Map<String, Integer> columnReverseMapping = new HashMap<>();
    AtomicInteger columnCount = new AtomicInteger();

    for (Map.Entry<SegmentIdWithShardSpec, Pair<Map<String, ColumnType>, Integer>> sinkDimensionsEntry : currentSinkSchema.entrySet()) {
      SegmentIdWithShardSpec segmentIdWithShardSpec = sinkDimensionsEntry.getKey();
      Map<String, ColumnType> sinkDimensions = sinkDimensionsEntry.getValue().lhs;
      Integer numRows = sinkDimensionsEntry.getValue().rhs;

      List<Integer> newColumns = Lists.newLinkedList();

      // new Sink
      for (Map.Entry<String, ColumnType> entry : sinkDimensions.entrySet()) {
        String column = entry.getKey();
        ColumnType columnType = entry.getValue();
        Integer columnNumber = columnReverseMapping.computeIfAbsent(column, v -> columnCount.getAndIncrement());
        columnMapping.computeIfAbsent(columnNumber, v -> new SegmentSchemas.ColumnInformation(column, columnType));
        newColumns.add(columnNumber);
      }

      if (newColumns.size() > 0) {
        SegmentId segmentId = segmentIdWithShardSpec.asSegmentId();
        SegmentSchemas.SegmentSchema segmentSchema =
            new SegmentSchemas.SegmentSchema(
                segmentId.getDataSource(),
                segmentId.toString(),
                false,
                numRows,
                newColumns,
                Collections.emptyList()
            );
        sinkSchemas.add(segmentSchema);
      }
    }

    return Optional.ofNullable(sinkSchemas.isEmpty() ? null : new SegmentSchemas(columnMapping, sinkSchemas));
  }

  /**
   * Compute schema change for the sinks.
   */
  @VisibleForTesting
  static Optional<SegmentSchemas> computeSchemaChange(
      Map<SegmentIdWithShardSpec, Pair<Map<String, ColumnType>, Integer>> previousSinkSchema,
      Map<SegmentIdWithShardSpec, Pair<Map<String, ColumnType>, Integer>> currentSinkSchema
  )
  {
    Map<Integer, SegmentSchemas.ColumnInformation> columnMapping = new HashMap<>();
    List<SegmentSchemas.SegmentSchema> sinkSchemas = new ArrayList<>();

    Map<String, Integer> columnReverseMapping = new HashMap<>();
    AtomicInteger columnCount = new AtomicInteger();

    for (Map.Entry<SegmentIdWithShardSpec, Pair<Map<String, ColumnType>, Integer>> sinkDimensionsEntry : currentSinkSchema.entrySet()) {
      SegmentIdWithShardSpec segmentIdWithShardSpec = sinkDimensionsEntry.getKey();
      Map<String, ColumnType> sinkDimensions = sinkDimensionsEntry.getValue().lhs;
      Integer numRows = sinkDimensionsEntry.getValue().rhs;

      List<Integer> newColumns = Lists.newLinkedList();
      List<Integer> updatedColumns = Lists.newLinkedList();

      boolean changed = false;
      boolean delta = false;

      if (!previousSinkSchema.containsKey(segmentIdWithShardSpec)) {
        // new Sink
        for (Map.Entry<String, ColumnType> entry : sinkDimensions.entrySet()) {
          String column = entry.getKey();
          ColumnType columnType = entry.getValue();
          Integer columnNumber = columnReverseMapping.computeIfAbsent(column, v -> columnCount.getAndIncrement());
          columnMapping.computeIfAbsent(columnNumber, v -> new SegmentSchemas.ColumnInformation(column, columnType));
          newColumns.add(columnNumber);
        }
        if (newColumns.size() > 0 || numRows > 0) {
          changed = true;
        }
      } else {
        Map<String, ColumnType> prevSinkDimensions = previousSinkSchema.get(segmentIdWithShardSpec).lhs;
        Integer prevNumRows = previousSinkSchema.get(segmentIdWithShardSpec).rhs;
        for (Map.Entry<String, ColumnType> entry : sinkDimensions.entrySet()) {
          String column = entry.getKey();
          ColumnType columnType = entry.getValue();

          if (!prevSinkDimensions.containsKey(column)) {
            Integer columnNumber = columnReverseMapping.computeIfAbsent(column, v -> columnCount.getAndIncrement());
            columnMapping.computeIfAbsent(columnNumber, v -> new SegmentSchemas.ColumnInformation(column, columnType));
            newColumns.add(columnNumber);
          } else if (!prevSinkDimensions.get(column).equals(columnType)) {
            Integer columnNumber = columnReverseMapping.computeIfAbsent(column, v -> columnCount.getAndIncrement());
            columnMapping.computeIfAbsent(columnNumber, v -> new SegmentSchemas.ColumnInformation(column, columnType));
            updatedColumns.add(columnNumber);
          }
        }
        if ((!Objects.equals(numRows, prevNumRows)) || (updatedColumns.size() > 0) || (newColumns.size() > 0)) {
          changed = true;
          delta = true;
        }
      }

      if (changed) {
        SegmentId segmentId = segmentIdWithShardSpec.asSegmentId();
        SegmentSchemas.SegmentSchema segmentSchema =
            new SegmentSchemas.SegmentSchema(
                segmentId.getDataSource(),
                segmentId.toString(),
                delta,
                numRows,
                newColumns,
                updatedColumns
            );
        sinkSchemas.add(segmentSchema);
      }
    }

    return Optional.ofNullable(sinkSchemas.isEmpty() ? null : new SegmentSchemas(columnMapping, sinkSchemas));
  }
}
