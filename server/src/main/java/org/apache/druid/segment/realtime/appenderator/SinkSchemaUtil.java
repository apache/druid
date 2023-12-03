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
import org.apache.druid.segment.realtime.appenderator.SegmentSchemas.SegmentSchema;
import org.apache.druid.timeline.SegmentId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

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
      Map<SegmentId, Pair<Map<String, ColumnType>, Integer>> sinkSchemaMap
  )
  {
    List<SegmentSchema> sinkSchemas = new ArrayList<>();

    for (Map.Entry<SegmentId, Pair<Map<String, ColumnType>, Integer>> entry : sinkSchemaMap.entrySet()) {
      SegmentId segmentId = entry.getKey();
      Map<String, ColumnType> sinkColumnMap = entry.getValue().lhs;

      List<String> newColumns = Lists.newLinkedList();

      Map<String, ColumnType> columnMapping = new HashMap<>();

      // new Sink
      for (Map.Entry<String, ColumnType> columnAndType : sinkColumnMap.entrySet()) {
        String column = columnAndType.getKey();
        ColumnType columnType = columnAndType.getValue();
        columnMapping.put(column, columnType);
        newColumns.add(column);
      }

      Integer numRows = entry.getValue().rhs;
      if (newColumns.size() > 0) {
        SegmentSchema segmentSchema =
            new SegmentSchema(
                segmentId.getDataSource(),
                segmentId.toString(),
                false,
                numRows,
                newColumns,
                Collections.emptyList(),
                columnMapping
            );
        sinkSchemas.add(segmentSchema);
      }
    }

    return Optional.ofNullable(sinkSchemas.isEmpty() ? null : new SegmentSchemas(sinkSchemas));
  }

  /**
   * Compute schema change for the sinks.
   */
  @VisibleForTesting
  static Optional<SegmentSchemas> computeSchemaChange(
      Map<SegmentId, Pair<Map<String, ColumnType>, Integer>> previousSinkSchemaMap,
      Map<SegmentId, Pair<Map<String, ColumnType>, Integer>> currentSinkSchemaMap
  )
  {
    List<SegmentSchema> sinkSchemas = new ArrayList<>();

    for (Map.Entry<SegmentId, Pair<Map<String, ColumnType>, Integer>> entry : currentSinkSchemaMap.entrySet()) {
      SegmentId segmentId = entry.getKey();
      Map<String, ColumnType> sinkDimensions = entry.getValue().lhs;

      Integer numRows = entry.getValue().rhs;

      List<String> newColumns = Lists.newLinkedList();
      List<String> updatedColumns = Lists.newLinkedList();
      Map<String, ColumnType> columnMapping = new HashMap<>();

      boolean update = false;
      boolean delta = false;

      if (!previousSinkSchemaMap.containsKey(segmentId)) {
        // new Sink
        for (Map.Entry<String, ColumnType> columnAndType : sinkDimensions.entrySet()) {
          String column = columnAndType.getKey();
          ColumnType columnType = columnAndType.getValue();
          columnMapping.put(column, columnType);
          newColumns.add(column);
        }
        if (newColumns.size() > 0 || numRows > 0) {
          update = true;
        }
      } else {
        Map<String, ColumnType> previousSinkDimensions = previousSinkSchemaMap.get(segmentId).lhs;
        Integer previousNumRows = previousSinkSchemaMap.get(segmentId).rhs;
        for (Map.Entry<String, ColumnType> columnAndType : sinkDimensions.entrySet()) {
          String column = columnAndType.getKey();
          ColumnType columnType = columnAndType.getValue();

          columnMapping.put(column, columnType);
          if (!previousSinkDimensions.containsKey(column)) {
            newColumns.add(column);
          } else if (!previousSinkDimensions.get(column).equals(columnType)) {
            updatedColumns.add(column);
          }
        }

        if ((!Objects.equals(numRows, previousNumRows)) || (updatedColumns.size() > 0) || (newColumns.size() > 0)) {
          update = true;
          delta = true;
        }
      }

      if (update) {
        SegmentSchema segmentSchema =
            new SegmentSchema(
                segmentId.getDataSource(),
                segmentId.toString(),
                delta,
                numRows,
                newColumns,
                updatedColumns,
                columnMapping
            );
        sinkSchemas.add(segmentSchema);
      }
    }

    return Optional.ofNullable(sinkSchemas.isEmpty() ? null : new SegmentSchemas(sinkSchemas));
  }
}
