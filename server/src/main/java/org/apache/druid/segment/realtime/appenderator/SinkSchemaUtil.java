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
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.realtime.appenderator.SegmentSchemas.SegmentSchema;
import org.apache.druid.timeline.SegmentId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

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
      Map<SegmentId, Pair<RowSignature, Integer>> sinkSchemaMap
  )
  {
    List<SegmentSchema> sinkSchemas = new ArrayList<>();

    for (Map.Entry<SegmentId, Pair<RowSignature, Integer>> entry : sinkSchemaMap.entrySet()) {
      SegmentId segmentId = entry.getKey();
      RowSignature sinkSignature = entry.getValue().lhs;

      List<String> newColumns = new ArrayList<>();

      Map<String, ColumnType> columnMapping = new HashMap<>();

      // new Sink
      for (String column : sinkSignature.getColumnNames()) {
        newColumns.add(column);
        sinkSignature.getColumnType(column).ifPresent(type -> columnMapping.put(column, type));
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
      Map<SegmentId, Pair<RowSignature, Integer>> previousSinkSignatureMap,
      Map<SegmentId, Pair<RowSignature, Integer>> currentSinkSignatureMap
  )
  {
    List<SegmentSchema> sinkSchemas = new ArrayList<>();

    for (Map.Entry<SegmentId, Pair<RowSignature, Integer>> entry : currentSinkSignatureMap.entrySet()) {
      SegmentId segmentId = entry.getKey();
      RowSignature currentSinkSignature = entry.getValue().lhs;

      Integer numRows = entry.getValue().rhs;

      List<String> newColumns = new ArrayList<>();
      List<String> updatedColumns = new ArrayList<>();
      Map<String, ColumnType> currentColumnMapping = new HashMap<>();

      // whether there are any changes to be published
      boolean shouldPublish = false;
      // if the resultant schema is delta
      boolean isDelta = false;

      if (!previousSinkSignatureMap.containsKey(segmentId)) {
        // new Sink
        for (String column : currentSinkSignature.getColumnNames()) {
          newColumns.add(column);
          currentSinkSignature.getColumnType(column).ifPresent(type -> currentColumnMapping.put(column, type));
        }
        if (newColumns.size() > 0) {
          shouldPublish = true;
        }
      } else {
        RowSignature previousSinkSignature = previousSinkSignatureMap.get(segmentId).lhs;
        Set<String> previousSinkDimensions = new HashSet<>(previousSinkSignature.getColumnNames());

        Integer previousNumRows = previousSinkSignatureMap.get(segmentId).rhs;
        for (String column : currentSinkSignature.getColumnNames()) {
          boolean added = false;
          if (!previousSinkDimensions.contains(column)) {
            newColumns.add(column);
            added = true;
          } else if (!Objects.equals(previousSinkSignature.getColumnType(column), currentSinkSignature.getColumnType(column))) {
            updatedColumns.add(column);
            added = true;
          }

          if (added) {
            currentSinkSignature.getColumnType(column).ifPresent(type -> currentColumnMapping.put(column, type));
          }
        }

        if ((!Objects.equals(numRows, previousNumRows)) || (updatedColumns.size() > 0) || (newColumns.size() > 0)) {
          shouldPublish = true;
          isDelta = true;
        }
      }

      if (shouldPublish) {
        SegmentSchema segmentSchema =
            new SegmentSchema(
                segmentId.getDataSource(),
                segmentId.toString(),
                isDelta,
                numRows,
                newColumns,
                updatedColumns,
                currentColumnMapping
            );
        sinkSchemas.add(segmentSchema);
      }
    }

    return Optional.ofNullable(sinkSchemas.isEmpty() ? null : new SegmentSchemas(sinkSchemas));
  }
}
