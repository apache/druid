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

package org.apache.druid.segment.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.RowAdapters;
import org.apache.druid.segment.RowBasedSegment;
import org.apache.druid.segment.UnnestCursorFactory;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A multi-row transform that unnests array columns during ingestion using the cursor-based unnest machinery.
 * Each input row is wrapped in a single-row segment, the unnest cursor iterates over array elements,
 * and each element becomes a separate output row.
 *
 * If the unnest column is missing or the array is empty, the input row passes through with the
 * unnest output column set to null.
 */
public class ScanTransform implements Transform
{
  private final String name;
  private final VirtualColumn unnestColumn;
  @Nullable
  private final DimFilter unnestFilter;

  @JsonCreator
  public ScanTransform(
      @JsonProperty("name") final String name,
      @JsonProperty("unnestColumn") final VirtualColumn unnestColumn,
      @JsonProperty("unnestFilter") @Nullable final DimFilter unnestFilter
  )
  {
    this.name = name;
    this.unnestColumn = unnestColumn;
    this.unnestFilter = unnestFilter;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Override
  @Nullable
  public RowFunction getRowFunction()
  {
    return null;
  }

  @JsonProperty
  public VirtualColumn getUnnestColumn()
  {
    return unnestColumn;
  }

  @JsonProperty
  @Nullable
  public DimFilter getUnnestFilter()
  {
    return unnestFilter;
  }

  @Override
  public Set<String> getRequiredColumns()
  {
    return Set.copyOf(unnestColumn.requiredColumns());
  }

  @Override
  public boolean isMultiRow()
  {
    return true;
  }

  @Override
  public List<InputRow> applyMultiRow(final InputRow inputRow)
  {
    final List<String> columns = getColumnsForProcessing(inputRow);
    final String unnestOutputName = unnestColumn.getOutputName();
    if (!columns.contains(unnestOutputName)) {
      columns.add(unnestOutputName);
    }

    final List<String> dimensionColumns = new ArrayList<>(inputRow.getDimensions());
    if (!dimensionColumns.contains(unnestOutputName)) {
      dimensionColumns.add(unnestOutputName);
    }

    final RowSignature.Builder signatureBuilder = RowSignature.builder();
    signatureBuilder.add(ColumnHolder.TIME_COLUMN_NAME, ColumnType.LONG);
    for (final String column : columns) {
      if (!ColumnHolder.TIME_COLUMN_NAME.equals(column)) {
        signatureBuilder.add(column, ColumnType.NESTED_DATA);
      }
    }
    final RowSignature inputSignature = signatureBuilder.build();

    final RowBasedSegment<InputRow> segment = new RowBasedSegment<>(
        Sequences.simple(List.of(inputRow)),
        RowAdapters.standardRow(),
        inputSignature
    );

    final CursorFactory baseCursorFactory = segment.as(CursorFactory.class);
    final CursorBuildSpec cursorBuildSpec = CursorBuildSpec.builder().setInterval(Intervals.ETERNITY).build();
    try (final CursorHolder cursorHolder = makeUnnestCursorFactory(baseCursorFactory, unnestFilter).makeCursorHolder(cursorBuildSpec)) {
      final Cursor cursor = cursorHolder.asCursor();
      if (cursor == null) {
        return List.of();
      }

      final ColumnSelectorFactory factory = cursor.getColumnSelectorFactory();
      final List<BaseObjectColumnValueSelector> selectors = new ArrayList<>(columns.size());
      for (final String column : columns) {
        selectors.add(factory.makeColumnValueSelector(column));
      }

      final List<InputRow> result = new ArrayList<>();

      while (!cursor.isDone()) {
        final Map<String, Object> event = new LinkedHashMap<>();
        for (int i = 0; i < columns.size(); i++) {
          final Object value = selectors.get(i).getObject();
          if (value != null) {
            event.put(columns.get(i), value);
          }
        }

        result.add(new MapBasedInputRow(inputRow.getTimestampFromEpoch(), dimensionColumns, event));
        cursor.advance();
      }

      if (result.isEmpty()) {
        if (unnestFilter != null && hasAnyUnnestValues(baseCursorFactory, cursorBuildSpec)) {
          return List.of();
        }

        final Map<String, Object> passthroughEvent = new LinkedHashMap<>();
        for (final String column : columns) {
          if (!ColumnHolder.TIME_COLUMN_NAME.equals(column)) {
            passthroughEvent.put(column, inputRow.getRaw(column));
          }
        }
        passthroughEvent.put(unnestOutputName, null);
        result.add(new MapBasedInputRow(inputRow.getTimestampFromEpoch(), dimensionColumns, passthroughEvent));
      }

      return result;
    }
  }

  private List<String> getColumnsForProcessing(final InputRow inputRow)
  {
    final LinkedHashSet<String> columns = new LinkedHashSet<>();
    columns.add(ColumnHolder.TIME_COLUMN_NAME);
    columns.addAll(inputRow.getDimensions());

    final MapBasedInputRow mapBasedInputRow = getMapBasedInputRow(inputRow);
    if (mapBasedInputRow != null) {
      columns.addAll(mapBasedInputRow.getEvent().keySet());
    }

    if (inputRow instanceof TransformedInputRow) {
      columns.addAll(((TransformedInputRow) inputRow).getTransformedColumns());
    }

    return new ArrayList<>(columns);
  }

  @Nullable
  private static MapBasedInputRow getMapBasedInputRow(final InputRow inputRow)
  {
    if (inputRow instanceof MapBasedInputRow) {
      return (MapBasedInputRow) inputRow;
    }
    if (inputRow instanceof TransformedInputRow) {
      return getMapBasedInputRow(((TransformedInputRow) inputRow).getBaseRow());
    }
    return null;
  }

  private UnnestCursorFactory makeUnnestCursorFactory(final CursorFactory baseCursorFactory, @Nullable final DimFilter filter)
  {
    return new UnnestCursorFactory(baseCursorFactory, unnestColumn, filter);
  }

  private boolean hasAnyUnnestValues(final CursorFactory baseCursorFactory, final CursorBuildSpec cursorBuildSpec)
  {
    try (final CursorHolder unfilteredCursorHolder = makeUnnestCursorFactory(baseCursorFactory, null).makeCursorHolder(cursorBuildSpec)) {
      final Cursor unfilteredCursor = unfilteredCursorHolder.asCursor();
      return unfilteredCursor != null && !unfilteredCursor.isDone();
    }
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ScanTransform that = (ScanTransform) o;
    return Objects.equals(name, that.name)
           && Objects.equals(unnestColumn, that.unnestColumn)
           && Objects.equals(unnestFilter, that.unnestFilter);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, unnestColumn, unnestFilter);
  }

  @Override
  public String toString()
  {
    return "ScanTransform{" +
           "name=" + name +
           ", unnestColumn=" + unnestColumn +
           ", unnestFilter=" + unnestFilter +
           '}';
  }
}
