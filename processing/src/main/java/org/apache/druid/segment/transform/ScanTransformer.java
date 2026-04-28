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

import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.UnnestDataSource;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentMapFunction;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * A {@link BaseTransformer} that processes input rows through a reusable scan query cursor pipeline.
 *
 * <p>The pipeline is built once at construction: a {@link SettableRowCursorFactory} is wrapped by the
 * scan query's {@link SegmentMapFunction} (e.g., unnest, filter). For each input row, the row is set
 * on the factory and the cursor is {@link Cursor#reset reset} — no per-row segment or cursor allocation.
 *
 * <p>When the scan query produces zero output rows (e.g., null/missing arrays, or filter rejection),
 * the input row passes through with unnest output columns set to null and virtual columns still evaluated.
 * This differs from {@link TransformSpec}'s filter behavior which drops the row entirely.
 *
 * <p>This class is not thread-safe. Each reader thread should have its own instance.
 */
public class ScanTransformer implements BaseTransformer
{
  private final ScanQuery query;
  private final SettableRowCursorFactory baseCursorFactory;
  private final CursorHolder cursorHolder;
  private Cursor cursor;

  ScanTransformer(final ScanQuery scanQuery)
  {
    this.query = scanQuery.withOverriddenContext(
        Map.of(QueryContexts.TIMEOUT_KEY, 0)
    );

    final RowSignature broadSignature = RowSignature.builder()
                                                     .add(ColumnHolder.TIME_COLUMN_NAME, ColumnType.LONG)
                                                     .build();

    final CursorBuildSpec cursorBuildSpec = CursorBuildSpec.builder()
                                                           .setInterval(query.getSingleInterval())
                                                           .setFilter(Filters.toFilter(query.getFilter()))
                                                           .setVirtualColumns(query.getVirtualColumns())
                                                           .build();

    this.baseCursorFactory = new SettableRowCursorFactory(broadSignature);
    final SegmentMapFunction segmentMapFunction = query.getDataSource().createSegmentMapFunction(query);
    final Segment mappedSegment = segmentMapFunction.apply(Optional.of(new CursorFactorySegment(baseCursorFactory)))
                                                    .orElseThrow(() -> new ISE("SegmentMapFunction returned empty"));
    final CursorFactory mappedCursorFactory = mappedSegment.as(CursorFactory.class);
    this.cursorHolder = mappedCursorFactory.makeCursorHolder(cursorBuildSpec);
  }

  @Override
  public boolean hasMultiRowTransform()
  {
    return true;
  }

  @Override
  @Nullable
  public InputRow transform(@Nullable final InputRow row)
  {
    throw new UnsupportedOperationException(
        "ScanTransformer does not support single-row transform; use transformToList()"
    );
  }

  @Override
  public List<InputRow> transformToList(@Nullable final InputRow row)
  {
    if (row == null) {
      return List.of();
    }

    return process(row);
  }

  @Override
  @Nullable
  public InputRowListPlusRawValues transform(@Nullable final InputRowListPlusRawValues row)
  {
    if (row == null || row.getInputRows() == null) {
      return row;
    }

    final List<InputRow> inputRows = row.getInputRows();
    final List<Map<String, Object>> inputRawValues = row.getRawValuesList();
    final List<InputRow> outputRows = new ArrayList<>();
    final List<Map<String, Object>> outputRawValues = inputRawValues == null ? null : new ArrayList<>();

    for (int i = 0; i < inputRows.size(); i++) {
      final List<InputRow> expandedRows = transformToList(inputRows.get(i));
      outputRows.addAll(expandedRows);
      if (outputRawValues != null) {
        for (int j = 0; j < expandedRows.size(); j++) {
          outputRawValues.add(inputRawValues.get(i));
        }
      }
    }

    return InputRowListPlusRawValues.ofList(outputRawValues, outputRows, row.getParseException());
  }

  @Override
  public void close() throws IOException
  {
    cursorHolder.close();
  }

  private List<InputRow> process(final InputRow inputRow)
  {
    baseCursorFactory.set(inputRow);

    if (cursor == null) {
      cursor = cursorHolder.asCursor();
    } else {
      cursor.reset();
    }

    if (cursor == null || cursor.isDone()) {
      return List.of(buildPassthroughRow(inputRow));
    }

    final List<String> columns = resolveColumnsForRow(inputRow);
    final List<String> dimensionColumns = resolveDimensionColumns(inputRow, columns);
    final ColumnSelectorFactory selectorFactory = cursor.getColumnSelectorFactory();

    final List<InputRow> result = new ArrayList<>();
    while (!cursor.isDone()) {
      final Map<String, Object> event = new LinkedHashMap<>();
      for (final String col : columns) {
        event.put(col, selectorFactory.makeColumnValueSelector(col).getObject());
      }
      result.add(new MapBasedInputRow(inputRow.getTimestampFromEpoch(), dimensionColumns, event));
      cursor.advance();
    }

    return result.isEmpty() ? List.of(buildPassthroughRow(inputRow)) : result;
  }

  private InputRow buildPassthroughRow(final InputRow inputRow)
  {
    final Set<String> unnestOutputColumns = new LinkedHashSet<>();
    collectOutputColumnNames(query.getDataSource(), unnestOutputColumns);

    final List<String> columns = resolveColumnsForRow(inputRow);
    final List<String> dimensionColumns = resolveDimensionColumns(inputRow, columns);
    final ColumnSelectorFactory factory = baseCursorFactory.getColumnSelectorFactory(query.getVirtualColumns());
    final Map<String, Object> event = new LinkedHashMap<>();
    for (final String col : columns) {
      if (unnestOutputColumns.contains(col)) {
        event.put(col, null);
      } else {
        event.put(col, factory.makeColumnValueSelector(col).getObject());
      }
    }
    return new MapBasedInputRow(inputRow.getTimestampFromEpoch(), dimensionColumns, event);
  }

  private List<String> resolveColumnsForRow(final InputRow inputRow)
  {
    final Set<String> columns = new LinkedHashSet<>();
    columns.add(ColumnHolder.TIME_COLUMN_NAME);
    columns.addAll(inputRow.getDimensions());
    for (final VirtualColumn vc : query.getVirtualColumns().getVirtualColumns()) {
      columns.add(vc.getOutputName());
    }
    collectOutputColumnNames(query.getDataSource(), columns);
    return new ArrayList<>(columns);
  }

  private static void collectOutputColumnNames(final DataSource dataSource, final Set<String> columns)
  {
    if (dataSource instanceof UnnestDataSource) {
      final UnnestDataSource unnest = (UnnestDataSource) dataSource;
      columns.add(unnest.getVirtualColumn().getOutputName());
    }
    for (final DataSource child : dataSource.getChildren()) {
      collectOutputColumnNames(child, columns);
    }
  }

  private static List<String> resolveDimensionColumns(
      final InputRow inputRow,
      @Nullable final List<String> resultColumns
  )
  {
    final LinkedHashSet<String> dims = new LinkedHashSet<>(inputRow.getDimensions());
    if (resultColumns != null) {
      for (final String col : resultColumns) {
        if (!ColumnHolder.TIME_COLUMN_NAME.equals(col)) {
          dims.add(col);
        }
      }
    }
    return new ArrayList<>(dims);
  }

  private static class CursorFactorySegment implements Segment
  {
    private final CursorFactory cursorFactory;

    CursorFactorySegment(final CursorFactory cursorFactory)
    {
      this.cursorFactory = cursorFactory;
    }

    @Nullable
    @Override
    public SegmentId getId()
    {
      return null;
    }

    @Nonnull
    @Override
    public Interval getDataInterval()
    {
      return Intervals.ETERNITY;
    }

    @Nullable
    @Override
    public <T> T as(final Class<T> clazz)
    {
      if (CursorFactory.class.equals(clazz)) {
        return (T) cursorFactory;
      }
      return null;
    }

    @Override
    public void close()
    {
    }
  }
}
