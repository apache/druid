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

package org.apache.druid.segment;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.NullFilter;
import org.apache.druid.query.filter.RangeFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.filter.AndFilter;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.shim.ShimCursor;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.segment.virtual.NestedFieldVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.joda.time.Interval;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Verifies that a time-descending vectorized cursor over a {@link QueryableIndex} produces exactly the same rows, in
 * the same order, as the non-vectorized descending cursor.
 */
public class DescendingVectorCursorDiffTest extends InitializedNullHandlingTest
{
  private static final int[] VECTOR_SIZES = new int[]{1, 2, 3, 7, 512};

  @TempDir
  public File temporaryFolder;

  @Test
  public void testStandardColumns()
  {
    final VirtualColumns virtualColumns = VirtualColumns.create(
        new ExpressionVirtualColumn("vc", "index * 2 + 1", ColumnType.DOUBLE, ExprMacroTable.nil())
    );

    assertVectorizedMatchesNonVectorized(
        new QueryableIndexCursorFactory(TestIndex.getMMappedTestIndex()),
        virtualColumns,
        ImmutableList.of(
            ColumnHolder.TIME_COLUMN_NAME,
            "market",               // single-value string
            "placementish",         // multi-value string
            "partial_null_column",  // partially-null string
            "qualityLong",
            "longNumericNull",      // null-bearing long
            "floatNumericNull",     // null-bearing float
            "doubleNumericNull",    // null-bearing double
            "index",                // double metric
            "quality_uniques",      // complex column
            "vc"                    // expression virtual column
        ),
        ImmutableMap.of(
            // Bitmap-indexed filters use DescendingBitmapVectorOffset.
            "bitmapIndex", new SelectorDimFilter("market", "spot", null).toFilter(),
            "bitmapIndexSelective", new InDimFilter("quality", ImmutableSet.of("automotive")).toFilter(),
            "nullBitmapIndex", new NotDimFilter(new NullFilter("longNumericNull", null)).toFilter(),

            // Filters without an index use FilteredVectorOffset on top of a descending offset.
            "valueMatcher", new RangeFilter("index", ColumnType.DOUBLE, 100.0, 1000.0, true, true, null),
            "virtualColumnMatcher", new RangeFilter("vc", ColumnType.DOUBLE, 500.0, null, true, false, null),
            "bitmapIndexAndValueMatcher", new AndFilter(
                ImmutableList.of(
                    new SelectorDimFilter("market", "spot", null).toFilter(),
                    new RangeFilter("index", ColumnType.DOUBLE, 100.0, null, true, false, null)
                )
            )
        ),
        // Strictly inside the segment interval, which runs from 2011-01-12 to 2011-04-15.
        Intervals.of("2011-01-13T00:00:00.000Z/2011-03-01T00:00:00.000Z")
    );
  }

  @Test
  public void testNestedColumns()
  {
    final VirtualColumns virtualColumns = VirtualColumns.create(
        new NestedFieldVirtualColumn("nested", "$.x", "nestedX", ColumnType.LONG),
        new NestedFieldVirtualColumn("nested", "$.y", "nestedY", ColumnType.STRING)
    );

    assertVectorizedMatchesNonVectorized(
        new QueryableIndexCursorFactory(makeNestedIndex()),
        virtualColumns,
        ImmutableList.of(
            ColumnHolder.TIME_COLUMN_NAME,
            "str",
            "lng",
            "dbl",
            "arr",
            "nested",
            "nestedX",
            "nestedY"
        ),
        ImmutableMap.of(
            "bitmapIndex", new SelectorDimFilter("str", "s3", null).toFilter(),
            "valueMatcher", new RangeFilter("lng", ColumnType.LONG, 50L, null, true, false, null),
            "nestedField", new SelectorDimFilter("nestedY", "a", null).toFilter()
        ),
        // Rows are one minute apart starting at 2000-01-01, so this clips to rows [37, 191).
        Intervals.of("2000-01-01T00:37:00.000Z/2000-01-01T03:11:00.000Z")
    );
  }

  /**
   * Runs the unfiltered case, plus each of the named {@code filters}, at each of the {@link #VECTOR_SIZES}. Each
   * combination runs twice: once over the entire segment, and once over {@code narrowInterval}, which must lie
   * strictly inside the segment so that the descending offset starts partway through the segment (nonzero start
   * offset) and stops before its final row (clipped end offset).
   */
  private void assertVectorizedMatchesNonVectorized(
      final CursorFactory cursorFactory,
      final VirtualColumns virtualColumns,
      final List<String> columns,
      final Map<String, Filter> filters,
      final Interval narrowInterval
  )
  {
    final Map<String, Filter> allCases = new LinkedHashMap<>();
    allCases.put("noFilter", null);
    allCases.putAll(filters);

    for (final Map.Entry<String, Filter> filterEntry : allCases.entrySet()) {
      for (final int vectorSize : VECTOR_SIZES) {
        // Row count for Intervals.ETERNITY, used to verify that "narrowInterval" actually narrows.
        int fullSegmentRowCount = -1;

        for (final Interval interval : ImmutableList.of(Intervals.ETERNITY, narrowInterval)) {
          final String message = StringUtils.format(
              "interval[%s] filter[%s] vectorSize[%d]",
              interval,
              filterEntry.getKey(),
              vectorSize
          );
          final CursorBuildSpec buildSpec =
              CursorBuildSpec.builder()
                             .setInterval(interval)
                             .setPreferredOrdering(Cursors.descendingTimeOrder())
                             .setVirtualColumns(virtualColumns)
                             .setFilter(filterEntry.getValue())
                             .setQueryContext(
                                 QueryContext.of(QueryContextParameters.VECTOR_SIZE, vectorSize)
                             )
                             .build();

          final List<List<Object>> expected;
          final List<List<Object>> actual;

          try (final CursorHolder holder = cursorFactory.makeCursorHolder(buildSpec)) {
            Assertions.assertEquals(Cursors.descendingTimeOrder(), holder.getOrdering(), message);
            expected = readRows(holder.asCursor(), columns);
          }

          try (final CursorHolder holder = cursorFactory.makeCursorHolder(buildSpec)) {
            Assertions.assertTrue(holder.canVectorize(), message);
            actual = readRows(new ShimCursor(holder.asVectorCursor()), columns);
          }

          Assertions.assertFalse(expected.isEmpty(), message + ": expected some rows");
          Assertions.assertEquals(expected.size(), actual.size(), message + ": row count");
          for (int i = 0; i < expected.size(); i++) {
            Assertions.assertEquals(expected.get(i), actual.get(i), message + ": row " + i);
          }

          // Sanity check: __time is non-increasing.
          long previousTime = Long.MAX_VALUE;
          for (final List<Object> row : actual) {
            final long time = ((Number) row.get(0)).longValue();
            Assertions.assertTrue(time <= previousTime, message + ": descending __time");
            previousTime = time;
          }

          if (Intervals.ETERNITY.equals(interval)) {
            fullSegmentRowCount = actual.size();
          } else {
            // Guards against the narrow-interval case silently degrading into a second full-segment scan.
            Assertions.assertTrue(
                actual.size() < fullSegmentRowCount,
                message + ": narrow interval must return fewer rows than the full segment"
            );
          }
        }
      }
    }
  }

  private QueryableIndex makeNestedIndex()
  {
    final List<String> dimensions = ImmutableList.of("str", "lng", "dbl", "arr", "nested");
    final List<InputRow> rows = new ArrayList<>();

    for (int i = 0; i < 300; i++) {
      final Map<String, Object> event = new HashMap<>();
      event.put("str", i % 7 == 0 ? null : "s" + (i % 13));
      event.put("lng", i % 5 == 0 ? null : (long) i);
      event.put("dbl", i % 3 == 0 ? null : i * 1.5);
      event.put("arr", i % 11 == 0 ? null : ImmutableList.of((long) i, (long) (i + 1)));
      event.put("nested", i % 9 == 0 ? null : ImmutableMap.of("x", i, "y", i % 4 == 0 ? "a" : "b"));
      rows.add(new MapBasedInputRow(DateTimes.of("2000-01-01").plusMinutes(i), dimensions, event));
    }

    return IndexBuilder.create()
                       .tmpDir(temporaryFolder)
                       .schema(
                           IncrementalIndexSchema
                               .builder()
                               .withDimensionsSpec(
                                   DimensionsSpec.builder()
                                                 .setDimensions(
                                                     dimensions.stream()
                                                               .map(d -> new AutoTypeColumnSchema(d, null, null))
                                                               .collect(Collectors.toList())
                                                 )
                                                 .build()
                               )
                               .withRollup(false)
                               .build()
                       )
                       .rows(rows)
                       .buildMMappedIndex();
  }

  private static List<List<Object>> readRows(final Cursor cursor, final List<String> columns)
  {
    final ColumnSelectorFactory columnSelectorFactory = cursor.getColumnSelectorFactory();
    final List<ColumnValueSelector<?>> selectors = new ArrayList<>(columns.size());
    for (final String column : columns) {
      selectors.add(columnSelectorFactory.makeColumnValueSelector(column));
    }

    final List<List<Object>> rows = new ArrayList<>();
    while (!cursor.isDone()) {
      final List<Object> row = new ArrayList<>(columns.size());
      for (final ColumnValueSelector<?> selector : selectors) {
        row.add(comparableValue(selector.getObject()));
      }
      rows.add(row);
      cursor.advance();
    }
    return rows;
  }

  /**
   * Converts a selector value into something with meaningful {@link Object#equals}.
   */
  @Nullable
  private static Object comparableValue(@Nullable final Object o)
  {
    if (o instanceof Object[]) {
      return comparableList(Arrays.asList((Object[]) o));
    } else if (o instanceof List) {
      return comparableList((List<?>) o);
    } else {
      return o;
    }
  }

  private static List<Object> comparableList(final List<?> list)
  {
    return list.stream().map(DescendingVectorCursorDiffTest::comparableValue).collect(Collectors.toList());
  }
}
