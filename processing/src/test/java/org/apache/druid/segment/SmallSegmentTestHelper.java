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
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.vector.VectorCursor;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.junit.jupiter.api.Assertions;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A small segment, and ways to read it, shared by the tests of the cursor factories that wrap other ones.
 */
public class SmallSegmentTestHelper
{
  /**
   * Rows of {@link #makeIndex}, one per hour from 2000-01-01.
   */
  private static final List<Map<String, Object>> ROWS = ImmutableList.of(
      row("a", "a", ImmutableList.of("a", "b"), 1L),
      row("b", "x", ImmutableList.of("b"), 2L),
      row("c", "c", ImmutableList.of("a", "c"), 3L),
      row("a", "y", null, 4L)
  );

  private SmallSegmentTestHelper()
  {
    // No instantiation.
  }

  /**
   * Returns a segment of {@link #ROWS}, with string columns "dim", "dim2" and "mv" (multi-valued), and long column
   * "num".
   */
  public static QueryableIndex makeIndex(final File tempDir)
  {
    final List<InputRow> rows = new ArrayList<>();
    for (int i = 0; i < ROWS.size(); i++) {
      rows.add(
          new MapBasedInputRow(
              DateTimes.of("2000-01-01").plusHours(i),
              ImmutableList.of("dim", "dim2", "mv", "num"),
              ROWS.get(i)
          )
      );
    }

    return IndexBuilder.create()
                       .tmpDir(tempDir)
                       .schema(
                           new IncrementalIndexSchema.Builder()
                               .withMinTimestamp(DateTimes.of("2000-01-01").getMillis())
                               .withDimensionsSpec(
                                   DimensionsSpec.builder()
                                                 .setDimensions(
                                                     ImmutableList.of(
                                                         new StringDimensionSchema("dim"),
                                                         new StringDimensionSchema("dim2"),
                                                         new StringDimensionSchema("mv"),
                                                         new LongDimensionSchema("num")
                                                     )
                                                 )
                                                 .build()
                               )
                               .withRollup(false)
                               .build()
                       )
                       .rows(rows)
                       .buildMMappedIndex();
  }

  /**
   * Returns the given columns of every row a non-vectorized cursor yields.
   */
  public static List<List<Object>> readRows(
      final CursorFactory factory,
      final CursorBuildSpec spec,
      final String... columns
  )
  {
    try (final CursorHolder holder = factory.makeCursorHolder(spec)) {
      return readRows(holder.asCursor(), columns);
    }
  }

  /**
   * Returns the given columns of every row the given cursor yields.
   */
  public static List<List<Object>> readRows(final Cursor cursor, final String... columns)
  {
    final List<ColumnValueSelector<?>> selectors = new ArrayList<>();
    for (final String column : columns) {
      selectors.add(cursor.getColumnSelectorFactory().makeColumnValueSelector(column));
    }

    final List<List<Object>> rows = new ArrayList<>();
    while (!cursor.isDone()) {
      final List<Object> row = new ArrayList<>();
      for (final ColumnValueSelector<?> selector : selectors) {
        row.add(selector.getObject());
      }
      rows.add(row);
      cursor.advance();
    }
    return rows;
  }

  /**
   * Returns the given columns of every row a vectorized cursor yields, read through object selectors. Fails if the
   * holder cannot vectorize.
   */
  public static List<List<Object>> readVectorRows(
      final CursorFactory factory,
      final CursorBuildSpec spec,
      final String... columns
  )
  {
    try (final CursorHolder holder = factory.makeCursorHolder(spec)) {
      Assertions.assertTrue(holder.canVectorize(), "holder must vectorize");
      final VectorCursor cursor = holder.asVectorCursor();
      final List<VectorObjectSelector> selectors = new ArrayList<>();
      for (final String column : columns) {
        selectors.add(cursor.getColumnSelectorFactory().makeObjectSelector(column));
      }

      final List<List<Object>> rows = new ArrayList<>();
      while (!cursor.isDone()) {
        final List<Object[]> vectors = new ArrayList<>();
        for (final VectorObjectSelector selector : selectors) {
          vectors.add(selector.getObjectVector());
        }
        for (int i = 0; i < cursor.getCurrentVectorSize(); i++) {
          final List<Object> row = new ArrayList<>();
          for (final Object[] vector : vectors) {
            row.add(vector[i]);
          }
          rows.add(row);
        }
        cursor.advance();
      }
      return rows;
    }
  }

  private static Map<String, Object> row(final String dim, final String dim2, final List<String> mv, final long num)
  {
    final Map<String, Object> row = new HashMap<>();
    row.put("dim", dim);
    row.put("dim2", dim2);
    row.put("mv", mv);
    row.put("num", num);
    return row;
  }
}
