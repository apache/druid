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

import com.google.common.base.Supplier;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

class MergingClusterGroupCursorTest
{
  @Test
  void testAscendingMergeAcrossGroups()
  {
    // Three individually time-sorted groups; the merge interleaves them into one globally ascending stream, and each
    // emitted value must come from the group that owns that timestamp (verifies per-row dispatch to the winner).
    final MergingClusterGroupCursor cursor = cursor(
        false,
        group(new long[]{1, 5, 9}, new String[]{"g0@1", "g0@5", "g0@9"}),
        group(new long[]{2, 3, 10}, new String[]{"g1@2", "g1@3", "g1@10"}),
        group(new long[]{4, 6}, new String[]{"g2@4", "g2@6"})
    );
    final List<Object[]> rows = drain(cursor);
    assertRows(
        rows,
        new long[]{1, 2, 3, 4, 5, 6, 9, 10},
        new String[]{"g0@1", "g1@2", "g1@3", "g2@4", "g0@5", "g2@6", "g0@9", "g1@10"}
    );
  }

  @Test
  void testDescendingMerge()
  {
    // Descending: each per-group cursor is itself descending (mirrors QueryableIndexCursorHolder reversing offsets),
    // and the merge uses a max-heap.
    final MergingClusterGroupCursor cursor = cursor(
        true,
        group(new long[]{9, 5, 1}, new String[]{"g0@9", "g0@5", "g0@1"}),
        group(new long[]{10, 3, 2}, new String[]{"g1@10", "g1@3", "g1@2"}),
        group(new long[]{6, 4}, new String[]{"g2@6", "g2@4"})
    );
    final List<Object[]> rows = drain(cursor);
    assertRows(
        rows,
        new long[]{10, 9, 6, 5, 4, 3, 2, 1},
        new String[]{"g1@10", "g0@9", "g2@6", "g0@5", "g2@4", "g1@3", "g1@2", "g0@1"}
    );
  }

  @Test
  void testTiesBreakByGroupIndex()
  {
    // Equal timestamps across groups break by ascending group index (deterministic). Within a group, rows are emitted
    // in the group's own order.
    final MergingClusterGroupCursor cursor = cursor(
        false,
        group(new long[]{5, 5}, new String[]{"g0a", "g0b"}),
        group(new long[]{5}, new String[]{"g1a"})
    );
    final List<Object[]> rows = drain(cursor);
    assertRows(rows, new long[]{5, 5, 5}, new String[]{"g0a", "g0b", "g1a"});
  }

  @Test
  void testEmptyGroupsSkipped()
  {
    final MergingClusterGroupCursor cursor = cursor(
        false,
        group(new long[]{}, new String[]{}),
        group(new long[]{1, 2}, new String[]{"x", "y"}),
        group(new long[]{}, new String[]{})
    );
    assertRows(drain(cursor), new long[]{1, 2}, new String[]{"x", "y"});
  }

  @Test
  void testSingleNonEmptyGroup()
  {
    final MergingClusterGroupCursor cursor = cursor(
        false,
        group(new long[]{}, new String[]{}),
        group(new long[]{7, 8, 9}, new String[]{"a", "b", "c"})
    );
    assertRows(drain(cursor), new long[]{7, 8, 9}, new String[]{"a", "b", "c"});
  }

  @Test
  void testAllEmptyIsDoneImmediately()
  {
    final MergingClusterGroupCursor cursor = cursor(
        false,
        group(new long[]{}, new String[]{}),
        group(new long[]{}, new String[]{})
    );
    Assertions.assertTrue(cursor.isDone());
  }

  @Test
  void testResetReplaysSameSequence()
  {
    final MergingClusterGroupCursor cursor = cursor(
        false,
        group(new long[]{1, 4}, new String[]{"g0@1", "g0@4"}),
        group(new long[]{2, 3}, new String[]{"g1@2", "g1@3"})
    );
    final List<Object[]> first = drain(cursor);
    cursor.reset();
    final List<Object[]> second = drain(cursor);
    assertRows(first, new long[]{1, 2, 3, 4}, new String[]{"g0@1", "g1@2", "g1@3", "g0@4"});
    assertRows(second, new long[]{1, 2, 3, 4}, new String[]{"g0@1", "g1@2", "g1@3", "g0@4"});
  }

  @Test
  void testRowIdMonotonicAndStableWithinRow()
  {
    final MergingClusterGroupCursor cursor = cursor(
        false,
        group(new long[]{1, 3}, new String[]{"g0@1", "g0@3"}),
        group(new long[]{2, 4}, new String[]{"g1@2", "g1@4"})
    );
    final RowIdSupplier rowIdSupplier = cursor.getColumnSelectorFactory().getRowIdSupplier();
    Assertions.assertNotNull(rowIdSupplier);
    long previous = -1;
    while (!cursor.isDone()) {
      final long id = rowIdSupplier.getRowId();
      Assertions.assertEquals(id, rowIdSupplier.getRowId(), "row id must be stable within a row");
      Assertions.assertTrue(id > previous, "row id must strictly increase across rows");
      previous = id;
      cursor.advance();
    }
  }

  @SafeVarargs
  private static MergingClusterGroupCursor cursor(boolean descending, Supplier<CursorHolder>... groups)
  {
    return new MergingClusterGroupCursor(new ArrayList<>(List.of(groups)), descending);
  }

  private static List<Object[]> drain(MergingClusterGroupCursor cursor)
  {
    final ColumnSelectorFactory factory = cursor.getColumnSelectorFactory();
    final ColumnValueSelector timeSelector = factory.makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME);
    final ColumnValueSelector valueSelector = factory.makeColumnValueSelector("v");
    final List<Object[]> rows = new ArrayList<>();
    while (!cursor.isDone()) {
      rows.add(new Object[]{timeSelector.getLong(), valueSelector.getObject()});
      cursor.advance();
    }
    return rows;
  }

  private static void assertRows(List<Object[]> rows, long[] expectedTimes, String[] expectedValues)
  {
    Assertions.assertEquals(expectedTimes.length, rows.size(), "row count");
    for (int i = 0; i < expectedTimes.length; i++) {
      Assertions.assertEquals(expectedTimes[i], (long) (Long) rows.get(i)[0], "time at row " + i);
      Assertions.assertEquals(expectedValues[i], rows.get(i)[1], "value at row " + i);
    }
  }

  private static Supplier<CursorHolder> group(long[] times, String[] values)
  {
    final Cursor cursor = new ListCursor(times, values);
    final CursorHolder holder = new CursorHolder()
    {
      @Override
      public Cursor asCursor()
      {
        return cursor;
      }
    };
    return () -> holder;
  }

  /**
   * A minimal scalar {@link Cursor} over an in-memory {@code (times, values)} pair, exposing {@code __time} (long) and
   * {@code v} (object) selectors backed by the current position. Stands in for a per-group cluster cursor.
   */
  private static final class ListCursor implements Cursor
  {
    private final long[] times;
    private final String[] values;
    private final ColumnSelectorFactory factory;
    private int pos;

    private ListCursor(long[] times, String[] values)
    {
      this.times = times;
      this.values = values;
      this.factory = new ColumnSelectorFactory()
      {
        @Override
        public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
        {
          throw new UnsupportedOperationException("not used");
        }

        @Override
        public ColumnValueSelector makeColumnValueSelector(String columnName)
        {
          if (ColumnHolder.TIME_COLUMN_NAME.equals(columnName)) {
            return new TestLongColumnSelector()
            {
              @Override
              public long getLong()
              {
                return times[pos];
              }

              @Override
              public boolean isNull()
              {
                return false;
              }
            };
          }
          return new TestObjectColumnSelector<String>()
          {
            @Override
            public Class<String> classOfObject()
            {
              return String.class;
            }

            @Nullable
            @Override
            public String getObject()
            {
              return values[pos];
            }
          };
        }

        @Nullable
        @Override
        public ColumnCapabilities getColumnCapabilities(String column)
        {
          return null;
        }
      };
    }

    @Override
    public ColumnSelectorFactory getColumnSelectorFactory()
    {
      return factory;
    }

    @Override
    public void advance()
    {
      pos++;
    }

    @Override
    public void advanceUninterruptibly()
    {
      pos++;
    }

    @Override
    public boolean isDone()
    {
      return pos >= times.length;
    }

    @Override
    public boolean isDoneOrInterrupted()
    {
      return isDone();
    }

    @Override
    public void reset()
    {
      pos = 0;
    }
  }
}
