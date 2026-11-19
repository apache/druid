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

import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Tests for {@link ProjectCursorFactory}.
 */
public class ProjectCursorFactoryTest extends InitializedNullHandlingTest
{
  private static final Closer CLOSER = Closer.create();
  private static QueryableIndex index;

  /**
   * Projection of {@link #index} that renames "dim" to "out", and doubles "num" as "doubled".
   */
  private static CursorFactory factory;

  @BeforeAll
  public static void setUpClass(@TempDir final File tempDir)
  {
    index = CLOSER.register(SmallSegmentTestHelper.makeIndex(tempDir));
    factory = project(
        new ExpressionVirtualColumn("out", Parser.identifier("dim"), null),
        expression("doubled", "num * 2")
    );
  }

  @AfterAll
  public static void tearDownClass() throws IOException
  {
    CLOSER.close();
  }

  @Test
  public void test_columns()
  {
    // For now, a projection presents every column of its base too.
    Assertions.assertEquals(
        RowSignature.builder()
                    .addAll(new QueryableIndexCursorFactory(index).getRowSignature())
                    .add("out", ColumnType.STRING)
                    .add("doubled", ColumnType.LONG)
                    .build(),
        factory.getRowSignature()
    );
    assertRows(
        CursorBuildSpec.FULL_SCAN,
        List.of(List.of("a", 2L, "a"), List.of("b", 4L, "x"), List.of("c", 6L, "c"), List.of("a", 8L, "y")),
        "out",
        "doubled",
        "dim2"
    );
  }

  @Test
  public void test_columnsShadowed()
  {
    // A virtual column takes the place of the base column it shadows.
    final RowSignature baseSignature = new QueryableIndexCursorFactory(index).getRowSignature();
    final RowSignature.Builder expected = RowSignature.builder();
    for (final String column : baseSignature.getColumnNames()) {
      if (!"dim".equals(column)) {
        expected.add(column, baseSignature.getColumnType(column).orElse(null));
      }
    }
    expected.add("dim", ColumnType.LONG);

    Assertions.assertEquals(expected.build(), project(expression("dim", "num * 2")).getRowSignature());
  }

  @Test
  public void test_filterOnOurColumn()
  {
    final CursorBuildSpec spec =
        CursorBuildSpec.builder().setFilter(new EqualityFilter("out", ColumnType.STRING, "b", null)).build();

    assertRows(spec, List.of(List.of("b", 4L)), "out", "doubled");
  }

  @Test
  public void test_queryVirtualColumns()
  {
    // The query's virtual columns may read ours, and may replace base columns that ours do not read.
    final CursorBuildSpec spec = spec(expression("v0", "concat(out, '!')"), expression("dim2", "'q'"));

    assertRows(
        spec,
        List.of(List.of("a!", "q"), List.of("b!", "q"), List.of("c!", "q"), List.of("a!", "q")),
        "v0",
        "dim2"
    );
  }

  @Test
  public void test_rejectQueryVirtualColumnShadowingColumnWeRead()
  {
    final CursorBuildSpec spec = spec(expression("dim", "'q'"));
    final String message = "Virtual column name[dim] is not permitted";

    Assertions.assertEquals(
        message,
        Assertions.assertThrows(DruidException.class, () -> factory.makeCursorHolder(spec)).getMessage()
    );
    Assertions.assertEquals(
        message,
        Assertions.assertThrows(DruidException.class, () -> factory.makeCursorHolderAsync(spec)).getMessage()
    );
  }

  @Test
  public void test_ordering()
  {
    // One of ours takes the place of "dim", so the ordering ends before it.
    final List<OrderBy> indexOrdering = index.getOrdering();
    Assertions.assertEquals("dim", indexOrdering.get(1).getColumnName());

    final CursorFactory shadowing = project(expression("dim", "'q'"));
    try (final CursorHolder holder = shadowing.makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
      Assertions.assertEquals(indexOrdering.subList(0, 1), holder.getOrdering());
    }
  }

  @Test
  public void test_async()
  {
    try (final AsyncCursorHolder asyncHolder = factory.makeCursorHolderAsync(CursorBuildSpec.FULL_SCAN);
         final CursorHolder holder = asyncHolder.release()) {
      Assertions.assertEquals(
          List.of(List.of("a", "a"), List.of("b", "b"), List.of("c", "c"), List.of("a", "a")),
          SmallSegmentTestHelper.readRows(holder.asCursor(), "out", "dim")
      );
    }
  }

  private static CursorFactory project(final VirtualColumn... virtualColumns)
  {
    return new ProjectCursorFactory(new QueryableIndexCursorFactory(index), VirtualColumns.create(virtualColumns));
  }

  private static VirtualColumn expression(final String name, final String expression)
  {
    return new ExpressionVirtualColumn(name, expression, null, ExprMacroTable.nil());
  }

  private static CursorBuildSpec spec(final VirtualColumn... virtualColumns)
  {
    return CursorBuildSpec.builder().setVirtualColumns(VirtualColumns.create(virtualColumns)).build();
  }

  /**
   * Asserts that the given spec reads the given rows from {@link #factory}, both with and without vectorization.
   */
  private static void assertRows(final CursorBuildSpec spec, final List<List<Object>> expected, final String... columns)
  {
    Assertions.assertEquals(expected, SmallSegmentTestHelper.readRows(factory, spec, columns));
    Assertions.assertEquals(expected, SmallSegmentTestHelper.readVectorRows(factory, spec, columns));
  }
}
