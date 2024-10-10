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

package org.apache.druid.query.rowsandcols.semantic;

import com.google.common.collect.Lists;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.operator.OffsetLimit;
import org.apache.druid.query.operator.window.RowsAndColumnsHelper;
import org.apache.druid.query.rowsandcols.LazilyDecoratedRowsAndColumns;
import org.apache.druid.query.rowsandcols.MapOfColumnsRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.junit.Test;

import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeNotNull;

public class EvaluateRowsAndColumnsTest extends SemanticTestBase
{
  public EvaluateRowsAndColumnsTest(String name, Function<MapOfColumnsRowsAndColumns, RowsAndColumns> fn)
  {
    super(name, fn);
  }

  @Test
  public void testMaterializeColumns()
  {
    Object[][] vals = new Object[][] {
        {1L, "a", 123L, new Object[]{"xyz", "x"}, 0L},
        {2L, "a", 456L, new Object[]{"abc"}, 1L},
        {3L, "b", 789L, new Object[]{null}, 2L},
        {4L, null, 123L, null, 3L},
    };

    RowSignature siggy = RowSignature.builder()
        .add("__time", ColumnType.LONG)
        .add("dim", ColumnType.STRING)
        .add("val", ColumnType.LONG)
        .add("array", ColumnType.STRING_ARRAY)
        .add("arrayIndex", ColumnType.LONG)
        .build();

    final RowsAndColumns base = make(MapOfColumnsRowsAndColumns.fromRowObjects(vals, siggy));

    Object[] expectedArr = new Object[][] {
        {"xyz", "x"},
        {"abc"},
        {null},
        null
    };

    new RowsAndColumnsHelper()
        .expectColumn("array", expectedArr, ColumnType.STRING_ARRAY)
        .validate(base);

    assumeNotNull("skipping: CursorFactory not supported", base.as(CursorFactory.class));

    LazilyDecoratedRowsAndColumns ras = new LazilyDecoratedRowsAndColumns(
        base,
        null,
        null,
        VirtualColumns.create(new ExpressionVirtualColumn(
            "expr",
            "val * 2",
            ColumnType.LONG,
            TestExprMacroTable.INSTANCE)),
        OffsetLimit.NONE,
        null,
        null,
        null);

    // do the materialziation
    ras.numRows();

    assertEquals(Lists.newArrayList("__time", "dim", "val", "array", "arrayIndex", "expr"), ras.getColumnNames());

    new RowsAndColumnsHelper()
        .expectColumn("expr", new long[] {123 * 2, 456L * 2, 789 * 2, 123 * 2})
        .validate(ras);

    new RowsAndColumnsHelper()
        .expectColumn("dim", new String[] {"a", "a", "b", null}, ColumnType.STRING)
        .validate(ras);

    new RowsAndColumnsHelper()
        .expectColumn("array", expectedArr, ColumnType.STRING_ARRAY)
        .validate(ras);
  }
}
