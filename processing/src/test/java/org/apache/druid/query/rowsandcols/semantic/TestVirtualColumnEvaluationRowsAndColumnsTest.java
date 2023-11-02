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
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.junit.Test;

import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeNotNull;

public class TestVirtualColumnEvaluationRowsAndColumnsTest extends SemanticTestBase
{
  public TestVirtualColumnEvaluationRowsAndColumnsTest(String name, Function<MapOfColumnsRowsAndColumns, RowsAndColumns> fn)
  {
    super(name, fn);
  }

  @Test
  public void testMaterializeVirtualColumns()
  {
    Object[][] vals = new Object[][] {
        {1L, "a", 123L, 0L},
        {2L, "a", 456L, 1L},
        {3L, "b", 789L, 2L},
        {4L, "b", 123L, 3L},
    };

    RowSignature siggy = RowSignature.builder()
        .add("__time", ColumnType.LONG)
        .add("dim", ColumnType.STRING)
        .add("val", ColumnType.LONG)
        .add("arrayIndex", ColumnType.LONG)
        .build();

    final RowsAndColumns base = make(MapOfColumnsRowsAndColumns.fromRowObjects(vals, siggy));

    assumeNotNull("skipping: StorageAdapter not supported", base.as(StorageAdapter.class));

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
        null);

    // do the materialziation
    ras.numRows();

    assertEquals(Lists.newArrayList("__time", "dim", "val", "arrayIndex", "expr"), ras.getColumnNames());

    new RowsAndColumnsHelper()
        .expectColumn("expr", new long[] {123 * 2, 456L * 2, 789 * 2, 123 * 2})
        .validate(ras);

  }

}
