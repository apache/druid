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

package org.apache.druid.segment.join;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.filter.SelectorFilter;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class JoinVirtualColumnSplitTest extends InitializedNullHandlingTest
{
  private static final RowSignature BASE_TABLE = RowSignature.builder()
                                                             .add("column", ColumnType.STRING)
                                                             .build();

  @Test
  public void test_resolve_dependencies()
  {
    final VirtualColumn v0 = new ExpressionVirtualColumn("v0", "concat(column, 'foo')", ColumnType.STRING, TestExprMacroTable.INSTANCE);
    final VirtualColumn v1 = new ExpressionVirtualColumn("v1", "concat(v0, 'foo')", ColumnType.STRING, TestExprMacroTable.INSTANCE);
    final VirtualColumn v2 = new ExpressionVirtualColumn("v2", "concat(v0, 'bar')", ColumnType.STRING, TestExprMacroTable.INSTANCE);
    final VirtualColumn v3 = new ExpressionVirtualColumn("v3", "concat(v2, 'foobar')", ColumnType.STRING, TestExprMacroTable.INSTANCE);
    final VirtualColumn v4 = new ExpressionVirtualColumn("v4", "concat(v1, v3)", ColumnType.STRING, TestExprMacroTable.INSTANCE);
    VirtualColumns virtualColumns = VirtualColumns.create(v0, v1, v2, v3);

    Assert.assertEquals(
        Collections.emptySet(),
        JoinVirtualColumnSplit.resolveDependencies(virtualColumns, v0)
    );
    Assert.assertEquals(
        ImmutableSet.of(v0),
        JoinVirtualColumnSplit.resolveDependencies(virtualColumns, v1)
    );
    Assert.assertEquals(
        ImmutableSet.of(v0, v2),
        JoinVirtualColumnSplit.resolveDependencies(virtualColumns, v3)
    );
    Assert.assertEquals(
        ImmutableSet.of(v0, v1, v2, v3),
        JoinVirtualColumnSplit.resolveDependencies(virtualColumns, v4)
    );
  }

  @Test
  public void test_split_virtual_column_with_nonexistent_input()
  {
    final List<VirtualColumn> virtualColumns = ImmutableList.of(
        new ExpressionVirtualColumn("v0", "concat(column, 'foo')", ColumnType.STRING, TestExprMacroTable.INSTANCE),
        new ExpressionVirtualColumn("v1", "concat(nonexistent, 'foo')", ColumnType.STRING, TestExprMacroTable.INSTANCE)
    );
    final JoinVirtualColumnSplit split = JoinVirtualColumnSplit.split(
        BASE_TABLE,
        VirtualColumns.create(virtualColumns),
        null
    );
    Assert.assertEquals(Collections.singletonList(virtualColumns.get(0)), split.getPreJoinVirtualColumns());
    Assert.assertEquals(Collections.singletonList(virtualColumns.get(1)), split.getPostJoinVirtualColumns());
  }

  @Test
  public void test_split_virtual_column_with_nonexistent_input_required_by_base_filter()
  {
    final List<VirtualColumn> virtualColumns = ImmutableList.of(
        new ExpressionVirtualColumn("v0", "concat(column, 'foo')", ColumnType.STRING, TestExprMacroTable.INSTANCE),
        new ExpressionVirtualColumn("v1", "concat(nonexistent, 'foo')", ColumnType.STRING, TestExprMacroTable.INSTANCE)
    );
    final JoinVirtualColumnSplit split = JoinVirtualColumnSplit.split(
        BASE_TABLE,
        VirtualColumns.create(virtualColumns),
        new SelectorFilter("v1", "xfoo")
    );
    Assert.assertEquals(virtualColumns, split.getPreJoinVirtualColumns());
    Assert.assertEquals(Collections.emptyList(), split.getPostJoinVirtualColumns());
  }

  @Test
  public void test_split_virtual_column_dependent_on_other()
  {
    final List<VirtualColumn> virtualColumns = ImmutableList.of(
        new ExpressionVirtualColumn("v1", "concat(v0, 'bar')", ColumnType.STRING, TestExprMacroTable.INSTANCE),
        new ExpressionVirtualColumn("v0", "concat(column, 'foo')", ColumnType.STRING, TestExprMacroTable.INSTANCE)
    );
    final JoinVirtualColumnSplit split = JoinVirtualColumnSplit.split(
        BASE_TABLE,
        VirtualColumns.create(virtualColumns),
        null
    );
    // order is switched from resolving dependencies
    Assert.assertEquals(
        ImmutableList.of(virtualColumns.get(1), virtualColumns.get(0)),
        split.getPreJoinVirtualColumns()
    );
    Assert.assertEquals(Collections.emptyList(), split.getPostJoinVirtualColumns());
  }

  @Test
  public void test_split_virtual_column_dependent_on_other_deeper()
  {
    final List<VirtualColumn> virtualColumns = ImmutableList.of(
        new ExpressionVirtualColumn("v2", "concat(v1, 'bar')", ColumnType.STRING, TestExprMacroTable.INSTANCE),
        new ExpressionVirtualColumn("v1", "concat(v0, 'bar')", ColumnType.STRING, TestExprMacroTable.INSTANCE),
        new ExpressionVirtualColumn("v0", "concat(column, 'foo')", ColumnType.STRING, TestExprMacroTable.INSTANCE)
    );
    final JoinVirtualColumnSplit split = JoinVirtualColumnSplit.split(
        BASE_TABLE,
        VirtualColumns.create(virtualColumns),
        null
    );
    // order is switched from resolving dependencies
    Assert.assertEquals(
        ImmutableList.of(virtualColumns.get(2), virtualColumns.get(1), virtualColumns.get(0)),
        split.getPreJoinVirtualColumns()
    );
    Assert.assertEquals(Collections.emptyList(), split.getPostJoinVirtualColumns());
  }

  @Test
  public void test_split_virtual_column_dependent_on_other_nonexistent_input()
  {
    final List<VirtualColumn> virtualColumns = ImmutableList.of(
        new ExpressionVirtualColumn("v0", "concat(nonexistent, 'foo')", ColumnType.STRING, TestExprMacroTable.INSTANCE),
        new ExpressionVirtualColumn("v1", "concat(v0, 'bar')", ColumnType.STRING, TestExprMacroTable.INSTANCE)
    );
    final JoinVirtualColumnSplit split = JoinVirtualColumnSplit.split(
        BASE_TABLE,
        VirtualColumns.create(virtualColumns),
        null
    );
    Assert.assertEquals(Collections.emptyList(), split.getPreJoinVirtualColumns());
    Assert.assertEquals(virtualColumns, split.getPostJoinVirtualColumns());
  }

  @Test
  public void test_split_virtual_column_dependent_on_other_nonexistent_input_deeper()
  {
    final List<VirtualColumn> virtualColumns = ImmutableList.of(
        new ExpressionVirtualColumn("v2", "concat(v1, 'foo')", ColumnType.STRING, TestExprMacroTable.INSTANCE),
        new ExpressionVirtualColumn("v0", "concat(nonexistent, 'foo')", ColumnType.STRING, TestExprMacroTable.INSTANCE),
        new ExpressionVirtualColumn("v1", "concat(v0, 'bar')", ColumnType.STRING, TestExprMacroTable.INSTANCE)
    );
    final JoinVirtualColumnSplit split = JoinVirtualColumnSplit.split(
        BASE_TABLE,
        VirtualColumns.create(virtualColumns),
        null
    );
    Assert.assertEquals(Collections.emptyList(), split.getPreJoinVirtualColumns());
    Assert.assertEquals(
        ImmutableList.of(virtualColumns.get(1), virtualColumns.get(2), virtualColumns.get(0)),
        split.getPostJoinVirtualColumns()
    );
  }

  @Test
  public void test_split_virtual_column_dependent_on_other_with_nonexistent_input_required_by_base_filter()
  {
    final List<VirtualColumn> virtualColumns = ImmutableList.of(
        new ExpressionVirtualColumn("v0", "concat(nonexistent, 'foo')", ColumnType.STRING, TestExprMacroTable.INSTANCE),
        new ExpressionVirtualColumn("v1", "concat(v0, 'bar')", ColumnType.STRING, TestExprMacroTable.INSTANCE)
    );
    final JoinVirtualColumnSplit split = JoinVirtualColumnSplit.split(
        BASE_TABLE,
        VirtualColumns.create(virtualColumns),
        new SelectorFilter("v1", "xfoobar")
    );
    Assert.assertEquals(
        virtualColumns,
        split.getPreJoinVirtualColumns()
    );
    Assert.assertEquals(Collections.emptyList(), split.getPostJoinVirtualColumns());
  }

  @Test
  public void test_split_virtual_column_dependent_on_other_with_nonexistent_input_partially_required_by_base_filter()
  {
    final List<VirtualColumn> virtualColumns = ImmutableList.of(
        new ExpressionVirtualColumn("v0", "concat(nonexistent, 'foo')", ColumnType.STRING, TestExprMacroTable.INSTANCE),
        new ExpressionVirtualColumn("v1", "concat(v0, 'bar')", ColumnType.STRING, TestExprMacroTable.INSTANCE)
    );
    final JoinVirtualColumnSplit split = JoinVirtualColumnSplit.split(
        BASE_TABLE,
        VirtualColumns.create(virtualColumns),
        new SelectorFilter("v0", "xfoo")
    );
    // we could technically skip pushing down v1 since v0 is a constant because of non-existent input, but the
    // dependency resolution logic isn't cool enough to determine the differece
    Assert.assertEquals(virtualColumns, split.getPreJoinVirtualColumns());
    Assert.assertEquals(Collections.emptyList(), split.getPostJoinVirtualColumns());
  }
}
