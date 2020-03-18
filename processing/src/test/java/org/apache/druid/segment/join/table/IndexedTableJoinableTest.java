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

package org.apache.druid.segment.join.table;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.ConstantDimensionSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.segment.join.JoinMatcher;
import org.junit.Assert;
import org.junit.Test;

public class IndexedTableJoinableTest
{
  private static final String PREFIX = "j.";

  static {
    NullHandling.initializeForTests();
  }

  private final ColumnSelectorFactory dummyColumnSelectorFactory = new ColumnSelectorFactory()
  {
    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      return new ConstantDimensionSelector("dummy");
    }

    @Override
    public ColumnValueSelector<?> makeColumnValueSelector(String columnName)
    {
      return null;
    }

    @Override
    public ColumnCapabilities getColumnCapabilities(String columnName)
    {
      return null;
    }
  };

  private final InlineDataSource inlineDataSource = InlineDataSource.fromIterable(
      ImmutableList.of(
          new Object[]{"foo", 1L},
          new Object[]{"bar", 2L}
      ),
      RowSignature.builder().add("str", ValueType.STRING).add("long", ValueType.LONG).build()
  );

  private final RowBasedIndexedTable<Object[]> indexedTable = new RowBasedIndexedTable<>(
      inlineDataSource.getRowsAsList(),
      inlineDataSource.rowAdapter(),
      inlineDataSource.getRowSignature(),
      ImmutableSet.of("str")
  );

  @Test
  public void test_getAvailableColumns()
  {
    final IndexedTableJoinable joinable = new IndexedTableJoinable(indexedTable);
    Assert.assertEquals(ImmutableList.of("str", "long"), joinable.getAvailableColumns());
  }

  @Test
  public void test_getColumnCapabilities_string()
  {
    final IndexedTableJoinable joinable = new IndexedTableJoinable(indexedTable);
    final ColumnCapabilities capabilities = joinable.getColumnCapabilities("str");
    Assert.assertEquals(ValueType.STRING, capabilities.getType());
    Assert.assertTrue(capabilities.isDictionaryEncoded());
    Assert.assertFalse(capabilities.hasBitmapIndexes());
    Assert.assertFalse(capabilities.hasMultipleValues());
    Assert.assertFalse(capabilities.hasSpatialIndexes());
    Assert.assertTrue(capabilities.isComplete());
  }

  @Test
  public void test_getColumnCapabilities_long()
  {
    final IndexedTableJoinable joinable = new IndexedTableJoinable(indexedTable);
    final ColumnCapabilities capabilities = joinable.getColumnCapabilities("long");
    Assert.assertEquals(ValueType.LONG, capabilities.getType());
    Assert.assertFalse(capabilities.isDictionaryEncoded());
    Assert.assertFalse(capabilities.hasBitmapIndexes());
    Assert.assertFalse(capabilities.hasMultipleValues());
    Assert.assertFalse(capabilities.hasSpatialIndexes());
    Assert.assertTrue(capabilities.isComplete());
  }

  @Test
  public void test_getColumnCapabilities_nonexistent()
  {
    final IndexedTableJoinable joinable = new IndexedTableJoinable(indexedTable);
    final ColumnCapabilities capabilities = joinable.getColumnCapabilities("nonexistent");
    Assert.assertNull(capabilities);
  }

  @Test
  public void test_makeJoinMatcher_dimensionSelectorOnString()
  {
    final IndexedTableJoinable joinable = new IndexedTableJoinable(indexedTable);
    final JoinConditionAnalysis condition = JoinConditionAnalysis.forExpression(
        "x == \"j.str\"",
        PREFIX,
        ExprMacroTable.nil()
    );
    final JoinMatcher joinMatcher = joinable.makeJoinMatcher(dummyColumnSelectorFactory, condition, false);

    final DimensionSelector selector = joinMatcher.getColumnSelectorFactory()
                                                  .makeDimensionSelector(DefaultDimensionSpec.of("str"));

    // getValueCardinality
    Assert.assertEquals(3, selector.getValueCardinality());

    // nameLookupPossibleInAdvance
    Assert.assertTrue(selector.nameLookupPossibleInAdvance());

    // lookupName
    Assert.assertEquals("foo", selector.lookupName(0));
    Assert.assertEquals("bar", selector.lookupName(1));
    Assert.assertNull(selector.lookupName(2));

    // lookupId
    Assert.assertNull(selector.idLookup());
  }
}
