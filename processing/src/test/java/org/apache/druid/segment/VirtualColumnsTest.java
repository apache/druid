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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Longs;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.dimension.ExtractionDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.extraction.BucketExtractionFn;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.ZeroIndexedInts;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.segment.virtual.NestedFieldVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class VirtualColumnsTest extends InitializedNullHandlingTest
{
  private static final String REAL_COLUMN_NAME = "real_column";

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Mock
  public ColumnSelectorFactory baseColumnSelectorFactory;

  @Test
  public void testExists()
  {
    final VirtualColumns virtualColumns = makeVirtualColumns();

    Assert.assertTrue(virtualColumns.exists("expr"));
    Assert.assertTrue(virtualColumns.exists("foo"));
    Assert.assertTrue(virtualColumns.exists("foo.5"));
    Assert.assertFalse(virtualColumns.exists("bar"));
  }

  @Test
  public void testIsEmpty()
  {
    assertTrue(VirtualColumns.EMPTY.isEmpty());
    assertTrue(VirtualColumns.create(Collections.emptyList()).isEmpty());
  }

  @Test
  public void testGetColumnNames()
  {
    final VirtualColumns virtualColumns = makeVirtualColumns();
    List<String> colNames = ImmutableList.<String>builder()
        .add("expr")
        .add("expr2i")
        .add("expr2")
        .add("foo")
        .build();
    assertEquals(colNames, virtualColumns.getColumnNames());
  }

  @Test
  public void testGetColumnCapabilitiesNilBase()
  {
    final VirtualColumns virtualColumns = makeVirtualColumns();
    final ColumnInspector baseInspector = column -> null;
    Assert.assertEquals(ValueType.FLOAT, virtualColumns.getColumnCapabilitiesWithoutFallback(baseInspector, "expr").getType());
    Assert.assertEquals(ValueType.LONG, virtualColumns.getColumnCapabilitiesWithoutFallback(baseInspector, "expr2").getType());
    Assert.assertNull(virtualColumns.getColumnCapabilitiesWithoutFallback(baseInspector, REAL_COLUMN_NAME));
  }

  @Test
  public void testGetColumnCapabilitiesDoubleBase()
  {
    final VirtualColumns virtualColumns = makeVirtualColumns();
    final ColumnInspector baseInspector = column -> {
      if (REAL_COLUMN_NAME.equals(column)) {
        return ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.DOUBLE);
      } else {
        return null;
      }
    };
    Assert.assertEquals(ValueType.FLOAT, virtualColumns.getColumnCapabilitiesWithoutFallback(baseInspector, "expr").getType());
    Assert.assertEquals(ValueType.DOUBLE, virtualColumns.getColumnCapabilitiesWithoutFallback(baseInspector, "expr2").getType());
    Assert.assertNull(virtualColumns.getColumnCapabilitiesWithoutFallback(baseInspector, REAL_COLUMN_NAME));
  }

  @Test
  public void testGetColumnCapabilitiesWithFallbackNilBase()
  {
    final VirtualColumns virtualColumns = makeVirtualColumns();
    final ColumnInspector baseInspector = column -> null;
    Assert.assertEquals(
        ValueType.FLOAT,
        virtualColumns.getColumnCapabilitiesWithFallback(baseInspector, "expr").getType()
    );
    Assert.assertEquals(
        ValueType.LONG,
        virtualColumns.getColumnCapabilitiesWithFallback(baseInspector, "expr2").getType()
    );
    Assert.assertNull(virtualColumns.getColumnCapabilitiesWithFallback(baseInspector, REAL_COLUMN_NAME));
  }

  @Test
  public void testGetColumnCapabilitiesWithFallbackDoubleBase()
  {
    final VirtualColumns virtualColumns = makeVirtualColumns();
    final ColumnInspector baseInspector = column -> {
      if (REAL_COLUMN_NAME.equals(column)) {
        return ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.DOUBLE);
      } else {
        return null;
      }
    };
    Assert.assertEquals(
        ValueType.FLOAT,
        virtualColumns.getColumnCapabilitiesWithFallback(baseInspector, "expr").getType()
    );
    Assert.assertEquals(
        ValueType.DOUBLE,
        virtualColumns.getColumnCapabilitiesWithFallback(baseInspector, "expr2").getType()
    );
    Assert.assertEquals(
        ValueType.DOUBLE,
        virtualColumns.getColumnCapabilitiesWithFallback(baseInspector, REAL_COLUMN_NAME).getType()
    );
  }

  @Test
  public void testWrapInspectorNilBase()
  {
    final VirtualColumns virtualColumns = makeVirtualColumns();
    final ColumnInspector baseInspector = column -> null;
    final ColumnInspector wrappedInspector = virtualColumns.wrapInspector(baseInspector);
    Assert.assertEquals(
        ValueType.FLOAT,
        wrappedInspector.getColumnCapabilities("expr").getType()
    );
    Assert.assertEquals(
        ValueType.LONG,
        wrappedInspector.getColumnCapabilities("expr2").getType()
    );
    Assert.assertNull(wrappedInspector.getColumnCapabilities(REAL_COLUMN_NAME));
  }

  @Test
  public void testWrapInspectorDoubleBase()
  {
    final VirtualColumns virtualColumns = makeVirtualColumns();
    final ColumnInspector baseInspector = column -> {
      if (REAL_COLUMN_NAME.equals(column)) {
        return ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.DOUBLE);
      } else {
        return null;
      }
    };
    final ColumnInspector wrappedInspector = virtualColumns.wrapInspector(baseInspector);
    Assert.assertEquals(
        ValueType.FLOAT,
        wrappedInspector.getColumnCapabilities("expr").getType()
    );
    Assert.assertEquals(
        ValueType.DOUBLE,
        wrappedInspector.getColumnCapabilities("expr2").getType()
    );
    Assert.assertEquals(
        ValueType.DOUBLE,
        wrappedInspector.getColumnCapabilities(REAL_COLUMN_NAME).getType()
    );
  }

  @Test
  public void testNonExistentSelector()
  {
    final VirtualColumns virtualColumns = makeVirtualColumns();

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("No such virtual column[bar]");

    virtualColumns.makeColumnValueSelector("bar", baseColumnSelectorFactory);
  }

  @Test
  public void testMakeSelectors()
  {
    Mockito.when(baseColumnSelectorFactory.getRowIdSupplier()).thenReturn(null);

    final VirtualColumns virtualColumns = makeVirtualColumns();
    final BaseObjectColumnValueSelector objectSelector = virtualColumns.makeColumnValueSelector(
        "expr",
        baseColumnSelectorFactory
    );
    final DimensionSelector dimensionSelector = virtualColumns.makeDimensionSelector(
        new DefaultDimensionSpec("expr", "x"),
        baseColumnSelectorFactory
    );
    final DimensionSelector extractionDimensionSelector = virtualColumns.makeDimensionSelector(
        new ExtractionDimensionSpec("expr", "x", new BucketExtractionFn(1.0, 0.5)),
        baseColumnSelectorFactory
    );
    final BaseFloatColumnValueSelector floatSelector = virtualColumns.makeColumnValueSelector(
        "expr",
        baseColumnSelectorFactory
    );
    final BaseLongColumnValueSelector longSelector = virtualColumns.makeColumnValueSelector(
        "expr",
        baseColumnSelectorFactory
    );

    Assert.assertEquals(1L, objectSelector.getObject());
    Assert.assertEquals("1", dimensionSelector.lookupName(dimensionSelector.getRow().get(0)));
    Assert.assertEquals("0.5", extractionDimensionSelector.lookupName(extractionDimensionSelector.getRow().get(0)));
    Assert.assertEquals(1.0f, floatSelector.getFloat(), 0.0f);
    Assert.assertEquals(1L, longSelector.getLong());
  }

  @Test
  public void testMakeSelectorsWithDotSupport()
  {
    final VirtualColumns virtualColumns = makeVirtualColumns();
    final BaseObjectColumnValueSelector objectSelector = virtualColumns.makeColumnValueSelector(
        "foo.5",
        baseColumnSelectorFactory
    );
    final DimensionSelector dimensionSelector = virtualColumns.makeDimensionSelector(
        new DefaultDimensionSpec("foo.5", "x"),
        baseColumnSelectorFactory
    );
    final BaseFloatColumnValueSelector floatSelector = virtualColumns.makeColumnValueSelector(
        "foo.5",
        baseColumnSelectorFactory
    );
    final BaseLongColumnValueSelector longSelector = virtualColumns.makeColumnValueSelector(
        "foo.5",
        baseColumnSelectorFactory
    );

    Assert.assertEquals(5L, objectSelector.getObject());
    Assert.assertEquals("5", dimensionSelector.lookupName(dimensionSelector.getRow().get(0)));
    Assert.assertEquals(5.0f, floatSelector.getFloat(), 0.0f);
    Assert.assertEquals(5L, longSelector.getLong());
  }

  @Test
  public void testMakeSelectorsWithDotSupportBaseNameOnly()
  {
    final VirtualColumns virtualColumns = makeVirtualColumns();
    final BaseObjectColumnValueSelector objectSelector = virtualColumns.makeColumnValueSelector(
        "foo",
        baseColumnSelectorFactory
    );
    final DimensionSelector dimensionSelector = virtualColumns.makeDimensionSelector(
        new DefaultDimensionSpec("foo", "x"),
        baseColumnSelectorFactory
    );
    final BaseFloatColumnValueSelector floatSelector = virtualColumns.makeColumnValueSelector(
        "foo",
        baseColumnSelectorFactory
    );
    final BaseLongColumnValueSelector longSelector = virtualColumns.makeColumnValueSelector(
        "foo",
        baseColumnSelectorFactory
    );

    Assert.assertEquals(-1L, objectSelector.getObject());
    Assert.assertEquals("-1", dimensionSelector.lookupName(dimensionSelector.getRow().get(0)));
    Assert.assertEquals(-1.0f, floatSelector.getFloat(), 0.0f);
    Assert.assertEquals(-1L, longSelector.getLong());
  }

  @Test
  public void testTimeNotAllowed()
  {
    final ExpressionVirtualColumn expr = new ExpressionVirtualColumn(
        "__time",
        "x + y",
        ColumnType.FLOAT,
        TestExprMacroTable.INSTANCE
    );

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("virtualColumn name[__time] not allowed");

    VirtualColumns.create(ImmutableList.of(expr));
  }

  @Test
  public void testDuplicateNameDetection()
  {
    final ExpressionVirtualColumn expr = new ExpressionVirtualColumn(
        "expr",
        "x + y",
        ColumnType.FLOAT,
        TestExprMacroTable.INSTANCE
    );

    final ExpressionVirtualColumn expr2 = new ExpressionVirtualColumn(
        "expr",
        "x * 2",
        ColumnType.FLOAT,
        TestExprMacroTable.INSTANCE
    );

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Duplicate virtualColumn name[expr]");

    VirtualColumns.create(ImmutableList.of(expr, expr2));
  }

  @Test
  public void testCycleDetection()
  {
    final ExpressionVirtualColumn expr = new ExpressionVirtualColumn(
        "expr",
        "x + expr2",
        ColumnType.FLOAT,
        TestExprMacroTable.INSTANCE
    );

    final ExpressionVirtualColumn expr2 = new ExpressionVirtualColumn(
        "expr2",
        "expr * 2",
        ColumnType.FLOAT,
        TestExprMacroTable.INSTANCE
    );

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Self-referential column[expr]");

    VirtualColumns.create(ImmutableList.of(expr, expr2));
  }

  @Test
  public void testGetCacheKey()
  {
    final VirtualColumns virtualColumns = VirtualColumns.create(
        ImmutableList.of(
            new ExpressionVirtualColumn("expr", "x + y", ColumnType.FLOAT, TestExprMacroTable.INSTANCE)
        )
    );

    final VirtualColumns virtualColumns2 = VirtualColumns.create(
        ImmutableList.of(
            new ExpressionVirtualColumn("expr", "x + y", ColumnType.FLOAT, TestExprMacroTable.INSTANCE)
        )
    );

    Assert.assertArrayEquals(virtualColumns.getCacheKey(), virtualColumns2.getCacheKey());
    Assert.assertFalse(Arrays.equals(virtualColumns.getCacheKey(), VirtualColumns.EMPTY.getCacheKey()));
  }

  @Test
  public void testEqualsAndHashCode()
  {
    final VirtualColumns virtualColumns = VirtualColumns.create(
        ImmutableList.of(
            new ExpressionVirtualColumn("expr", "x + y", ColumnType.FLOAT, TestExprMacroTable.INSTANCE)
        )
    );

    final VirtualColumns virtualColumns2 = VirtualColumns.create(
        ImmutableList.of(
            new ExpressionVirtualColumn("expr", "x + y", ColumnType.FLOAT, TestExprMacroTable.INSTANCE)
        )
    );

    Assert.assertEquals(virtualColumns, virtualColumns);
    Assert.assertEquals(virtualColumns, virtualColumns2);
    Assert.assertNotEquals(VirtualColumns.EMPTY, virtualColumns);
    Assert.assertNotEquals(VirtualColumns.EMPTY, null);

    Assert.assertEquals(virtualColumns.hashCode(), virtualColumns.hashCode());
    Assert.assertEquals(virtualColumns.hashCode(), virtualColumns2.hashCode());
    Assert.assertNotEquals(VirtualColumns.EMPTY.hashCode(), virtualColumns.hashCode());
  }

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper mapper = TestHelper.makeJsonMapper();
    final ImmutableList<VirtualColumn> theColumns = ImmutableList.of(
        new ExpressionVirtualColumn("expr", "x + y", ColumnType.FLOAT, TestExprMacroTable.INSTANCE),
        new ExpressionVirtualColumn("expr2", "x + z", ColumnType.FLOAT, TestExprMacroTable.INSTANCE)
    );
    final VirtualColumns virtualColumns = VirtualColumns.create(theColumns);

    Assert.assertEquals(
        virtualColumns,
        mapper.readValue(
            mapper.writeValueAsString(virtualColumns),
            VirtualColumns.class
        )
    );

    Assert.assertEquals(
        theColumns,
        mapper.readValue(
            mapper.writeValueAsString(virtualColumns),
            mapper.getTypeFactory().constructParametricType(List.class, VirtualColumn.class)
        )
    );
  }

  @Test
  public void testCompositeVirtualColumnsCycles()
  {
    final ExpressionVirtualColumn expr1 = new ExpressionVirtualColumn("v1", "1 + x", ColumnType.LONG, TestExprMacroTable.INSTANCE);
    final ExpressionVirtualColumn expr2 = new ExpressionVirtualColumn("v2", "1 + y", ColumnType.LONG, TestExprMacroTable.INSTANCE);
    final ExpressionVirtualColumn expr0 = new ExpressionVirtualColumn("v0", "case_searched(notnull(1 + x), v1, v2)", ColumnType.LONG, TestExprMacroTable.INSTANCE);
    final VirtualColumns virtualColumns = VirtualColumns.create(ImmutableList.of(expr0, expr1, expr2));

    Assert.assertTrue(virtualColumns.exists("v0"));
    Assert.assertTrue(virtualColumns.exists("v1"));
    Assert.assertTrue(virtualColumns.exists("v2"));
  }

  @Test
  public void testCompositeVirtualColumnsCyclesSiblings()
  {
    final ExpressionVirtualColumn expr1 = new ExpressionVirtualColumn("v1", "1 + x", ColumnType.LONG, TestExprMacroTable.INSTANCE);
    final ExpressionVirtualColumn expr2 = new ExpressionVirtualColumn("v2", "1 + y", ColumnType.LONG, TestExprMacroTable.INSTANCE);
    final ExpressionVirtualColumn expr0 = new ExpressionVirtualColumn("v0", "case_searched(notnull(v1), v1, v2)", ColumnType.LONG, TestExprMacroTable.INSTANCE);
    final VirtualColumns virtualColumns = VirtualColumns.create(ImmutableList.of(expr0, expr1, expr2));

    Assert.assertTrue(virtualColumns.exists("v0"));
    Assert.assertTrue(virtualColumns.exists("v1"));
    Assert.assertTrue(virtualColumns.exists("v2"));
  }

  @Test
  public void testCompositeVirtualColumnsCyclesTree()
  {
    final ExpressionVirtualColumn expr1 = new ExpressionVirtualColumn("v1", "1 + x", ColumnType.LONG, TestExprMacroTable.INSTANCE);
    final ExpressionVirtualColumn expr2 = new ExpressionVirtualColumn("v2", "1 + v1", ColumnType.LONG, TestExprMacroTable.INSTANCE);
    final ExpressionVirtualColumn expr0 = new ExpressionVirtualColumn("v0", "v1 + v2", ColumnType.LONG, TestExprMacroTable.INSTANCE);
    final VirtualColumns virtualColumns = VirtualColumns.create(ImmutableList.of(expr0, expr1, expr2));

    Assert.assertTrue(virtualColumns.exists("v0"));
    Assert.assertTrue(virtualColumns.exists("v1"));
    Assert.assertTrue(virtualColumns.exists("v2"));
  }

  @Test
  public void testCompositeVirtualColumnsCapabilitiesHasAccessToOtherVirtualColumns()
  {
    final ColumnInspector baseInspector = new ColumnInspector()
    {
      @Nullable
      @Override
      public ColumnCapabilities getColumnCapabilities(String column)
      {
        if ("x".equals(column)) {
          return ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.LONG);
        }
        if ("n".equals(column)) {
          return ColumnCapabilitiesImpl.createDefault().setType(ColumnType.NESTED_DATA);
        }
        return null;
      }
    };
    final NestedFieldVirtualColumn v0 = new NestedFieldVirtualColumn("n", "$.x", "v0", ColumnType.STRING);
    final NestedFieldVirtualColumn v1 = new NestedFieldVirtualColumn("n", "$.y", "v1", ColumnType.LONG);
    final ExpressionVirtualColumn expr1 = new ExpressionVirtualColumn("v2", "v0 * v1", null, TestExprMacroTable.INSTANCE);
    final ExpressionVirtualColumn expr2 = new ExpressionVirtualColumn("v3", "v0 * x", null, TestExprMacroTable.INSTANCE);
    final VirtualColumns virtualColumns = VirtualColumns.create(ImmutableList.of(v0, v1, expr1, expr2));

    Assert.assertEquals(ColumnType.STRING, virtualColumns.getColumnCapabilitiesWithoutFallback(baseInspector, "v0").toColumnType());
    Assert.assertEquals(ColumnType.LONG, virtualColumns.getColumnCapabilitiesWithoutFallback(baseInspector, "v1").toColumnType());
    Assert.assertEquals(ColumnType.DOUBLE, virtualColumns.getColumnCapabilitiesWithoutFallback(baseInspector, "v2").toColumnType());
    Assert.assertEquals(ColumnType.DOUBLE, virtualColumns.getColumnCapabilitiesWithoutFallback(baseInspector, "v3").toColumnType());
    Assert.assertTrue(virtualColumns.canVectorize(baseInspector));
  }

  private VirtualColumns makeVirtualColumns()
  {
    final ExpressionVirtualColumn expr = new ExpressionVirtualColumn(
        "expr",
        "1",
        ColumnType.FLOAT,
        TestExprMacroTable.INSTANCE
    );
    // expr2i is an input to expr2, and is used to verify that capabilities of inputs are checked properly.
    final ExpressionVirtualColumn expr2i = new ExpressionVirtualColumn(
        "expr2i",
        "2",
        ColumnType.LONG,
        TestExprMacroTable.INSTANCE
    );
    // expr2 will think it is:
    // 1) FLOAT if it has no type info;
    // 2) LONG if it only knows about expr2i;
    // 3) DOUBLE if it knows about both inputs.
    final ExpressionVirtualColumn expr2 = new ExpressionVirtualColumn(
        "expr2",
        "expr2i + " + REAL_COLUMN_NAME,
        null,
        TestExprMacroTable.INSTANCE
    );
    final DottyVirtualColumn dotty = new DottyVirtualColumn("foo");
    return VirtualColumns.create(ImmutableList.of(expr, expr2i, expr2, dotty));
  }

  static class DottyVirtualColumn implements VirtualColumn
  {
    private final String name;

    public DottyVirtualColumn(String name)
    {
      this.name = name;
    }

    @Override
    public String getOutputName()
    {
      return name;
    }

    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec, ColumnSelectorFactory factory)
    {
      final BaseLongColumnValueSelector selector = makeColumnValueSelector(dimensionSpec.getDimension(), factory);
      final ExtractionFn extractionFn = dimensionSpec.getExtractionFn();
      final DimensionSelector dimensionSelector = new DimensionSelector()
      {
        @Override
        public IndexedInts getRow()
        {
          return ZeroIndexedInts.instance();
        }

        @Override
        public int getValueCardinality()
        {
          return DimensionDictionarySelector.CARDINALITY_UNKNOWN;
        }

        @Override
        public String lookupName(int id)
        {
          final String stringValue = String.valueOf(selector.getLong());
          return extractionFn == null ? stringValue : extractionFn.apply(stringValue);
        }

        @Override
        public ValueMatcher makeValueMatcher(final String value)
        {
          return DimensionSelectorUtils.makeValueMatcherGeneric(this, value);
        }

        @Override
        public ValueMatcher makeValueMatcher(final DruidPredicateFactory predicateFactory)
        {
          return DimensionSelectorUtils.makeValueMatcherGeneric(this, predicateFactory);
        }

        @Override
        public boolean nameLookupPossibleInAdvance()
        {
          return false;
        }

        @Nullable
        @Override
        public IdLookup idLookup()
        {
          return new IdLookup()
          {
            @Override
            public int lookupId(final String name)
            {
              return 0;
            }
          };
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          // Don't care about runtime shape in tests
        }

        @Nullable
        @Override
        public Object getObject()
        {
          return lookupName(0);
        }

        @Override
        public Class classOfObject()
        {
          return String.class;
        }

        @Override
        public boolean isNull()
        {
          return selector.isNull();
        }
      };

      return dimensionSpec.decorate(dimensionSelector);
    }

    @Override
    public ColumnValueSelector<?> makeColumnValueSelector(String columnName, ColumnSelectorFactory factory)
    {
      final String subColumn = VirtualColumns.splitColumnName(columnName).rhs;
      final Long boxed = subColumn == null ? null : Longs.tryParse(subColumn);
      final long theLong = boxed == null ? -1 : boxed;
      return new TestLongColumnSelector()
      {
        @Override
        public long getLong()
        {
          return theLong;
        }

        @Override
        public boolean isNull()
        {
          return false;
        }
      };
    }

    @Override
    public ColumnCapabilities capabilities(String columnName)
    {
      return ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.LONG);
    }

    @Override
    public List<String> requiredColumns()
    {
      return ImmutableList.of();
    }

    @Override
    public boolean usesDotNotation()
    {
      return true;
    }

    @Override
    public byte[] getCacheKey()
    {
      throw new UnsupportedOperationException();
    }
  }
}
