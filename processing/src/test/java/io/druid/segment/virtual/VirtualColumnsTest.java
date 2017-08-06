/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.virtual;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Longs;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.ExtractionDimensionSpec;
import io.druid.query.expression.TestExprMacroTable;
import io.druid.query.extraction.BucketExtractionFn;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;
import io.druid.segment.DimensionSelectorUtils;
import io.druid.segment.DoubleColumnSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.IdLookup;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.TestDoubleColumnSelector;
import io.druid.segment.TestFloatColumnSelector;
import io.druid.segment.TestHelper;
import io.druid.segment.TestLongColumnSelector;
import io.druid.segment.VirtualColumn;
import io.druid.segment.VirtualColumns;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ColumnCapabilitiesImpl;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.ZeroIndexedInts;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;

public class VirtualColumnsTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testMakeSelectors()
  {
    final VirtualColumns virtualColumns = makeVirtualColumns();
    final ObjectColumnSelector objectSelector = virtualColumns.makeObjectColumnSelector("expr", null);
    final DimensionSelector dimensionSelector = virtualColumns.makeDimensionSelector(
        new DefaultDimensionSpec("expr", "x"),
        null
    );
    final DimensionSelector extractionDimensionSelector = virtualColumns.makeDimensionSelector(
        new ExtractionDimensionSpec("expr", "x", new BucketExtractionFn(1.0, 0.5)),
        null
    );
    final FloatColumnSelector floatSelector = virtualColumns.makeFloatColumnSelector("expr", null);
    final LongColumnSelector longSelector = virtualColumns.makeLongColumnSelector("expr", null);

    Assert.assertEquals(1L, objectSelector.get());
    Assert.assertEquals("1", dimensionSelector.lookupName(dimensionSelector.getRow().get(0)));
    Assert.assertEquals("0.5", extractionDimensionSelector.lookupName(extractionDimensionSelector.getRow().get(0)));
    Assert.assertEquals(1.0f, floatSelector.get(), 0.0f);
    Assert.assertEquals(1L, longSelector.get());
  }

  @Test
  public void testMakeSelectorsWithDotSupport()
  {
    final VirtualColumns virtualColumns = makeVirtualColumns();
    final ObjectColumnSelector objectSelector = virtualColumns.makeObjectColumnSelector("foo.5", null);
    final DimensionSelector dimensionSelector = virtualColumns.makeDimensionSelector(
        new DefaultDimensionSpec("foo.5", "x"),
        null
    );
    final FloatColumnSelector floatSelector = virtualColumns.makeFloatColumnSelector("foo.5", null);
    final LongColumnSelector longSelector = virtualColumns.makeLongColumnSelector("foo.5", null);

    Assert.assertEquals(5L, objectSelector.get());
    Assert.assertEquals("5", dimensionSelector.lookupName(dimensionSelector.getRow().get(0)));
    Assert.assertEquals(5.0f, floatSelector.get(), 0.0f);
    Assert.assertEquals(5L, longSelector.get());
  }

  @Test
  public void testMakeSelectorsWithDotSupportBaseNameOnly()
  {
    final VirtualColumns virtualColumns = makeVirtualColumns();
    final ObjectColumnSelector objectSelector = virtualColumns.makeObjectColumnSelector("foo", null);
    final DimensionSelector dimensionSelector = virtualColumns.makeDimensionSelector(
        new DefaultDimensionSpec("foo", "x"),
        null
    );
    final FloatColumnSelector floatSelector = virtualColumns.makeFloatColumnSelector("foo", null);
    final LongColumnSelector longSelector = virtualColumns.makeLongColumnSelector("foo", null);

    Assert.assertEquals(-1L, objectSelector.get());
    Assert.assertEquals("-1", dimensionSelector.lookupName(dimensionSelector.getRow().get(0)));
    Assert.assertEquals(-1.0f, floatSelector.get(), 0.0f);
    Assert.assertEquals(-1L, longSelector.get());
  }

  @Test
  public void testTimeNotAllowed()
  {
    final ExpressionVirtualColumn expr = new ExpressionVirtualColumn(
        "__time",
        "x + y",
        ValueType.FLOAT,
        TestExprMacroTable.INSTANCE
    );

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("virtualColumn name[__time] not allowed");

    VirtualColumns.create(ImmutableList.<VirtualColumn>of(expr));
  }

  @Test
  public void testDuplicateNameDetection()
  {
    final ExpressionVirtualColumn expr = new ExpressionVirtualColumn(
        "expr",
        "x + y",
        ValueType.FLOAT,
        TestExprMacroTable.INSTANCE
    );

    final ExpressionVirtualColumn expr2 = new ExpressionVirtualColumn(
        "expr",
        "x * 2",
        ValueType.FLOAT,
        TestExprMacroTable.INSTANCE
    );

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Duplicate virtualColumn name[expr]");

    VirtualColumns.create(ImmutableList.<VirtualColumn>of(expr, expr2));
  }

  @Test
  public void testCycleDetection()
  {
    final ExpressionVirtualColumn expr = new ExpressionVirtualColumn(
        "expr",
        "x + expr2",
        ValueType.FLOAT,
        TestExprMacroTable.INSTANCE
    );

    final ExpressionVirtualColumn expr2 = new ExpressionVirtualColumn(
        "expr2",
        "expr * 2",
        ValueType.FLOAT,
        TestExprMacroTable.INSTANCE
    );

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Self-referential column[expr]");

    VirtualColumns.create(ImmutableList.<VirtualColumn>of(expr, expr2));
  }

  @Test
  public void testGetCacheKey() throws Exception
  {
    final VirtualColumns virtualColumns = VirtualColumns.create(
        ImmutableList.<VirtualColumn>of(
            new ExpressionVirtualColumn("expr", "x + y", ValueType.FLOAT, TestExprMacroTable.INSTANCE)
        )
    );

    final VirtualColumns virtualColumns2 = VirtualColumns.create(
        ImmutableList.<VirtualColumn>of(
            new ExpressionVirtualColumn("expr", "x + y", ValueType.FLOAT, TestExprMacroTable.INSTANCE)
        )
    );

    Assert.assertArrayEquals(virtualColumns.getCacheKey(), virtualColumns2.getCacheKey());
    Assert.assertFalse(Arrays.equals(virtualColumns.getCacheKey(), VirtualColumns.EMPTY.getCacheKey()));
  }

  @Test
  public void testEqualsAndHashCode() throws Exception
  {
    final VirtualColumns virtualColumns = VirtualColumns.create(
        ImmutableList.<VirtualColumn>of(
            new ExpressionVirtualColumn("expr", "x + y", ValueType.FLOAT, TestExprMacroTable.INSTANCE)
        )
    );

    final VirtualColumns virtualColumns2 = VirtualColumns.create(
        ImmutableList.<VirtualColumn>of(
            new ExpressionVirtualColumn("expr", "x + y", ValueType.FLOAT, TestExprMacroTable.INSTANCE)
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
    final ObjectMapper mapper = TestHelper.getJsonMapper();
    final ImmutableList<VirtualColumn> theColumns = ImmutableList.<VirtualColumn>of(
        new ExpressionVirtualColumn("expr", "x + y", ValueType.FLOAT, TestExprMacroTable.INSTANCE),
        new ExpressionVirtualColumn("expr2", "x + z", ValueType.FLOAT, TestExprMacroTable.INSTANCE)
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

  private VirtualColumns makeVirtualColumns()
  {
    final ExpressionVirtualColumn expr = new ExpressionVirtualColumn(
        "expr",
        "1",
        ValueType.FLOAT,
        TestExprMacroTable.INSTANCE
    );
    final DottyVirtualColumn dotty = new DottyVirtualColumn("foo");
    return VirtualColumns.create(ImmutableList.of(expr, dotty));
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
    public ObjectColumnSelector makeObjectColumnSelector(String columnName, ColumnSelectorFactory factory)
    {
      final LongColumnSelector selector = makeLongColumnSelector(columnName, factory);
      return new ObjectColumnSelector()
      {
        @Override
        public Class classOfObject()
        {
          return Long.class;
        }

        @Override
        public Object get()
        {
          return selector.get();
        }
      };
    }

    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec, ColumnSelectorFactory factory)
    {
      final LongColumnSelector selector = makeLongColumnSelector(dimensionSpec.getDimension(), factory);
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
          return DimensionSelector.CARDINALITY_UNKNOWN;
        }

        @Override
        public String lookupName(int id)
        {
          final String stringValue = String.valueOf(selector.get());
          return extractionFn == null ? stringValue : extractionFn.apply(stringValue);
        }

        @Override
        public ValueMatcher makeValueMatcher(final String value)
        {
          return DimensionSelectorUtils.makeValueMatcherGeneric(this, value);
        }

        @Override
        public ValueMatcher makeValueMatcher(final Predicate<String> predicate)
        {
          return DimensionSelectorUtils.makeValueMatcherGeneric(this, predicate);
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
      };

      return dimensionSpec.decorate(dimensionSelector);
    }

    @Override
    public FloatColumnSelector makeFloatColumnSelector(String columnName, ColumnSelectorFactory factory)
    {
      final LongColumnSelector selector = makeLongColumnSelector(columnName, factory);
      return new TestFloatColumnSelector()
      {
        @Override
        public float get()
        {
          return selector.get();
        }
      };
    }

    @Override
    public LongColumnSelector makeLongColumnSelector(String columnName, ColumnSelectorFactory factory)
    {
      final String subColumn = VirtualColumns.splitColumnName(columnName).rhs;
      final Long boxed = subColumn == null ? null : Longs.tryParse(subColumn);
      final long theLong = boxed == null ? -1 : boxed;
      return new TestLongColumnSelector()
      {
        @Override
        public long get()
        {
          return theLong;
        }
      };
    }

    @Override
    public DoubleColumnSelector makeDoubleColumnSelector(
        String columnName, ColumnSelectorFactory factory
    )
    {
      final LongColumnSelector selector = makeLongColumnSelector(columnName, factory);
      return new TestDoubleColumnSelector() {

        @Override
        public double get()
        {
          return selector.get();
        }
      };
    }

    @Override
    public ColumnCapabilities capabilities(String columnName)
    {
      return new ColumnCapabilitiesImpl().setType(ValueType.LONG);
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
