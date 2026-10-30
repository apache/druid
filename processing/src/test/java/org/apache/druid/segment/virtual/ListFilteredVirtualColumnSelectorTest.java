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

package org.apache.druid.segment.virtual;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.ColumnIndexSelector;
import org.apache.druid.segment.ColumnCache;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexCursorFactory;
import org.apache.druid.segment.RowAdapters;
import org.apache.druid.segment.RowBasedColumnSelectorFactory;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.BaseColumnHolder;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.filter.SelectorFilter;
import org.apache.druid.segment.index.semantic.DictionaryEncodedStringValueIndex;
import org.apache.druid.segment.index.semantic.DruidPredicateIndexes;
import org.apache.druid.segment.index.semantic.NullValueIndex;
import org.apache.druid.segment.index.semantic.StringValueSetIndexes;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ListFilteredVirtualColumnSelectorTest extends InitializedNullHandlingTest
{
  private static final String COLUMN_NAME = "x";
  private static final String NON_EXISTENT_COLUMN_NAME = "nope";
  private static final String ALLOW_VIRTUAL_NAME = "allowed";
  private static final String DENY_VIRTUAL_NAME = "no-stairway";
  private final RowSignature rowSignature = RowSignature.builder()
                                                        .addTimeColumn()
                                                        .addDimensions(ImmutableList.of(DefaultDimensionSpec.of(COLUMN_NAME)))
                                                        .build();


  @Test
  public void testListFilteredVirtualColumnNilDimensionSelector()
  {
    ListFilteredVirtualColumn virtualColumn = new ListFilteredVirtualColumn(
        ALLOW_VIRTUAL_NAME,
        new DefaultDimensionSpec(NON_EXISTENT_COLUMN_NAME, NON_EXISTENT_COLUMN_NAME, ColumnType.STRING),
        ImmutableSet.of("a", "b"),
        true
    );

    VirtualizedColumnSelectorFactory selectorFactory = makeSelectorFactory(virtualColumn);
    DimensionSelector selector = selectorFactory.makeDimensionSelector(DefaultDimensionSpec.of(ALLOW_VIRTUAL_NAME));
    Assertions.assertNull(selector.getObject());
  }

  @Test
  public void testListFilteredVirtualColumnNilColumnValueSelector()
  {
    ListFilteredVirtualColumn virtualColumn = new ListFilteredVirtualColumn(
        ALLOW_VIRTUAL_NAME,
        new DefaultDimensionSpec(NON_EXISTENT_COLUMN_NAME, NON_EXISTENT_COLUMN_NAME, ColumnType.STRING),
        ImmutableSet.of("a", "b"),
        true
    );

    VirtualizedColumnSelectorFactory selectorFactory = makeSelectorFactory(virtualColumn);
    ColumnValueSelector<?> selector = selectorFactory.makeColumnValueSelector(ALLOW_VIRTUAL_NAME);
    Assertions.assertNull(selector.getObject());
  }


  @Test
  public void testListFilteredVirtualColumnAllowListDimensionSelector()
  {
    ListFilteredVirtualColumn virtualColumn = new ListFilteredVirtualColumn(
        ALLOW_VIRTUAL_NAME,
        new DefaultDimensionSpec(COLUMN_NAME, COLUMN_NAME, ColumnType.STRING),
        ImmutableSet.of("a", "b"),
        true
    );

    VirtualizedColumnSelectorFactory selectorFactory = makeSelectorFactory(virtualColumn);
    DimensionSelector selector = selectorFactory.makeDimensionSelector(DefaultDimensionSpec.of(ALLOW_VIRTUAL_NAME));
    Assertions.assertEquals(ImmutableList.of("a", "b"), selector.getObject());
    assertCapabilities(selectorFactory, ALLOW_VIRTUAL_NAME);
  }

  @Test
  public void testListFilteredVirtualColumnAllowListColumnValueSelector()
  {
    ListFilteredVirtualColumn virtualColumn = new ListFilteredVirtualColumn(
        ALLOW_VIRTUAL_NAME,
        new DefaultDimensionSpec(COLUMN_NAME, COLUMN_NAME, ColumnType.STRING),
        ImmutableSet.of("a", "b"),
        true
    );

    VirtualizedColumnSelectorFactory selectorFactory = makeSelectorFactory(virtualColumn);
    ColumnValueSelector<?> selector = selectorFactory.makeColumnValueSelector(ALLOW_VIRTUAL_NAME);
    Assertions.assertEquals(ImmutableList.of("a", "b"), selector.getObject());
    assertCapabilities(selectorFactory, ALLOW_VIRTUAL_NAME);
  }

  @Test
  public void testListFilteredVirtualColumnDenyListDimensionSelector()
  {
    ListFilteredVirtualColumn virtualColumn = new ListFilteredVirtualColumn(
        DENY_VIRTUAL_NAME,
        new DefaultDimensionSpec(COLUMN_NAME, COLUMN_NAME, ColumnType.STRING),
        ImmutableSet.of("a", "b"),
        false
    );

    VirtualizedColumnSelectorFactory selectorFactory = makeSelectorFactory(virtualColumn);
    DimensionSelector selector = selectorFactory.makeDimensionSelector(DefaultDimensionSpec.of(DENY_VIRTUAL_NAME));
    Assertions.assertEquals(ImmutableList.of("c", "d"), selector.getObject());
    assertCapabilities(selectorFactory, DENY_VIRTUAL_NAME);
  }

  @Test
  public void testListFilteredVirtualColumnDenyListColumnValueSelector()
  {
    ListFilteredVirtualColumn virtualColumn = new ListFilteredVirtualColumn(
        DENY_VIRTUAL_NAME,
        new DefaultDimensionSpec(COLUMN_NAME, COLUMN_NAME, ColumnType.STRING),
        ImmutableSet.of("a", "b"),
        false
    );

    VirtualizedColumnSelectorFactory selectorFactory = makeSelectorFactory(virtualColumn);
    ColumnValueSelector<?> selector = selectorFactory.makeColumnValueSelector(DENY_VIRTUAL_NAME);
    Assertions.assertEquals(ImmutableList.of("c", "d"), selector.getObject());
    assertCapabilities(selectorFactory, DENY_VIRTUAL_NAME);
  }

  @Test
  public void testFilterListFilteredVirtualColumnAllowIndex() throws IOException
  {
    ListFilteredVirtualColumn virtualColumn = new ListFilteredVirtualColumn(
        ALLOW_VIRTUAL_NAME,
        new DefaultDimensionSpec(COLUMN_NAME, COLUMN_NAME, ColumnType.STRING),
        ImmutableSet.of("b", "c"),
        true
    );

    QueryableIndex queryableIndex = EasyMock.createMock(QueryableIndex.class);
    BaseColumnHolder holder = EasyMock.createMock(BaseColumnHolder.class);
    BaseColumnHolder timeHolder = EasyMock.createMock(BaseColumnHolder.class);
    DictionaryEncodedStringValueIndex index = EasyMock.createMock(DictionaryEncodedStringValueIndex.class);
    ImmutableBitmap bitmap = EasyMock.createMock(ImmutableBitmap.class);
    BitmapFactory bitmapFactory = EasyMock.createMock(BitmapFactory.class);
    ColumnIndexSupplier indexSupplier = EasyMock.createMock(ColumnIndexSupplier.class);

    EasyMock.expect(queryableIndex.getColumnHolder(COLUMN_NAME)).andReturn(holder).atLeastOnce();
    EasyMock.expect(queryableIndex.getColumnHolder(ColumnHolder.TIME_COLUMN_NAME)).andReturn(timeHolder).atLeastOnce();
    EasyMock.expect(timeHolder.getLength()).andReturn(10).anyTimes();
    EasyMock.expect(queryableIndex.getColumnCapabilities(COLUMN_NAME))
            .andReturn(new ColumnCapabilitiesImpl().setType(ColumnType.STRING)
                                                   .setDictionaryEncoded(true)
                                                   .setDictionaryValuesUnique(true)
                                                   .setDictionaryValuesSorted(true)
                                                   .setHasBitmapIndexes(true)
            ).anyTimes();


    EasyMock.expect(holder.getIndexSupplier()).andReturn(indexSupplier).atLeastOnce();
    EasyMock.expect(indexSupplier.as(DictionaryEncodedStringValueIndex.class)).andReturn(index).atLeastOnce();

    EasyMock.expect(index.getCardinality()).andReturn(3).atLeastOnce();
    EasyMock.expect(index.getValue(0)).andReturn("a").atLeastOnce();
    EasyMock.expect(index.getValue(1)).andReturn("b").atLeastOnce();
    EasyMock.expect(index.getValue(2)).andReturn("c").atLeastOnce();

    EasyMock.expect(index.getBitmap(2)).andReturn(bitmap).once();

    EasyMock.replay(queryableIndex, holder, timeHolder, indexSupplier, index, bitmap, bitmapFactory);

    try (final Closer closer = Closer.create()) {
      ColumnIndexSelector bitmapIndexSelector = new ColumnCache(
          queryableIndex,
          VirtualColumns.create(virtualColumn),
          closer
      );

      SelectorFilter filter = new SelectorFilter(ALLOW_VIRTUAL_NAME, "a");
      Assertions.assertNotNull(filter.getBitmapColumnIndex(bitmapIndexSelector));

      DictionaryEncodedStringValueIndex listFilteredIndex =
          bitmapIndexSelector.getIndexSupplier(ALLOW_VIRTUAL_NAME).as(DictionaryEncodedStringValueIndex.class);
      Assertions.assertEquals(2, listFilteredIndex.getCardinality());
      Assertions.assertEquals("b", listFilteredIndex.getValue(0));
      Assertions.assertEquals("c", listFilteredIndex.getValue(1));
      Assertions.assertEquals(bitmap, listFilteredIndex.getBitmap(1));

      EasyMock.verify(queryableIndex, holder, timeHolder, indexSupplier, index, bitmap, bitmapFactory);
    }
  }

  @Test
  public void testFilterListFilteredVirtualColumnDenyIndex()
  {
    ListFilteredVirtualColumn virtualColumn = new ListFilteredVirtualColumn(
        DENY_VIRTUAL_NAME,
        new DefaultDimensionSpec(COLUMN_NAME, COLUMN_NAME, ColumnType.STRING),
        ImmutableSet.of("a", "b"),
        false
    );


    QueryableIndex queryableIndex = EasyMock.createMock(QueryableIndex.class);
    BaseColumnHolder holder = EasyMock.createMock(BaseColumnHolder.class);
    BaseColumnHolder timeHolder = EasyMock.createMock(BaseColumnHolder.class);
    DictionaryEncodedStringValueIndex index = EasyMock.createMock(DictionaryEncodedStringValueIndex.class);
    ImmutableBitmap bitmap = EasyMock.createMock(ImmutableBitmap.class);
    ColumnIndexSupplier indexSupplier = EasyMock.createMock(ColumnIndexSupplier.class);
    BitmapFactory bitmapFactory = EasyMock.createMock(BitmapFactory.class);

    EasyMock.expect(queryableIndex.getColumnHolder(COLUMN_NAME)).andReturn(holder).atLeastOnce();
    EasyMock.expect(queryableIndex.getColumnHolder(ColumnHolder.TIME_COLUMN_NAME)).andReturn(timeHolder).atLeastOnce();
    EasyMock.expect(timeHolder.getLength()).andReturn(10).anyTimes();
    EasyMock.expect(queryableIndex.getColumnCapabilities(COLUMN_NAME))
            .andReturn(new ColumnCapabilitiesImpl().setType(ColumnType.STRING)
                                                   .setDictionaryEncoded(true)
                                                   .setDictionaryValuesUnique(true)
                                                   .setDictionaryValuesSorted(true)
                                                   .setHasBitmapIndexes(true)
            ).anyTimes();
    EasyMock.expect(holder.getIndexSupplier()).andReturn(indexSupplier).atLeastOnce();
    EasyMock.expect(indexSupplier.as(DictionaryEncodedStringValueIndex.class)).andReturn(index).atLeastOnce();
    EasyMock.expect(index.getCardinality()).andReturn(3).atLeastOnce();
    EasyMock.expect(index.getValue(0)).andReturn("a").atLeastOnce();
    EasyMock.expect(index.getValue(1)).andReturn("b").atLeastOnce();
    EasyMock.expect(index.getValue(2)).andReturn("c").atLeastOnce();

    EasyMock.expect(index.getBitmap(0)).andReturn(bitmap).once();

    EasyMock.replay(queryableIndex, holder, timeHolder, indexSupplier, index, bitmap, bitmapFactory);

    try (final Closer closer = Closer.create()) {
      ColumnIndexSelector bitmapIndexSelector = new ColumnCache(
          queryableIndex,
          VirtualColumns.create(virtualColumn),
          closer
      );

      SelectorFilter filter = new SelectorFilter(DENY_VIRTUAL_NAME, "c");
      Assertions.assertNotNull(filter.getBitmapColumnIndex(bitmapIndexSelector));

      DictionaryEncodedStringValueIndex listFilteredIndex =
          bitmapIndexSelector.getIndexSupplier(DENY_VIRTUAL_NAME).as(DictionaryEncodedStringValueIndex.class);
      Assertions.assertEquals(1, listFilteredIndex.getCardinality());
      Assertions.assertEquals(bitmap, listFilteredIndex.getBitmap(1));

      EasyMock.verify(queryableIndex, holder, timeHolder, indexSupplier, index, bitmap, bitmapFactory);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testListFilteredVirtualColumnExpressionDelegate() throws IOException
  {
    // expression maps many dictionary values to the same value, so the delegate selector has duplicate names
    final ExpressionVirtualColumn expressionVirtualColumn = new ExpressionVirtualColumn(
        "expr",
        "if(placementish == 'preferred', 'p', 'x')",
        ColumnType.STRING,
        TestExprMacroTable.INSTANCE
    );
    final ListFilteredVirtualColumn allowVirtualColumn = new ListFilteredVirtualColumn(
        ALLOW_VIRTUAL_NAME,
        DefaultDimensionSpec.of("expr"),
        ImmutableSet.of("x"),
        true
    );
    final ListFilteredVirtualColumn denyVirtualColumn = new ListFilteredVirtualColumn(
        DENY_VIRTUAL_NAME,
        DefaultDimensionSpec.of("expr"),
        ImmutableSet.of("x"),
        false
    );
    final VirtualColumns virtualColumns = VirtualColumns.create(
        expressionVirtualColumn,
        allowVirtualColumn,
        denyVirtualColumn
    );
    final QueryableIndex index = TestIndex.getMMappedTestIndex();

    try (final Closer closer = Closer.create()) {
      final CursorHolder cursorHolder = closer.register(
          new QueryableIndexCursorFactory(index).makeCursorHolder(
              CursorBuildSpec.builder().setVirtualColumns(virtualColumns).build()
          )
      );
      final Cursor cursor = cursorHolder.asCursor();
      final DimensionSelector baseSelector =
          cursor.getColumnSelectorFactory().makeDimensionSelector(DefaultDimensionSpec.of("placementish"));
      final DimensionSelector allowSelector =
          cursor.getColumnSelectorFactory().makeDimensionSelector(DefaultDimensionSpec.of(ALLOW_VIRTUAL_NAME));
      final DimensionSelector denySelector =
          cursor.getColumnSelectorFactory().makeDimensionSelector(DefaultDimensionSpec.of(DENY_VIRTUAL_NAME));

      // delegate is dictionary backed, so the filtered selector can remap dictionary ids instead of evaluating the
      // expression for every row
      Assertions.assertEquals(
          "org.apache.druid.query.dimension.ForwardingFilteredDimensionSelector",
          allowSelector.getClass().getName()
      );
      Assertions.assertEquals(
          "org.apache.druid.query.dimension.ForwardingFilteredDimensionSelector",
          denySelector.getClass().getName()
      );
      Assertions.assertNull(allowSelector.idLookup());

      int rows = 0;
      while (!cursor.isDone()) {
        final List<String> expectedAllow = new ArrayList<>();
        final List<String> expectedDeny = new ArrayList<>();
        final IndexedInts baseRow = baseSelector.getRow();
        for (int i = 0; i < baseRow.size(); i++) {
          if ("preferred".equals(baseSelector.lookupName(baseRow.get(i)))) {
            expectedDeny.add("p");
          } else {
            expectedAllow.add("x");
          }
        }
        Assertions.assertEquals(expectedAllow, lookupRow(allowSelector));
        Assertions.assertEquals(expectedDeny, lookupRow(denySelector));
        Assertions.assertEquals(!expectedAllow.isEmpty(), allowSelector.makeValueMatcher("x").matches(false));
        Assertions.assertEquals(!expectedDeny.isEmpty(), denySelector.makeValueMatcher("p").matches(false));
        rows++;
        cursor.advance();
      }
      Assertions.assertEquals(index.getNumRows(), rows);

      // the expression virtual column does not provide a dictionary index, so there are no list filtered indexes
      final ColumnIndexSelector indexSelector = new ColumnCache(index, virtualColumns, closer);
      final ColumnIndexSupplier indexSupplier = indexSelector.getIndexSupplier(ALLOW_VIRTUAL_NAME);
      Assertions.assertNotNull(indexSupplier);
      Assertions.assertNull(indexSupplier.as(DictionaryEncodedStringValueIndex.class));
      Assertions.assertNull(indexSupplier.as(StringValueSetIndexes.class));
      Assertions.assertNull(indexSupplier.as(DruidPredicateIndexes.class));
      Assertions.assertNull(indexSupplier.as(NullValueIndex.class));
    }
  }

  private static List<String> lookupRow(DimensionSelector selector)
  {
    final IndexedInts row = selector.getRow();
    final List<String> values = new ArrayList<>(row.size());
    for (int i = 0; i < row.size(); i++) {
      values.add(selector.lookupName(row.get(i)));
    }
    return values;
  }

  private void assertCapabilities(VirtualizedColumnSelectorFactory selectorFactory, String columnName)
  {
    ColumnCapabilities capabilities = selectorFactory.getColumnCapabilities(columnName);
    Assertions.assertNotNull(capabilities);
    Assertions.assertEquals(ValueType.STRING, capabilities.getType());
    Assertions.assertTrue(capabilities.hasMultipleValues().isMaybeTrue());
  }

  private VirtualizedColumnSelectorFactory makeSelectorFactory(ListFilteredVirtualColumn virtualColumn)
  {
    VirtualizedColumnSelectorFactory selectorFactory = new VirtualizedColumnSelectorFactory(
        RowBasedColumnSelectorFactory.create(
            RowAdapters.standardRow(),
            () -> new MapBasedRow(0L, ImmutableMap.of(COLUMN_NAME, ImmutableList.of("a", "b", "c", "d"))),
            rowSignature,
            false
        ),
        VirtualColumns.create(virtualColumn)
    );

    return selectorFactory;
  }
}
