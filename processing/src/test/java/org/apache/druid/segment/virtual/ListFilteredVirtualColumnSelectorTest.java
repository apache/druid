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
import org.apache.druid.collections.bitmap.RoaringBitmapFactory;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.segment.ColumnSelector;
import org.apache.druid.segment.ColumnSelectorColumnIndexSelector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.RowAdapters;
import org.apache.druid.segment.RowBasedColumnSelectorFactory;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.DictionaryEncodedStringValueIndex;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.filter.SelectorFilter;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

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
    Assert.assertNull(selector.getObject());
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
    Assert.assertNull(selector.getObject());
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
    Assert.assertEquals(ImmutableList.of("a", "b"), selector.getObject());
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
    Assert.assertEquals(ImmutableList.of("a", "b"), selector.getObject());
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
    Assert.assertEquals(ImmutableList.of("c", "d"), selector.getObject());
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
    Assert.assertEquals(ImmutableList.of("c", "d"), selector.getObject());
    assertCapabilities(selectorFactory, DENY_VIRTUAL_NAME);
  }

  @Test
  public void testFilterListFilteredVirtualColumnAllowIndex()
  {
    ListFilteredVirtualColumn virtualColumn = new ListFilteredVirtualColumn(
        ALLOW_VIRTUAL_NAME,
        new DefaultDimensionSpec(COLUMN_NAME, COLUMN_NAME, ColumnType.STRING),
        ImmutableSet.of("b", "c"),
        true
    );

    ColumnSelector selector = EasyMock.createMock(ColumnSelector.class);
    ColumnHolder holder = EasyMock.createMock(ColumnHolder.class);
    ColumnHolder timeHolder = EasyMock.createMock(ColumnHolder.class);
    DictionaryEncodedStringValueIndex index = EasyMock.createMock(DictionaryEncodedStringValueIndex.class);
    ImmutableBitmap bitmap = EasyMock.createMock(ImmutableBitmap.class);
    BitmapFactory bitmapFactory = EasyMock.createMock(BitmapFactory.class);
    ColumnIndexSupplier indexSupplier = EasyMock.createMock(ColumnIndexSupplier.class);

    EasyMock.expect(selector.getColumnHolder(COLUMN_NAME)).andReturn(holder).atLeastOnce();
    EasyMock.expect(selector.getColumnHolder(ColumnHolder.TIME_COLUMN_NAME)).andReturn(timeHolder).atLeastOnce();
    EasyMock.expect(timeHolder.getLength()).andReturn(10).anyTimes();
    EasyMock.expect(selector.getColumnCapabilities(COLUMN_NAME))
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

    EasyMock.replay(selector, holder, timeHolder, indexSupplier, index, bitmap, bitmapFactory);

    ColumnSelectorColumnIndexSelector bitmapIndexSelector = new ColumnSelectorColumnIndexSelector(
        new RoaringBitmapFactory(),
        VirtualColumns.create(Collections.singletonList(virtualColumn)),
        selector
    );

    SelectorFilter filter = new SelectorFilter(ALLOW_VIRTUAL_NAME, "a");
    Assert.assertNotNull(filter.getBitmapColumnIndex(bitmapIndexSelector));

    DictionaryEncodedStringValueIndex listFilteredIndex =
        bitmapIndexSelector.getIndexSupplier(ALLOW_VIRTUAL_NAME).as(DictionaryEncodedStringValueIndex.class);
    Assert.assertEquals(2, listFilteredIndex.getCardinality());
    Assert.assertEquals("b", listFilteredIndex.getValue(0));
    Assert.assertEquals("c", listFilteredIndex.getValue(1));
    Assert.assertEquals(bitmap, listFilteredIndex.getBitmap(1));

    EasyMock.verify(selector, holder, timeHolder, indexSupplier, index, bitmap, bitmapFactory);
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


    ColumnSelector selector = EasyMock.createMock(ColumnSelector.class);
    ColumnHolder holder = EasyMock.createMock(ColumnHolder.class);
    ColumnHolder timeHolder = EasyMock.createMock(ColumnHolder.class);
    DictionaryEncodedStringValueIndex index = EasyMock.createMock(DictionaryEncodedStringValueIndex.class);
    ImmutableBitmap bitmap = EasyMock.createMock(ImmutableBitmap.class);
    ColumnIndexSupplier indexSupplier = EasyMock.createMock(ColumnIndexSupplier.class);
    BitmapFactory bitmapFactory = EasyMock.createMock(BitmapFactory.class);

    EasyMock.expect(selector.getColumnHolder(COLUMN_NAME)).andReturn(holder).atLeastOnce();
    EasyMock.expect(selector.getColumnHolder(ColumnHolder.TIME_COLUMN_NAME)).andReturn(timeHolder).atLeastOnce();
    EasyMock.expect(timeHolder.getLength()).andReturn(10).anyTimes();
    EasyMock.expect(selector.getColumnCapabilities(COLUMN_NAME))
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

    EasyMock.replay(selector, holder, timeHolder, indexSupplier, index, bitmap, bitmapFactory);

    ColumnSelectorColumnIndexSelector bitmapIndexSelector = new ColumnSelectorColumnIndexSelector(
        new RoaringBitmapFactory(),
        VirtualColumns.create(Collections.singletonList(virtualColumn)),
        selector
    );

    SelectorFilter filter = new SelectorFilter(DENY_VIRTUAL_NAME, "c");
    Assert.assertNotNull(filter.getBitmapColumnIndex(bitmapIndexSelector));

    DictionaryEncodedStringValueIndex listFilteredIndex =
        bitmapIndexSelector.getIndexSupplier(DENY_VIRTUAL_NAME).as(DictionaryEncodedStringValueIndex.class);
    Assert.assertEquals(1, listFilteredIndex.getCardinality());
    Assert.assertEquals(bitmap, listFilteredIndex.getBitmap(1));

    EasyMock.verify(selector, holder, timeHolder, indexSupplier, index, bitmap, bitmapFactory);
  }

  private void assertCapabilities(VirtualizedColumnSelectorFactory selectorFactory, String columnName)
  {
    ColumnCapabilities capabilities = selectorFactory.getColumnCapabilities(columnName);
    Assert.assertNotNull(capabilities);
    Assert.assertEquals(ValueType.STRING, capabilities.getType());
    Assert.assertTrue(capabilities.hasMultipleValues().isMaybeTrue());
  }

  private VirtualizedColumnSelectorFactory makeSelectorFactory(ListFilteredVirtualColumn virtualColumn)
  {
    VirtualizedColumnSelectorFactory selectorFactory = new VirtualizedColumnSelectorFactory(
        RowBasedColumnSelectorFactory.create(
            RowAdapters.standardRow(),
            () -> new MapBasedRow(0L, ImmutableMap.of(COLUMN_NAME, ImmutableList.of("a", "b", "c", "d"))),
            rowSignature,
            false,
            false
        ),
        VirtualColumns.create(ImmutableList.of(virtualColumn))
    );

    return selectorFactory;
  }
}
