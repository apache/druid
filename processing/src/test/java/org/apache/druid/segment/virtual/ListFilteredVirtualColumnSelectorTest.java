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
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.RowAdapters;
import org.apache.druid.segment.RowBasedColumnSelectorFactory;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

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
        new DefaultDimensionSpec(NON_EXISTENT_COLUMN_NAME, NON_EXISTENT_COLUMN_NAME, ValueType.STRING),
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
        new DefaultDimensionSpec(NON_EXISTENT_COLUMN_NAME, NON_EXISTENT_COLUMN_NAME, ValueType.STRING),
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
        new DefaultDimensionSpec(COLUMN_NAME, COLUMN_NAME, ValueType.STRING),
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
        new DefaultDimensionSpec(COLUMN_NAME, COLUMN_NAME, ValueType.STRING),
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
        new DefaultDimensionSpec(COLUMN_NAME, COLUMN_NAME, ValueType.STRING),
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
        new DefaultDimensionSpec(COLUMN_NAME, COLUMN_NAME, ValueType.STRING),
        ImmutableSet.of("a", "b"),
        false
    );

    VirtualizedColumnSelectorFactory selectorFactory = makeSelectorFactory(virtualColumn);
    ColumnValueSelector<?> selector = selectorFactory.makeColumnValueSelector(DENY_VIRTUAL_NAME);
    Assert.assertEquals(ImmutableList.of("c", "d"), selector.getObject());
    assertCapabilities(selectorFactory, DENY_VIRTUAL_NAME);
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
            false
        ),
        VirtualColumns.create(ImmutableList.of(virtualColumn))
    );

    return selectorFactory;
  }
}
