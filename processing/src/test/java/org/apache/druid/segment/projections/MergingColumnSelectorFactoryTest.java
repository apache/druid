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

package org.apache.druid.segment.projections;

import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.RowIdSupplier;
import org.apache.druid.segment.TestObjectColumnSelector;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;

class MergingColumnSelectorFactoryTest
{
  private final int[] currentGroup = {0};
  private final long[] rowId = {0};

  private MergingColumnSelectorFactory factory(ColumnSelectorFactory... groups)
  {
    // No clustering columns: every requested column dispatches to the per-group factories.
    return factory(RowSignature.empty(), Collections.nCopies(groups.length, new Object[0]), groups);
  }

  private MergingColumnSelectorFactory factory(
      RowSignature clusteringColumns,
      List<Object[]> clusteringValuesByGroup,
      ColumnSelectorFactory... groups
  )
  {
    return factory(clusteringColumns, clusteringValuesByGroup, VirtualColumns.EMPTY, groups);
  }

  private MergingColumnSelectorFactory factory(
      RowSignature clusteringColumns,
      List<Object[]> clusteringValuesByGroup,
      VirtualColumns queryVirtualColumns,
      ColumnSelectorFactory... groups
  )
  {
    final IntSupplier currentGroupSupplier = () -> currentGroup[0];
    final LongSupplier rowIdSupplier = () -> rowId[0];
    return new MergingColumnSelectorFactory(
        groups,
        clusteringColumns,
        clusteringValuesByGroup,
        queryVirtualColumns,
        currentGroupSupplier,
        rowIdSupplier
    );
  }

  @Test
  void testGetColumnCapabilitiesStripsDictionaryEncoding()
  {
    // The representative group advertises "v" as a dictionary-encoded string with bitmap indexes; because per-group
    // dictionary ids are not stable across the merged stream, the factory must strip those flags (forcing value-based
    // grouping) while keeping the type.
    final MergingColumnSelectorFactory factory = factory(groupFactory("g0"), groupFactory("g1"));
    final ColumnCapabilities caps = factory.getColumnCapabilities("v");
    Assertions.assertNotNull(caps);
    Assertions.assertTrue(caps.is(ValueType.STRING));
    Assertions.assertTrue(caps.isDictionaryEncoded().isFalse());
    Assertions.assertTrue(caps.areDictionaryValuesSorted().isFalse());
    Assertions.assertTrue(caps.areDictionaryValuesUnique().isFalse());
    Assertions.assertFalse(caps.hasBitmapIndexes());
  }

  @Test
  void testGetColumnCapabilitiesNullWhenRepresentativeHasNone()
  {
    // Unknown column -> representative returns null -> factory returns null (caller skips it).
    final MergingColumnSelectorFactory factory = factory(groupFactory("g0"));
    Assertions.assertNull(factory.getColumnCapabilities("nope"));
    // No non-null group factory at all -> null capabilities.
    Assertions.assertNull(factory(new ColumnSelectorFactory[]{null}).getColumnCapabilities("v"));
  }

  @Test
  void testColumnValueSelectorDispatchesToCurrentGroup()
  {
    final MergingColumnSelectorFactory factory = factory(groupFactory("g0"), groupFactory("g1"));
    final ColumnValueSelector selector = factory.makeColumnValueSelector("v");
    currentGroup[0] = 0;
    Assertions.assertEquals("g0", selector.getObject());
    currentGroup[0] = 1;
    Assertions.assertEquals("g1", selector.getObject());
    currentGroup[0] = 0;
    Assertions.assertEquals("g0", selector.getObject());
  }

  @Test
  void testDimensionSelectorDispatchesAndReportsUnstableDictionary()
  {
    final MergingColumnSelectorFactory factory = factory(groupFactory("g0"), groupFactory("g1"));
    final DimensionSelector selector = factory.makeDimensionSelector(DefaultDimensionSpec.of("v"));

    currentGroup[0] = 0;
    Assertions.assertEquals("g0", selector.getObject());
    Assertions.assertEquals("g0", selector.lookupName(selector.getRow().get(0)));
    currentGroup[0] = 1;
    Assertions.assertEquals("g1", selector.getObject());

    // Cross-group dictionary instability is advertised so engines use value-based grouping.
    Assertions.assertEquals(DimensionDictionarySelector.CARDINALITY_UNKNOWN, selector.getValueCardinality());
    Assertions.assertNull(selector.idLookup());
    Assertions.assertFalse(selector.nameLookupPossibleInAdvance());
  }

  @Test
  void testClusteringColumnExposedAsWinningGroupConstant()
  {
    // Clustering columns are not read from the per-group cursors (which may not carry them); the factory injects the
    // winning group's clustering value. Verify both the value selector and dimension selector track the current group.
    final RowSignature clusteringColumns = RowSignature.builder().add("tenant", ColumnType.STRING).build();
    final List<Object[]> clusteringValuesByGroup = List.of(new Object[]{"acme"}, new Object[]{"globex"});
    final MergingColumnSelectorFactory factory =
        factory(clusteringColumns, clusteringValuesByGroup, groupFactory("g0"), groupFactory("g1"));

    final ColumnValueSelector valueSelector = factory.makeColumnValueSelector("tenant");
    final DimensionSelector dimensionSelector = factory.makeDimensionSelector(DefaultDimensionSpec.of("tenant"));
    currentGroup[0] = 0;
    Assertions.assertEquals("acme", valueSelector.getObject());
    Assertions.assertEquals("acme", dimensionSelector.getObject());
    currentGroup[0] = 1;
    Assertions.assertEquals("globex", valueSelector.getObject());
    Assertions.assertEquals("globex", dimensionSelector.getObject());

    // Clustering capabilities are type-based and never dictionary-encoded across the merge.
    final ColumnCapabilities caps = factory.getColumnCapabilities("tenant");
    Assertions.assertNotNull(caps);
    Assertions.assertTrue(caps.is(ValueType.STRING));
    Assertions.assertTrue(caps.isDictionaryEncoded().isFalse());
    Assertions.assertEquals(DimensionDictionarySelector.CARDINALITY_UNKNOWN, dimensionSelector.getValueCardinality());
  }

  @Test
  void testNumericClusteringColumnExposedAsWinningGroupConstant()
  {
    final RowSignature clusteringColumns = RowSignature.builder().add("priority", ColumnType.LONG).build();
    final List<Object[]> clusteringValuesByGroup = List.of(new Object[]{5L}, new Object[]{9L});
    final MergingColumnSelectorFactory factory =
        factory(clusteringColumns, clusteringValuesByGroup, groupFactory("g0"), groupFactory("g1"));

    final ColumnValueSelector selector = factory.makeColumnValueSelector("priority");
    currentGroup[0] = 0;
    Assertions.assertEquals(5L, selector.getLong());
    Assertions.assertEquals(5L, selector.getObject());
    currentGroup[0] = 1;
    Assertions.assertEquals(9L, selector.getLong());

    Assertions.assertTrue(factory.getColumnCapabilities("priority").is(ValueType.LONG));
  }

  @Test
  void testClusteringColumnShadowedByQueryVcDispatchesToGroup()
  {
    // A query virtual column whose output name equals a clustering column must NOT be served as the group's clustering
    // constant; it dispatches to the winning group (whose factory resolves the VC), mirroring
    // ClusteringColumnSelectorFactory. The stub group factory answers "g0"/"g1" for any column, standing in for the
    // computed VC value; the assertion is that we get the dispatched value, not the "acme"/"globex" constant.
    final RowSignature clusteringColumns = RowSignature.builder().add("tenant", ColumnType.STRING).build();
    final List<Object[]> clusteringValuesByGroup = List.of(new Object[]{"acme"}, new Object[]{"globex"});
    final VirtualColumns shadowingVc = VirtualColumns.create(
        new ExpressionVirtualColumn("tenant", "concat('t_', m)", ColumnType.STRING, ExprMacroTable.nil())
    );
    final MergingColumnSelectorFactory factory =
        factory(clusteringColumns, clusteringValuesByGroup, shadowingVc, groupFactory("g0"), groupFactory("g1"));

    final ColumnValueSelector valueSelector = factory.makeColumnValueSelector("tenant");
    final DimensionSelector dimensionSelector = factory.makeDimensionSelector(DefaultDimensionSpec.of("tenant"));
    currentGroup[0] = 0;
    Assertions.assertEquals("g0", valueSelector.getObject());
    Assertions.assertEquals("g0", dimensionSelector.getObject());
    currentGroup[0] = 1;
    Assertions.assertEquals("g1", valueSelector.getObject());
    Assertions.assertEquals("g1", dimensionSelector.getObject());
  }

  @Test
  void testRowIdSupplierReflectsMintedOutputRowId()
  {
    final MergingColumnSelectorFactory factory = factory(groupFactory("g0"));
    final RowIdSupplier supplier = factory.getRowIdSupplier();
    Assertions.assertNotNull(supplier);
    rowId[0] = 0;
    Assertions.assertEquals(0L, supplier.getRowId());
    rowId[0] = 7;
    Assertions.assertEquals(7L, supplier.getRowId());
  }

  /**
   * A stub group factory that reports column "v" as a dictionary-encoded string (to exercise stripping) and returns
   * constant selectors carrying {@code tag} (to verify per-group dispatch).
   */
  private static ColumnSelectorFactory groupFactory(String tag)
  {
    return new ColumnSelectorFactory()
    {
      @Override
      public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
      {
        return DimensionSelector.constant(tag);
      }

      @Override
      public ColumnValueSelector makeColumnValueSelector(String columnName)
      {
        return new TestObjectColumnSelector<String>()
        {
          @Override
          public Class<String> classOfObject()
          {
            return String.class;
          }

          @Nullable
          @Override
          public String getObject()
          {
            return tag;
          }
        };
      }

      @Nullable
      @Override
      public ColumnCapabilities getColumnCapabilities(String column)
      {
        if (!"v".equals(column)) {
          return null;
        }
        return ColumnCapabilitiesImpl.createSimpleSingleValueStringColumnCapabilities()
                                     .setDictionaryEncoded(true)
                                     .setDictionaryValuesSorted(true)
                                     .setDictionaryValuesUnique(true)
                                     .setHasBitmapIndexes(true);
      }
    };
  }
}
