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

import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.RowIdSupplier;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

class SingleGroupClusteringColumnSelectorFactoryTest
{
  private static final RowSignature CLUSTERING = RowSignature.builder().add("tenant", ColumnType.STRING).build();

  @Test
  void testNonClusteringColumnPreservesDictionaryEncodedCapabilities()
  {
    final ColumnCapabilities dictEncoded =
        ColumnCapabilitiesImpl.createSimpleSingleValueStringColumnCapabilities()
                              .setDictionaryEncoded(true)
                              .setDictionaryValuesUnique(true)
                              .setDictionaryValuesSorted(true);
    final RecordingDelegate delegate = new RecordingDelegate(dictEncoded);
    final SingleGroupClusteringColumnSelectorFactory f =
        new SingleGroupClusteringColumnSelectorFactory(delegate, CLUSTERING, new Object[]{"acme"});

    final ColumnCapabilities caps = f.getColumnCapabilities("region");
    Assertions.assertNotNull(caps);
    Assertions.assertTrue(caps.isDictionaryEncoded().isTrue());
    Assertions.assertEquals("region", delegate.lastCapabilitiesColumn);
  }

  @Test
  void testNonClusteringColumnSelectorsDelegatedDirectly()
  {
    final RecordingDelegate delegate = new RecordingDelegate(null);
    final SingleGroupClusteringColumnSelectorFactory f =
        new SingleGroupClusteringColumnSelectorFactory(delegate, CLUSTERING, new Object[]{"acme"});

    // No wrapper / generation indirection: the delegate is consulted immediately on selector creation.
    final DimensionSelector dim = f.makeDimensionSelector(DefaultDimensionSpec.of("region"));
    Assertions.assertEquals("region", delegate.lastDimSelectorName);
    Assertions.assertEquals("delegated:region", dim.lookupName(0));

    f.makeColumnValueSelector("metric");
    Assertions.assertEquals("metric", delegate.lastValueSelectorName);
  }

  @Test
  void testClusteringColumnReturnsConstantWithoutHittingDelegate()
  {
    final RecordingDelegate delegate = new RecordingDelegate(null);
    final SingleGroupClusteringColumnSelectorFactory f =
        new SingleGroupClusteringColumnSelectorFactory(delegate, CLUSTERING, new Object[]{"acme"});

    final DimensionSelector dim = f.makeDimensionSelector(DefaultDimensionSpec.of("tenant"));
    Assertions.assertEquals("acme", dim.lookupName(dim.getRow().get(0)));
    Assertions.assertNull(delegate.lastDimSelectorName, "delegate must not be hit for clustering columns");

    final ColumnValueSelector val = f.makeColumnValueSelector("tenant");
    Assertions.assertEquals("acme", val.getObject());

    // A single group's clustering value is a constant 1-entry dictionary, so it is reported as dictionary-encoded /
    // sorted / unique to route grouping through the dictionary-id path.
    final ColumnCapabilities caps = f.getColumnCapabilities("tenant");
    Assertions.assertNotNull(caps);
    Assertions.assertTrue(caps.is(ValueType.STRING));
    Assertions.assertTrue(caps.isDictionaryEncoded().isTrue());
    Assertions.assertTrue(caps.areDictionaryValuesSorted().isTrue());
    Assertions.assertTrue(caps.areDictionaryValuesUnique().isTrue());
  }

  private static class RecordingDelegate implements ColumnSelectorFactory
  {
    @Nullable
    private final ColumnCapabilities capabilities;
    String lastDimSelectorName;
    String lastValueSelectorName;
    String lastCapabilitiesColumn;

    RecordingDelegate(@Nullable ColumnCapabilities capabilities)
    {
      this.capabilities = capabilities;
    }

    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      lastDimSelectorName = dimensionSpec.getDimension();
      return DimensionSelector.constant("delegated:" + dimensionSpec.getDimension());
    }

    @Override
    public ColumnValueSelector makeColumnValueSelector(String columnName)
    {
      lastValueSelectorName = columnName;
      return NilColumnValueSelector.instance();
    }

    @Nullable
    @Override
    public ColumnCapabilities getColumnCapabilities(String column)
    {
      lastCapabilitiesColumn = column;
      return capabilities;
    }

    @Nullable
    @Override
    public RowIdSupplier getRowIdSupplier()
    {
      return null;
    }
  }
}
