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
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionDictionarySelector;
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

class ClusteringColumnSelectorFactoryTest
{
  private static final RowSignature SIGNATURE = RowSignature.builder().add("tenant", ColumnType.STRING).build();

  @Test
  void testStringClusteringColumnDimensionSelector()
  {
    RecordingDelegate delegate = new RecordingDelegate();
    ClusteringColumnSelectorFactory f = new ClusteringColumnSelectorFactory(
        delegate,
        SIGNATURE,
        new Object[]{"acme"}
    );

    DimensionSelector sel = f.makeDimensionSelector(DefaultDimensionSpec.of("tenant"));
    Assertions.assertEquals("acme", sel.lookupName(0));
    Assertions.assertNull(delegate.lastDimSelectorName, "delegate must not be hit for clustering columns");
  }

  @Test
  void testStringClusteringColumnValueSelector()
  {
    ClusteringColumnSelectorFactory f = new ClusteringColumnSelectorFactory(
        new RecordingDelegate(),
        SIGNATURE,
        new Object[]{"acme"}
    );

    ColumnValueSelector sel = f.makeColumnValueSelector("tenant");
    Assertions.assertEquals("acme", sel.getObject());
    // Non-numeric string: ExprEval reports it as numerically null (matches a real string column read via getLong).
    Assertions.assertTrue(sel.isNull());
  }

  @Test
  void testLongClusteringColumnValueSelectorTypedMethods()
  {
    ClusteringColumnSelectorFactory f = new ClusteringColumnSelectorFactory(
        new RecordingDelegate(),
        RowSignature.builder().add("priority", ColumnType.LONG).build(),
        new Object[]{42L}
    );

    ColumnValueSelector sel = f.makeColumnValueSelector("priority");
    Assertions.assertEquals(42L, sel.getLong());
    Assertions.assertEquals(42.0, sel.getDouble());
    Assertions.assertEquals(42.0f, sel.getFloat());
    Assertions.assertEquals(42L, sel.getObject());
    Assertions.assertFalse(sel.isNull());
  }

  @Test
  void testDoubleClusteringColumnValueSelector()
  {
    ClusteringColumnSelectorFactory f = new ClusteringColumnSelectorFactory(
        new RecordingDelegate(),
        RowSignature.builder().add("price", ColumnType.DOUBLE).build(),
        new Object[]{3.14}
    );

    ColumnValueSelector sel = f.makeColumnValueSelector("price");
    Assertions.assertEquals(3.14, sel.getDouble());
    Assertions.assertEquals(3.14, sel.getObject());
  }

  @Test
  void testFloatClusteringColumnValueSelector()
  {
    ClusteringColumnSelectorFactory f = new ClusteringColumnSelectorFactory(
        new RecordingDelegate(),
        RowSignature.builder().add("ratio", ColumnType.FLOAT).build(),
        new Object[]{0.5f}
    );

    ColumnValueSelector sel = f.makeColumnValueSelector("ratio");
    Assertions.assertEquals(0.5f, sel.getFloat());
    // ExpressionType collapses FLOAT to DOUBLE, so the unwrapped value is a Double.
    Assertions.assertEquals(0.5d, sel.getObject());
  }

  @Test
  void testNullClusteringValueDimensionSelectorIsNil()
  {
    ClusteringColumnSelectorFactory f = new ClusteringColumnSelectorFactory(
        new RecordingDelegate(),
        SIGNATURE,
        new Object[]{null}
    );

    DimensionSelector sel = f.makeDimensionSelector(DefaultDimensionSpec.of("tenant"));
    Assertions.assertNull(sel.lookupName(sel.getRow().get(0)));
  }

  @Test
  void testNullClusteringValueValueSelectorIsNull()
  {
    ClusteringColumnSelectorFactory f = new ClusteringColumnSelectorFactory(
        new RecordingDelegate(),
        RowSignature.builder().add("priority", ColumnType.LONG).build(),
        new Object[]{null}
    );

    ColumnValueSelector sel = f.makeColumnValueSelector("priority");
    Assertions.assertTrue(sel.isNull());
    Assertions.assertNull(sel.getObject());
  }

  @Test
  void testNonClusteringDimensionSelectorDelegated()
  {
    RecordingDelegate delegate = new RecordingDelegate();
    ClusteringColumnSelectorFactory f = new ClusteringColumnSelectorFactory(
        delegate,
        SIGNATURE,
        new Object[]{"acme"}
    );

    // Non-clustering selectors are wrapped in a lazy delegating wrapper; the delegate is only consulted on first
    // use (this enables ConcatenatingCursor to swap delegates between cluster groups without recreating selectors).
    DimensionSelector sel = f.makeDimensionSelector(DefaultDimensionSpec.of("region"));
    Assertions.assertNull(delegate.lastDimSelectorName, "delegate must not be hit until selector is used");

    Assertions.assertEquals("delegated:region", sel.lookupName(0));
    Assertions.assertEquals("region", delegate.lastDimSelectorName);
  }

  @Test
  void testNonClusteringValueSelectorDelegated()
  {
    RecordingDelegate delegate = new RecordingDelegate();
    ClusteringColumnSelectorFactory f = new ClusteringColumnSelectorFactory(
        delegate,
        SIGNATURE,
        new Object[]{"acme"}
    );

    ColumnValueSelector sel = f.makeColumnValueSelector("region");
    Assertions.assertNull(delegate.lastValueSelectorName, "delegate must not be hit until selector is used");

    sel.getObject();
    Assertions.assertEquals("region", delegate.lastValueSelectorName);
  }

  @Test
  void testNonClusteringSelectorObservesNewDelegateAfterSetDelegate()
  {
    RecordingDelegate first = new RecordingDelegate();
    ClusteringColumnSelectorFactory f = new ClusteringColumnSelectorFactory(
        first,
        SIGNATURE,
        new Object[]{"acme"}
    );

    DimensionSelector sel = f.makeDimensionSelector(DefaultDimensionSpec.of("region"));
    Assertions.assertEquals("delegated:region", sel.lookupName(0));   // routed to first delegate

    RecordingDelegate second = new RecordingDelegate();
    f.setDelegate(second, new Object[]{"globex"});

    // Same selector instance, now backed by the new delegate's makeDimensionSelector result.
    Assertions.assertEquals("delegated:region", sel.lookupName(0));
    Assertions.assertEquals("region", second.lastDimSelectorName);
  }

  @Test
  void testGetColumnCapabilitiesForClusteringColumns()
  {
    ClusteringColumnSelectorFactory f = new ClusteringColumnSelectorFactory(
        new RecordingDelegate(),
        RowSignature.builder()
                    .add("tenant", ColumnType.STRING)
                    .add("priority", ColumnType.LONG)
                    .build(),
        new Object[]{"acme", 5L}
    );

    ColumnCapabilities tenantCaps = f.getColumnCapabilities("tenant");
    Assertions.assertEquals(ColumnType.STRING, ColumnType.fromCapabilities(tenantCaps));

    ColumnCapabilities priorityCaps = f.getColumnCapabilities("priority");
    Assertions.assertEquals(ColumnType.LONG, ColumnType.fromCapabilities(priorityCaps));
  }

  @Test
  void testGetColumnCapabilitiesForNonClusteringDelegated()
  {
    RecordingDelegate delegate = new RecordingDelegate();
    ClusteringColumnSelectorFactory f = new ClusteringColumnSelectorFactory(
        delegate,
        SIGNATURE,
        new Object[]{"acme"}
    );

    f.getColumnCapabilities("metric");
    Assertions.assertEquals("metric", delegate.lastCapabilitiesColumn);
  }

  @Test
  void testSetDelegateUpdatesClusteringValues()
  {
    ClusteringColumnSelectorFactory f = new ClusteringColumnSelectorFactory(
        new RecordingDelegate(),
        SIGNATURE,
        new Object[]{"acme"}
    );

    // Acquire a selector tied to the first group's value.
    ColumnValueSelector sel = f.makeColumnValueSelector("tenant");
    Assertions.assertEquals("acme", sel.getObject());

    // Simulate group transition.
    f.setDelegate(new RecordingDelegate(), new Object[]{"globex"});

    // The same selector should observe the new value via the generation-counter cache invalidation.
    Assertions.assertEquals("globex", sel.getObject());
  }

  @Test
  void testSetDelegateUpdatesDimensionSelector()
  {
    ClusteringColumnSelectorFactory f = new ClusteringColumnSelectorFactory(
        new RecordingDelegate(),
        SIGNATURE,
        new Object[]{"acme"}
    );

    DimensionSelector sel = f.makeDimensionSelector(DefaultDimensionSpec.of("tenant"));
    Assertions.assertEquals("acme", sel.lookupName(sel.getRow().get(0)));

    f.setDelegate(new RecordingDelegate(), new Object[]{"globex"});

    Assertions.assertEquals("globex", sel.lookupName(sel.getRow().get(0)));
  }

  @Test
  void testClusteringMatcherObservesGroupTransition()
  {
    // The clustering-column path: a matcher built before the group transition still gives the right verdict
    // afterwards because it re-resolves through the generation-aware wrapper.
    ClusteringColumnSelectorFactory f = new ClusteringColumnSelectorFactory(
        new RecordingDelegate(),
        SIGNATURE,
        new Object[]{"acme"}
    );
    DimensionSelector sel = f.makeDimensionSelector(DefaultDimensionSpec.of("tenant"));
    org.apache.druid.query.filter.ValueMatcher acmeMatcher = sel.makeValueMatcher("acme");

    // Pre-transition: the constant clustering value is "acme"; matcher returns true.
    Assertions.assertTrue(acmeMatcher.matches(false));

    f.setDelegate(new RecordingDelegate(), new Object[]{"globex"});

    // Post-transition: the constant is now "globex"; the same matcher (held across the transition) returns false.
    Assertions.assertFalse(acmeMatcher.matches(false));
  }

  @Test
  void testNonClusteringDelegatingMatcherObservesGroupTransition()
  {
    // The non-clustering "delegating" path: a matcher built before the transition still routes through the
    // current delegate's matcher afterwards. We use two RecordingDelegates whose makeDimensionSelector returns
    // selectors with predictable lookupName, then drive the matcher across a delegate swap.
    final RecordingDelegate first = new RecordingDelegate();
    ClusteringColumnSelectorFactory f = new ClusteringColumnSelectorFactory(
        first,
        SIGNATURE,
        new Object[]{"acme"}
    );
    DimensionSelector sel = f.makeDimensionSelector(DefaultDimensionSpec.of("region"));
    // Force the inner selector to materialize so RecordingDelegate.lastDimSelectorName is "region".
    sel.lookupName(0);
    org.apache.druid.query.filter.ValueMatcher matcher = sel.makeValueMatcher("delegated:region");

    // Pre-transition: RecordingDelegate's stub selector returns "delegated:region" for any id, so the matcher
    // is true.
    Assertions.assertTrue(matcher.matches(false));

    final RecordingDelegate second = new RecordingDelegate();
    f.setDelegate(second, new Object[]{"acme"});

    // Post-transition: still true (RecordingDelegate's stub is identical), but the important thing is that
    // calling matches() now invokes the second delegate's selector (verified by lastDimSelectorName getting set).
    Assertions.assertTrue(matcher.matches(false));
    Assertions.assertEquals("region", second.lastDimSelectorName);
  }

  @Test
  void testClusteringColumnDimensionSelectorForcesValueBasedGrouping()
  {
    // Across a concatenating multi-group cursor the clustering column's per-group constant id (always 0) is not
    // stable: id 0 means a different clustering value in each group. The selector must therefore NOT advertise
    // dictionary-encoded grouping, otherwise the group-by engine keys on the per-group id and silently collapses
    // every group into one bucket.
    ClusteringColumnSelectorFactory f = new ClusteringColumnSelectorFactory(
        new RecordingDelegate(),
        SIGNATURE,
        new Object[]{"acme"}
    );
    DimensionSelector sel = f.makeDimensionSelector(DefaultDimensionSpec.of("tenant"));
    Assertions.assertEquals(DimensionDictionarySelector.CARDINALITY_UNKNOWN, sel.getValueCardinality());
    Assertions.assertFalse(sel.nameLookupPossibleInAdvance());
    Assertions.assertNull(sel.idLookup());
    // Value resolution still works per row against the current group's constant.
    Assertions.assertEquals("acme", sel.lookupName(sel.getRow().get(0)));
  }

  @Test
  void testNonClusteringDelegatingDimensionSelectorForcesValueBasedGrouping()
  {
    // The non-clustering (delegating) path has the same cross-group id-instability and must also force value-based
    // grouping, even though its delegate here (a constant selector) would otherwise report a stable dictionary.
    ClusteringColumnSelectorFactory f = new ClusteringColumnSelectorFactory(
        new RecordingDelegate(),
        SIGNATURE,
        new Object[]{"acme"}
    );
    DimensionSelector sel = f.makeDimensionSelector(DefaultDimensionSpec.of("region"));
    Assertions.assertEquals(DimensionDictionarySelector.CARDINALITY_UNKNOWN, sel.getValueCardinality());
    Assertions.assertFalse(sel.nameLookupPossibleInAdvance());
    Assertions.assertNull(sel.idLookup());
  }

  @Test
  void testStringDimensionCapabilitiesFlavorIsSingleValue()
  {
    ClusteringColumnSelectorFactory f = new ClusteringColumnSelectorFactory(
        new RecordingDelegate(),
        SIGNATURE,
        new Object[]{"acme"}
    );
    ColumnCapabilities caps = f.getColumnCapabilities("tenant");
    // Sanity: the simple-string-capabilities helper returns a STRING type with no multi-value flag set.
    Assertions.assertNotNull(caps);
    Assertions.assertTrue(caps.is(ValueType.STRING));
    Assertions.assertEquals(
        ColumnCapabilities.Capable.FALSE,
        caps.hasMultipleValues()
    );
    Assertions.assertEquals(
        ColumnCapabilitiesImpl.createSimpleSingleValueStringColumnCapabilities().getType(),
        caps.getType()
    );
  }

  /**
   * Stub delegate that returns nothing; used to verify that calls for clustering columns are intercepted before
   * ever reaching it. For non-clustering columns, methods record the requested name and return Nil/null shapes.
   */
  private static class RecordingDelegate implements ColumnSelectorFactory
  {
    String lastDimSelectorName;
    String lastValueSelectorName;
    String lastCapabilitiesColumn;

    @Override
    public DimensionSelector makeDimensionSelector(org.apache.druid.query.dimension.DimensionSpec dimensionSpec)
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
      return null;
    }

    @Nullable
    @Override
    public RowIdSupplier getRowIdSupplier()
    {
      return null;
    }
  }
}
