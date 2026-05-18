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

import org.apache.druid.error.DruidException;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.NoFilterVectorOffset;
import org.apache.druid.segment.vector.ReadableVectorInspector;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

class ClusteringVectorColumnSelectorFactoryTest
{
  private static final RowSignature CLUSTER_SIGNATURE = RowSignature.builder().add("tenant", ColumnType.STRING).build();

  @Test
  void testStringClusteringSingleValueDimensionSelector()
  {
    StubDelegate delegate = new StubDelegate(inspectorFor(8));
    ClusteringVectorColumnSelectorFactory f = new ClusteringVectorColumnSelectorFactory(
        delegate,
        CLUSTER_SIGNATURE,
        new Object[]{"acme"}
    );

    SingleValueDimensionVectorSelector sel = f.makeSingleValueDimensionSelector(DefaultDimensionSpec.of("tenant"));
    Assertions.assertNull(delegate.lastSingleValDimRequest, "delegate must not be hit for clustering column");
    Assertions.assertEquals("acme", sel.lookupName(0));
    Assertions.assertEquals(1, sel.getValueCardinality());
  }

  @Test
  void testLongClusteringValueSelector()
  {
    StubDelegate delegate = new StubDelegate(inspectorFor(4));
    ClusteringVectorColumnSelectorFactory f = new ClusteringVectorColumnSelectorFactory(
        delegate,
        RowSignature.builder().add("priority", ColumnType.LONG).build(),
        new Object[]{42L}
    );

    VectorValueSelector sel = f.makeValueSelector("priority");
    Assertions.assertNull(delegate.lastValueRequest);
    long[] vec = sel.getLongVector();
    for (long v : vec) {
      Assertions.assertEquals(42L, v);
    }
  }

  @Test
  void testDoubleClusteringObjectSelector()
  {
    StubDelegate delegate = new StubDelegate(inspectorFor(4));
    ClusteringVectorColumnSelectorFactory f = new ClusteringVectorColumnSelectorFactory(
        delegate,
        RowSignature.builder().add("price", ColumnType.DOUBLE).build(),
        new Object[]{3.14}
    );

    VectorObjectSelector sel = f.makeObjectSelector("price");
    Object[] vec = sel.getObjectVector();
    for (Object v : vec) {
      Assertions.assertEquals(3.14, v);
    }
  }

  @Test
  void testFloatClusteringValueSelector()
  {
    StubDelegate delegate = new StubDelegate(inspectorFor(4));
    ClusteringVectorColumnSelectorFactory f = new ClusteringVectorColumnSelectorFactory(
        delegate,
        RowSignature.builder().add("ratio", ColumnType.FLOAT).build(),
        new Object[]{0.5f}
    );

    VectorValueSelector sel = f.makeValueSelector("ratio");
    float[] vec = sel.getFloatVector();
    for (float v : vec) {
      Assertions.assertEquals(0.5f, v);
    }
  }

  @Test
  void testNullStringClusteringValueDimensionSelectorIsNil()
  {
    StubDelegate delegate = new StubDelegate(inspectorFor(4));
    ClusteringVectorColumnSelectorFactory f = new ClusteringVectorColumnSelectorFactory(
        delegate,
        CLUSTER_SIGNATURE,
        new Object[]{null}
    );

    SingleValueDimensionVectorSelector sel = f.makeSingleValueDimensionSelector(DefaultDimensionSpec.of("tenant"));
    // Nil vector selector returns null on lookupName(0) regardless of id.
    Assertions.assertNull(sel.lookupName(0));
  }

  @Test
  void testNonClusteringColumnDelegated()
  {
    StubDelegate delegate = new StubDelegate(inspectorFor(4));
    ClusteringVectorColumnSelectorFactory f = new ClusteringVectorColumnSelectorFactory(
        delegate,
        CLUSTER_SIGNATURE,
        new Object[]{"acme"}
    );

    // Non-clustering selectors are wrapped in lazy delegating wrappers; the delegate is only consulted on first
    // use, so a multi-group ConcatenatingVectorCursor can swap delegates between groups without recreating the
    // selector instance.
    SingleValueDimensionVectorSelector svdSel =
        f.makeSingleValueDimensionSelector(DefaultDimensionSpec.of("region"));
    Assertions.assertNull(delegate.lastSingleValDimRequest, "delegate must not be hit until selector is used");
    try {
      svdSel.getRowVector();
    }
    catch (NullPointerException expected) {
      // StubDelegate returns null for the inner selector; the wrapper forwards to it. We just want to confirm
      // the delegate's makeSingleValueDimensionSelector was invoked.
    }
    Assertions.assertEquals("region", delegate.lastSingleValDimRequest);

    VectorValueSelector vvSel = f.makeValueSelector("metric");
    Assertions.assertNull(delegate.lastValueRequest);
    try {
      vvSel.getLongVector();
    }
    catch (NullPointerException expected) {
      // same ^, confirming the delegate was reached
    }
    Assertions.assertEquals("metric", delegate.lastValueRequest);

    VectorObjectSelector voSel = f.makeObjectSelector("region");
    try {
      voSel.getObjectVector();
    }
    catch (NullPointerException expected) {
      // same
    }
    Assertions.assertEquals("region", delegate.lastObjectRequest);

    // getColumnCapabilities is NOT lazy; it returns the result directly.
    f.getColumnCapabilities("metric");
    Assertions.assertEquals("metric", delegate.lastCapsRequest);
  }

  @Test
  void testSetDelegateUpdatesClusteringValueOnExistingSelector()
  {
    StubDelegate firstDelegate = new StubDelegate(inspectorFor(4));
    ClusteringVectorColumnSelectorFactory f = new ClusteringVectorColumnSelectorFactory(
        firstDelegate,
        RowSignature.builder().add("priority", ColumnType.LONG).build(),
        new Object[]{5L}
    );

    VectorValueSelector sel = f.makeValueSelector("priority");
    long[] firstVec = sel.getLongVector();
    for (long v : firstVec) {
      Assertions.assertEquals(5L, v);
    }

    // Simulate group transition.
    StubDelegate secondDelegate = new StubDelegate(inspectorFor(4));
    f.setDelegate(secondDelegate, new Object[]{42L});

    long[] secondVec = sel.getLongVector();
    for (long v : secondVec) {
      Assertions.assertEquals(42L, v);
    }
  }

  @Test
  void testSetDelegateUpdatesNonClusteringSelector()
  {
    StubDelegate first = new StubDelegate(inspectorFor(4));
    ClusteringVectorColumnSelectorFactory f = new ClusteringVectorColumnSelectorFactory(
        first,
        CLUSTER_SIGNATURE,
        new Object[]{"acme"}
    );

    SingleValueDimensionVectorSelector sel =
        f.makeSingleValueDimensionSelector(DefaultDimensionSpec.of("region"));
    try {
      sel.getRowVector();   // warms the cache against the first delegate
    }
    catch (NullPointerException expected) {
      // expected; just confirming the route
    }
    Assertions.assertEquals("region", first.lastSingleValDimRequest);

    StubDelegate second = new StubDelegate(inspectorFor(4));
    f.setDelegate(second, new Object[]{"globex"});

    try {
      sel.getRowVector();   // generation bumped → re-fetches against second delegate
    }
    catch (NullPointerException expected) {
      // expected
    }
    Assertions.assertEquals("region", second.lastSingleValDimRequest);
  }

  @Test
  void testGetColumnCapabilitiesForClusteringColumns()
  {
    StubDelegate delegate = new StubDelegate(inspectorFor(4));
    ClusteringVectorColumnSelectorFactory f = new ClusteringVectorColumnSelectorFactory(
        delegate,
        RowSignature.builder()
                    .add("tenant", ColumnType.STRING)
                    .add("priority", ColumnType.LONG)
                    .build(),
        new Object[]{"acme", 5L}
    );

    ColumnCapabilities tenantCaps = f.getColumnCapabilities("tenant");
    Assertions.assertTrue(tenantCaps.is(ValueType.STRING));

    ColumnCapabilities priorityCaps = f.getColumnCapabilities("priority");
    Assertions.assertTrue(priorityCaps.is(ValueType.LONG));

    Assertions.assertNull(delegate.lastCapsRequest, "delegate must not be hit for clustering capabilities");
  }

  @Test
  void testMultiValueDimensionSelectorOnClusteringRejected()
  {
    ClusteringVectorColumnSelectorFactory f = new ClusteringVectorColumnSelectorFactory(
        new StubDelegate(inspectorFor(4)),
        CLUSTER_SIGNATURE,
        new Object[]{"acme"}
    );
    Assertions.assertThrows(
        DruidException.class,
        () -> f.makeMultiValueDimensionSelector(DefaultDimensionSpec.of("tenant"))
    );
  }

  private static ReadableVectorInspector inspectorFor(int size)
  {
    return new NoFilterVectorOffset(size, 0, size);
  }

  private static class StubDelegate implements VectorColumnSelectorFactory
  {
    final ReadableVectorInspector inspector;
    String lastSingleValDimRequest;
    String lastValueRequest;
    String lastObjectRequest;
    String lastCapsRequest;

    StubDelegate(ReadableVectorInspector inspector)
    {
      this.inspector = inspector;
    }

    @Override
    public ReadableVectorInspector getReadableVectorInspector()
    {
      return inspector;
    }

    @Override
    public SingleValueDimensionVectorSelector makeSingleValueDimensionSelector(
        org.apache.druid.query.dimension.DimensionSpec dimensionSpec
    )
    {
      lastSingleValDimRequest = dimensionSpec.getDimension();
      return null;
    }

    @Override
    public MultiValueDimensionVectorSelector makeMultiValueDimensionSelector(
        org.apache.druid.query.dimension.DimensionSpec dimensionSpec
    )
    {
      throw new UnsupportedOperationException("not used");
    }

    @Override
    public VectorValueSelector makeValueSelector(String column)
    {
      lastValueRequest = column;
      return null;
    }

    @Override
    public VectorObjectSelector makeObjectSelector(String column)
    {
      lastObjectRequest = column;
      return null;
    }

    @Nullable
    @Override
    public ColumnCapabilities getColumnCapabilities(String column)
    {
      lastCapsRequest = column;
      return null;
    }
  }
}
