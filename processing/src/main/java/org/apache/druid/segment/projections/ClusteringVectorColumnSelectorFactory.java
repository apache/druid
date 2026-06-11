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
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.vector.ConstantVectorSelectors;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.ReadableVectorInspector;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;

/**
 * Vectorized counterpart of {@link ClusteringColumnSelectorFactory}. Wraps a delegate
 * {@link VectorColumnSelectorFactory} and intercepts requests for clustering columns, returning constant-typed
 * vector selectors (via {@link ConstantVectorSelectors}). Other column requests pass through to delegating wrappers.
 *
 * The factory is mutable via {@link #setDelegate(VectorColumnSelectorFactory, Object[])}: a multi-group
 * {@code ConcatenatingVectorCursor} swaps the underlying delegate + clustering values on each group transition.
 * Selectors previously returned by this factory observe the new state on their next call thanks to a generation
 * counter cache invalidation.
 *
 * For single-group dispatch, the factory is constructed once and {@code setDelegate} is never called; selectors'
 * caches fill on first access and never invalidate.
 */
public class ClusteringVectorColumnSelectorFactory implements VectorColumnSelectorFactory
{
  private final RowSignature clusteringColumns;
  private final int maxVectorSize;
  private VectorColumnSelectorFactory delegate;
  private Object[] clusteringValues;
  // Bumped on every setDelegate(...) so per-call selector wrappers can detect group transitions and rebuild their
  // cached inner state.
  private long generation;

  /**
   * Convenience overload that derives {@code maxVectorSize} from the supplied delegate. Used by single-group
   * dispatch where the delegate is the per-group {@link VectorColumnSelectorFactory} from the cursor itself.
   */
  public ClusteringVectorColumnSelectorFactory(
      VectorColumnSelectorFactory delegate,
      RowSignature clusteringColumns,
      Object[] clusteringValues
  )
  {
    this(delegate, clusteringColumns, clusteringValues, delegate.getMaxVectorSize());
  }

  public ClusteringVectorColumnSelectorFactory(
      VectorColumnSelectorFactory delegate,
      RowSignature clusteringColumns,
      Object[] clusteringValues,
      int maxVectorSize
  )
  {
    this.clusteringColumns = clusteringColumns;
    this.maxVectorSize = maxVectorSize;
    setDelegate(delegate, clusteringValues);
  }

  /**
   * Update the underlying delegate and the constant clustering values for the current cluster group. Called by a
   * multi-group {@code ConcatenatingVectorCursor} on each group transition.
   */
  public void setDelegate(VectorColumnSelectorFactory delegate, Object[] clusteringValues)
  {
    if (clusteringValues == null || clusteringValues.length != clusteringColumns.size()) {
      throw DruidException.defensive(
          "clusteringValues length [%s] must match clusteringColumns size [%s]",
          clusteringValues == null ? "null" : clusteringValues.length,
          clusteringColumns.size()
      );
    }
    this.delegate = delegate;
    this.clusteringValues = clusteringValues;
    this.generation++;
  }

  VectorColumnSelectorFactory getDelegate()
  {
    return delegate;
  }

  long getGeneration()
  {
    return generation;
  }

  Object currentValue(int idx)
  {
    return clusteringValues[idx];
  }

  @Override
  public ReadableVectorInspector getReadableVectorInspector()
  {
    return delegate.getReadableVectorInspector();
  }

  @Override
  public int getMaxVectorSize()
  {
    return maxVectorSize;
  }

  @Override
  public SingleValueDimensionVectorSelector makeSingleValueDimensionSelector(DimensionSpec dimensionSpec)
  {
    final int idx = clusteringColumns.indexOf(dimensionSpec.getDimension());
    if (idx < 0) {
      return new DelegatingSingleValueDimensionVectorSelector(this, dimensionSpec);
    }
    return new ClusteringSingleValueDimensionVectorSelector(this, idx, dimensionSpec);
  }

  @Override
  public MultiValueDimensionVectorSelector makeMultiValueDimensionSelector(DimensionSpec dimensionSpec)
  {
    final int idx = clusteringColumns.indexOf(dimensionSpec.getDimension());
    if (idx < 0) {
      return new DelegatingMultiValueDimensionVectorSelector(this, dimensionSpec);
    }
    // Clustering values are single-typed primitives. Multi-value requests on a clustering column shouldn't happen
    // in practice; throw to surface caller bugs rather than silently misbehave.
    throw DruidException.defensive(
        "multi-value vector selector not supported for clustering column [" + dimensionSpec.getDimension() + "]"
    );
  }

  @Override
  public VectorValueSelector makeValueSelector(String column)
  {
    final int idx = clusteringColumns.indexOf(column);
    if (idx < 0) {
      return new DelegatingVectorValueSelector(this, column);
    }
    return new ClusteringVectorValueSelector(this, idx);
  }

  @Override
  public VectorObjectSelector makeObjectSelector(String column)
  {
    final int idx = clusteringColumns.indexOf(column);
    if (idx < 0) {
      return new DelegatingVectorObjectSelector(this, column);
    }
    return new ClusteringVectorObjectSelector(this, idx);
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    final int idx = clusteringColumns.indexOf(column);
    if (idx < 0) {
      return delegate.getColumnCapabilities(column);
    }
    final ColumnType type = clusteringColumns.getColumnType(idx).orElseThrow();
    if (type.is(ValueType.STRING)) {
      return ColumnCapabilitiesImpl.createSimpleSingleValueStringColumnCapabilities();
    }
    return ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(type);
  }

  private static final class ClusteringSingleValueDimensionVectorSelector implements SingleValueDimensionVectorSelector
  {
    private final ClusteringVectorColumnSelectorFactory parent;
    private final int idx;
    private final DimensionSpec spec;
    private long cachedGeneration = -1;
    private SingleValueDimensionVectorSelector cachedInner;

    private ClusteringSingleValueDimensionVectorSelector(
        ClusteringVectorColumnSelectorFactory parent,
        int idx,
        DimensionSpec spec
    )
    {
      this.parent = parent;
      this.idx = idx;
      this.spec = spec;
    }

    private SingleValueDimensionVectorSelector currentInner()
    {
      final long currentGeneration = parent.getGeneration();
      if (cachedGeneration == currentGeneration) {
        return cachedInner;
      }
      final Object raw = parent.currentValue(idx);
      final String stringValue = raw == null ? null : String.valueOf(raw);
      final String afterExtraction =
          spec.getExtractionFn() == null ? stringValue : spec.getExtractionFn().apply(stringValue);
      cachedInner = ConstantVectorSelectors.singleValueDimensionVectorSelector(
          parent.getReadableVectorInspector(),
          afterExtraction
      );
      cachedGeneration = currentGeneration;
      return cachedInner;
    }

    @Override
    public int[] getRowVector()
    {
      return currentInner().getRowVector();
    }

    @Override
    public int getValueCardinality()
    {
      return currentInner().getValueCardinality();
    }

    @Nullable
    @Override
    public String lookupName(int id)
    {
      return currentInner().lookupName(id);
    }

    @Override
    public boolean nameLookupPossibleInAdvance()
    {
      return currentInner().nameLookupPossibleInAdvance();
    }

    @Nullable
    @Override
    public IdLookup idLookup()
    {
      return currentInner().idLookup();
    }

    @Override
    public int getMaxVectorSize()
    {
      return parent.getMaxVectorSize();
    }

    @Override
    public int getCurrentVectorSize()
    {
      return parent.getReadableVectorInspector().getCurrentVectorSize();
    }
  }

  private static final class ClusteringVectorValueSelector implements VectorValueSelector
  {
    private final ClusteringVectorColumnSelectorFactory parent;
    private final int idx;
    private long cachedGeneration = -1;
    private VectorValueSelector cachedInner;

    private ClusteringVectorValueSelector(ClusteringVectorColumnSelectorFactory parent, int idx)
    {
      this.parent = parent;
      this.idx = idx;
    }

    private VectorValueSelector currentInner()
    {
      final long currentGeneration = parent.getGeneration();
      if (cachedGeneration == currentGeneration) {
        return cachedInner;
      }
      final Object raw = parent.currentValue(idx);
      // VectorValueSelector is meant for DOUBLE/FLOAT/LONG. STRING clustering shouldn't reach this method per the
      // VectorColumnSelectorFactory contract; pass null (treated as null-numeric) to be conservative.
      final Number number = (raw instanceof Number) ? (Number) raw : null;
      cachedInner = ConstantVectorSelectors.vectorValueSelector(parent.getReadableVectorInspector(), number);
      cachedGeneration = currentGeneration;
      return cachedInner;
    }

    @Override
    public long[] getLongVector()
    {
      return currentInner().getLongVector();
    }

    @Override
    public float[] getFloatVector()
    {
      return currentInner().getFloatVector();
    }

    @Override
    public double[] getDoubleVector()
    {
      return currentInner().getDoubleVector();
    }

    @Nullable
    @Override
    public boolean[] getNullVector()
    {
      return currentInner().getNullVector();
    }

    @Override
    public int getMaxVectorSize()
    {
      return parent.getMaxVectorSize();
    }

    @Override
    public int getCurrentVectorSize()
    {
      return parent.getReadableVectorInspector().getCurrentVectorSize();
    }
  }

  private static final class ClusteringVectorObjectSelector implements VectorObjectSelector
  {
    private final ClusteringVectorColumnSelectorFactory parent;
    private final int idx;
    private long cachedGeneration = -1;
    private VectorObjectSelector cachedInner;

    private ClusteringVectorObjectSelector(ClusteringVectorColumnSelectorFactory parent, int idx)
    {
      this.parent = parent;
      this.idx = idx;
    }

    private VectorObjectSelector currentInner()
    {
      final long currentGeneration = parent.getGeneration();
      if (cachedGeneration == currentGeneration) {
        return cachedInner;
      }
      cachedInner = ConstantVectorSelectors.vectorObjectSelector(
          parent.getReadableVectorInspector(),
          parent.currentValue(idx)
      );
      cachedGeneration = currentGeneration;
      return cachedInner;
    }

    @Override
    public Object[] getObjectVector()
    {
      return currentInner().getObjectVector();
    }

    @Override
    public int getMaxVectorSize()
    {
      return parent.getMaxVectorSize();
    }

    @Override
    public int getCurrentVectorSize()
    {
      return parent.getReadableVectorInspector().getCurrentVectorSize();
    }
  }

  private static final class DelegatingSingleValueDimensionVectorSelector implements SingleValueDimensionVectorSelector
  {
    private final ClusteringVectorColumnSelectorFactory parent;
    private final DimensionSpec spec;
    private long cachedGeneration = -1;
    private SingleValueDimensionVectorSelector cachedInner;

    private DelegatingSingleValueDimensionVectorSelector(
        ClusteringVectorColumnSelectorFactory parent,
        DimensionSpec spec
    )
    {
      this.parent = parent;
      this.spec = spec;
    }

    private SingleValueDimensionVectorSelector currentInner()
    {
      final long currentGeneration = parent.getGeneration();
      if (cachedGeneration != currentGeneration) {
        cachedInner = parent.getDelegate().makeSingleValueDimensionSelector(spec);
        cachedGeneration = currentGeneration;
      }
      return cachedInner;
    }

    @Override
    public int[] getRowVector()
    {
      return currentInner().getRowVector();
    }

    @Override
    public int getValueCardinality()
    {
      return currentInner().getValueCardinality();
    }

    @Nullable
    @Override
    public String lookupName(int id)
    {
      return currentInner().lookupName(id);
    }

    @Override
    public boolean nameLookupPossibleInAdvance()
    {
      return currentInner().nameLookupPossibleInAdvance();
    }

    @Nullable
    @Override
    public IdLookup idLookup()
    {
      return currentInner().idLookup();
    }

    @Override
    public int getMaxVectorSize()
    {
      return currentInner().getMaxVectorSize();
    }

    @Override
    public int getCurrentVectorSize()
    {
      return currentInner().getCurrentVectorSize();
    }
  }

  private static final class DelegatingMultiValueDimensionVectorSelector implements MultiValueDimensionVectorSelector
  {
    private final ClusteringVectorColumnSelectorFactory parent;
    private final DimensionSpec spec;
    private long cachedGeneration = -1;
    private MultiValueDimensionVectorSelector cachedInner;

    private DelegatingMultiValueDimensionVectorSelector(
        ClusteringVectorColumnSelectorFactory parent,
        DimensionSpec spec
    )
    {
      this.parent = parent;
      this.spec = spec;
    }

    private MultiValueDimensionVectorSelector currentInner()
    {
      final long currentGeneration = parent.getGeneration();
      if (cachedGeneration != currentGeneration) {
        cachedInner = parent.getDelegate().makeMultiValueDimensionSelector(spec);
        cachedGeneration = currentGeneration;
      }
      return cachedInner;
    }

    @Override
    public IndexedInts[] getRowVector()
    {
      return currentInner().getRowVector();
    }

    @Override
    public int getValueCardinality()
    {
      return currentInner().getValueCardinality();
    }

    @Nullable
    @Override
    public String lookupName(int id)
    {
      return currentInner().lookupName(id);
    }

    @Override
    public boolean nameLookupPossibleInAdvance()
    {
      return currentInner().nameLookupPossibleInAdvance();
    }

    @Nullable
    @Override
    public IdLookup idLookup()
    {
      return currentInner().idLookup();
    }

    @Override
    public int getMaxVectorSize()
    {
      return currentInner().getMaxVectorSize();
    }

    @Override
    public int getCurrentVectorSize()
    {
      return currentInner().getCurrentVectorSize();
    }
  }

  private static final class DelegatingVectorValueSelector implements VectorValueSelector
  {
    private final ClusteringVectorColumnSelectorFactory parent;
    private final String column;
    private long cachedGeneration = -1;
    private VectorValueSelector cachedInner;

    private DelegatingVectorValueSelector(ClusteringVectorColumnSelectorFactory parent, String column)
    {
      this.parent = parent;
      this.column = column;
    }

    private VectorValueSelector currentInner()
    {
      final long currentGeneration = parent.getGeneration();
      if (cachedGeneration != currentGeneration) {
        cachedInner = parent.getDelegate().makeValueSelector(column);
        cachedGeneration = currentGeneration;
      }
      return cachedInner;
    }

    @Override
    public long[] getLongVector()
    {
      return currentInner().getLongVector();
    }

    @Override
    public float[] getFloatVector()
    {
      return currentInner().getFloatVector();
    }

    @Override
    public double[] getDoubleVector()
    {
      return currentInner().getDoubleVector();
    }

    @Nullable
    @Override
    public boolean[] getNullVector()
    {
      return currentInner().getNullVector();
    }

    @Override
    public int getMaxVectorSize()
    {
      return currentInner().getMaxVectorSize();
    }

    @Override
    public int getCurrentVectorSize()
    {
      return currentInner().getCurrentVectorSize();
    }
  }

  private static final class DelegatingVectorObjectSelector implements VectorObjectSelector
  {
    private final ClusteringVectorColumnSelectorFactory parent;
    private final String column;
    private long cachedGeneration = -1;
    private VectorObjectSelector cachedInner;

    private DelegatingVectorObjectSelector(ClusteringVectorColumnSelectorFactory parent, String column)
    {
      this.parent = parent;
      this.column = column;
    }

    private VectorObjectSelector currentInner()
    {
      final long currentGeneration = parent.getGeneration();
      if (cachedGeneration != currentGeneration) {
        cachedInner = parent.getDelegate().makeObjectSelector(column);
        cachedGeneration = currentGeneration;
      }
      return cachedInner;
    }

    @Override
    public Object[] getObjectVector()
    {
      return currentInner().getObjectVector();
    }

    @Override
    public int getMaxVectorSize()
    {
      return currentInner().getMaxVectorSize();
    }

    @Override
    public int getCurrentVectorSize()
    {
      return currentInner().getCurrentVectorSize();
    }
  }
}
