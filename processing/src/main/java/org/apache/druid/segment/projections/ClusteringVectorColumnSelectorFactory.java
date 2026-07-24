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
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
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
 * As with the scalar wrapper, a clustering column whose name is also a query virtual column's output is NOT
 * intercepted but left to the delegate (which resolves virtual columns before physical columns).
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
  // Query virtual columns whose output names shadow a clustering column are deferred to the delegate; see class doc.
  private final VirtualColumns queryVirtualColumns;
  private final int maxVectorSize;
  private VectorColumnSelectorFactory delegate;
  private Object[] clusteringValues;
  // Bumped on every setDelegate(...) so per-call selector wrappers can detect group transitions and rebuild their
  // cached inner state.
  private long generation;
  private final ReadableVectorInspector readableVectorInspector = new DelegatingReadableVectorInspector(this);

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
    this(delegate, clusteringColumns, clusteringValues, maxVectorSize, VirtualColumns.EMPTY);
  }

  public ClusteringVectorColumnSelectorFactory(
      VectorColumnSelectorFactory delegate,
      RowSignature clusteringColumns,
      Object[] clusteringValues,
      int maxVectorSize,
      VirtualColumns queryVirtualColumns
  )
  {
    this.clusteringColumns = clusteringColumns;
    this.queryVirtualColumns = queryVirtualColumns;
    this.maxVectorSize = maxVectorSize;
    setDelegate(delegate, clusteringValues);
  }

  /**
   * Whether {@code column} should be served as this group's clustering constant: a clustering column not shadowed by a
   * query virtual column of the same output name (see class doc).
   */
  private boolean servesClusteringConstant(String column)
  {
    return clusteringColumns.indexOf(column) >= 0 && !queryVirtualColumns.exists(column);
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
    return readableVectorInspector;
  }

  @Override
  public int getMaxVectorSize()
  {
    return maxVectorSize;
  }

  @Override
  public SingleValueDimensionVectorSelector makeSingleValueDimensionSelector(DimensionSpec dimensionSpec)
  {
    // clusteredValueGroups columns are never dictionary-encoded (see getColumnCapabilities), so the vector engine
    // always uses object/value selectors and never requests a dimension vector selector here. A request would mean a
    // column was wrongly advertised as dictionary-encoded.
    throw DruidException.defensive(
        "clusteredValueGroups columns are not dictionary-encoded; no single-value dimension vector selector for [%s]",
        dimensionSpec.getDimension()
    );
  }

  @Override
  public MultiValueDimensionVectorSelector makeMultiValueDimensionSelector(DimensionSpec dimensionSpec)
  {
    // See makeSingleValueDimensionSelector: clusteredValueGroups columns are never dictionary-encoded, so a
    // multi-value dimension vector selector is never requested either.
    throw DruidException.defensive(
        "clusteredValueGroups columns are not dictionary-encoded; no multi-value dimension vector selector for [%s]",
        dimensionSpec.getDimension()
    );
  }

  @Override
  public VectorValueSelector makeValueSelector(String column)
  {
    if (!servesClusteringConstant(column)) {
      return new DelegatingVectorValueSelector(this, column);
    }
    return new ClusteringVectorValueSelector(this, clusteringColumns.indexOf(column));
  }

  @Override
  public VectorObjectSelector makeObjectSelector(String column)
  {
    if (!servesClusteringConstant(column)) {
      return new DelegatingVectorObjectSelector(this, column);
    }
    return new ClusteringVectorObjectSelector(this, clusteringColumns.indexOf(column));
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    if (!servesClusteringConstant(column)) {
      // Non-clustering columns (or a clustering column shadowed by a query VC, resolved by the delegate) are stored
      // per cluster group with per-group local dictionaries that are NOT stable
      // across the concatenating vector cursor. We must not advertise dictionary encoding: the vectorized group-by
      // keys on the selector's (per-group-local) IDs when the column reports as dictionary-encoded
      // (GroupByVectorColumnProcessorFactory#useDictionaryEncodedSelector), conflating distinct values across groups.
      // Reporting non-dictionary-encoded routes it to the value-building vector selector, which is correct across
      // groups.
      final ColumnCapabilities delegateCapabilities = delegate.getColumnCapabilities(column);
      if (delegateCapabilities == null) {
        return null;
      }
      return ColumnCapabilitiesImpl.copyOf(delegateCapabilities)
                                   .setDictionaryEncoded(false)
                                   .setDictionaryValuesSorted(false)
                                   .setDictionaryValuesUnique(false)
                                   .setHasBitmapIndexes(false);
    }
    final int idx = clusteringColumns.indexOf(column);
    final ColumnType type = clusteringColumns.getColumnType(idx).orElseThrow();
    if (type.is(ValueType.STRING)) {
      return ColumnCapabilitiesImpl.createSimpleSingleValueStringColumnCapabilities();
    }
    return ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(type);
  }

  /**
   * A {@link ReadableVectorInspector} bound to the factory, not to one delegate: {@link #getCurrentVectorSize} and
   * {@link #getId} follow whichever delegate is current, so an inspector grabbed once keeps tracking the active group
   * across {@code ConcatenatingVectorCursor} transitions. {@link #getMaxVectorSize} is the factory's fixed max.
   */
  private static final class DelegatingReadableVectorInspector implements ReadableVectorInspector
  {
    private final ClusteringVectorColumnSelectorFactory parent;
    private int vectorId = NULL_ID;
    private int lastDelegateId = NULL_ID;
    private VectorColumnSelectorFactory lastDelegate;

    private DelegatingReadableVectorInspector(ClusteringVectorColumnSelectorFactory parent)
    {
      this.parent = parent;
    }

    @Override
    public int getMaxVectorSize()
    {
      return parent.getMaxVectorSize();
    }

    @Override
    public int getCurrentVectorSize()
    {
      return parent.getDelegate().getReadableVectorInspector().getCurrentVectorSize();
    }

    @Override
    public int getId()
    {
      final VectorColumnSelectorFactory delegate = parent.getDelegate();
      final int id = delegate.getReadableVectorInspector().getId();
      if (id == NULL_ID) {
        return NULL_ID;
      }
      if (delegate != lastDelegate || id != lastDelegateId) {
        lastDelegate = delegate;
        lastDelegateId = id;
        vectorId++;
      }
      return vectorId;
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
