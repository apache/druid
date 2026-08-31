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
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.ConstantExprEvalSelector;
import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.RowIdSupplier;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;
import java.util.function.Supplier;

/**
 * {@link ColumnSelectorFactory} wrapper that intercepts requests for clustering columns and returns selectors
 * carrying the group's constant value, while delegating all other column lookups to a wrapped factory. This is the
 * mechanism by which a cluster group's clustering columns, which are NOT stored in the per-group column data since
 * they're constant across the group, are made visible to query engines as if they were ordinary columns.
 * <p>
 * A clustering column whose name is also a query virtual column's output is NOT intercepted; it is left to the delegate
 * (which resolves virtual columns before physical columns), so a shadowing VC — and anything reading it — observes the
 * computed value rather than the constant. Names that were remapped away (a query VC equivalent to a materialized
 * column) never reach this wrapper: the enclosing {@code RemapColumnSelectorFactory} rewrites them to their materialized
 * target first.
 */
public class ClusteringColumnSelectorFactory implements ColumnSelectorFactory
{
  /**
   * Throwing placeholder delegate for a {@link ClusteringColumnSelectorFactory} that will have its real delegate
   * set by a concatenating cursor's lazy init before any selector is exposed to the caller. Shared by the
   * historical and realtime clustered cursor factories, which both construct the wrapper with this placeholder
   * before handing it to {@link org.apache.druid.segment.ConcatenatingCursor}.
   */
  public static final ColumnSelectorFactory UNINITIALIZED_DELEGATE = new ColumnSelectorFactory()
  {
    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      throw DruidException.defensive("ConcatenatingCursor delegate accessed before initialization");
    }

    @Override
    public ColumnValueSelector makeColumnValueSelector(String columnName)
    {
      throw DruidException.defensive("ConcatenatingCursor delegate accessed before initialization");
    }

    @Nullable
    @Override
    public ColumnCapabilities getColumnCapabilities(String column)
    {
      return null;
    }
  };

  private final RowSignature clusteringColumns;
  // Query virtual columns whose output names shadow a clustering column are deferred to the delegate; see class doc.
  private final VirtualColumns queryVirtualColumns;
  private ColumnSelectorFactory delegate;
  private Object[] clusteringValues;
  // Bumped on every setDelegate(...) so per-call selector wrappers can detect group transitions and rebuild their
  // cached inner state
  private long generation;
  private final RowIdSupplier rowIdSupplier = new DelegatingRowIdSupplier(this);

  public ClusteringColumnSelectorFactory(
      ColumnSelectorFactory delegate,
      RowSignature clusteringColumns,
      Object[] clusteringValues
  )
  {
    this(delegate, clusteringColumns, clusteringValues, VirtualColumns.EMPTY);
  }

  public ClusteringColumnSelectorFactory(
      ColumnSelectorFactory delegate,
      RowSignature clusteringColumns,
      Object[] clusteringValues,
      VirtualColumns queryVirtualColumns
  )
  {
    this.clusteringColumns = clusteringColumns;
    this.queryVirtualColumns = queryVirtualColumns;
    setDelegate(delegate, clusteringValues);
  }

  /**
   * Whether {@code name} should be served as this group's clustering constant: a clustering column not shadowed by a
   * query virtual column of the same output name (see class doc).
   */
  private boolean servesClusteringConstant(String name)
  {
    return clusteringColumns.indexOf(name) >= 0 && !queryVirtualColumns.exists(name);
  }

  /**
   * Update the underlying factory and the constant values for the current cluster group. Called by a multi-group
   * concatenating cursor on each group transition. Selectors previously returned by this factory will, on their next
   * invocation, observe the updated state; see the per-call indirection in the inner selector classes.
   */
  public void setDelegate(ColumnSelectorFactory delegate, Object[] clusteringValues)
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

  ColumnSelectorFactory getDelegate()
  {
    return delegate;
  }

  long getGeneration()
  {
    return generation;
  }

  @Override
  public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
  {
    final String name = dimensionSpec.getDimension();
    if (!servesClusteringConstant(name)) {
      return new DelegatingDimensionSelector(this, dimensionSpec);
    }
    return new ClusteringDimensionSelector(this, clusteringColumns.indexOf(name), dimensionSpec);
  }

  @Override
  public ColumnValueSelector makeColumnValueSelector(String columnName)
  {
    if (!servesClusteringConstant(columnName)) {
      return new DelegatingColumnValueSelector(this, columnName);
    }
    final int idx = clusteringColumns.indexOf(columnName);
    return new ClusteringColumnValueSelector(this, idx, clusteringColumns.getColumnType(idx).orElseThrow());
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    if (!servesClusteringConstant(column)) {
      // Non-clustering columns (or a clustering column shadowed by a query VC, resolved by the delegate) are stored
      // per cluster group, each with its own local dictionary. The
      // ConcatenatingCursor walks those groups behind a single cursor, so a column's dictionary IDs are NOT stable
      // across the whole cursor. We therefore must not advertise dictionary encoding here: otherwise the group-by
      // engine keys on the (per-group-local) IDs and conflates distinct values from different groups. Reporting the
      // column as non-dictionary-encoded forces value-based grouping, which is correct across groups.
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

  @Nullable
  @Override
  public RowIdSupplier getRowIdSupplier()
  {
    // A delegate may not support row-id caching; mirror that so callers skip caching (null)
    return delegate.getRowIdSupplier() == null ? null : rowIdSupplier;
  }

  Object currentValue(int idx)
  {
    return clusteringValues[idx];
  }

  /**
   * A {@link RowIdSupplier} bound to the factory, {@link #getRowId} follows whichever delegate is current, so a
   * supplier grabbed once keeps tracking the active group across {@code ConcatenatingCursor} transitions.
   */
  private static final class DelegatingRowIdSupplier implements RowIdSupplier
  {
    private final ClusteringColumnSelectorFactory parent;
    private long rowId = INIT;
    private long lastDelegateRowId = INIT;
    private RowIdSupplier lastDelegate;

    private DelegatingRowIdSupplier(ClusteringColumnSelectorFactory parent)
    {
      this.parent = parent;
    }

    @Override
    public long getRowId()
    {
      final RowIdSupplier delegate = parent.getDelegate().getRowIdSupplier();
      if (delegate == null) {
        // getRowIdSupplier() only hands out this wrapper when the delegate supports row ids; clustered groups are
        // homogeneous, so a null here would mean a group mid-cursor stopped supporting them.
        throw DruidException.defensive("delegate row id supplier became null across a cluster-group transition");
      }
      final long id = delegate.getRowId();
      if (id == INIT) {
        return INIT;
      }
      if (delegate != lastDelegate || id != lastDelegateRowId) {
        lastDelegate = delegate;
        lastDelegateRowId = id;
        rowId++;
      }
      return rowId;
    }
  }

  /**
   * Dimension selector for a clustering column. Delegates the value lookup back to the parent factory each call so
   * that group transitions (which mutate the parent's clustering values) are observed immediately. Internally
   * decorates a {@link DimensionSelector#constant(String)} re-built when the underlying value changes.
   */
  private static final class ClusteringDimensionSelector implements DimensionSelector
  {
    private final ClusteringColumnSelectorFactory parent;
    private final int idx;
    private final DimensionSpec spec;
    private DimensionSelector cachedSelector;
    private long cachedGeneration = -1;

    private ClusteringDimensionSelector(ClusteringColumnSelectorFactory parent, int idx, DimensionSpec spec)
    {
      this.parent = parent;
      this.idx = idx;
      this.spec = spec;
    }

    private DimensionSelector currentSelector()
    {
      final long currentGeneration = parent.getGeneration();
      if (cachedGeneration == currentGeneration) {
        return cachedSelector;
      }
      final Object raw = parent.currentValue(idx);
      final String stringValue = raw == null ? null : String.valueOf(raw);
      cachedSelector = DimensionSelector.constant(stringValue, spec.getExtractionFn());
      cachedGeneration = currentGeneration;
      return cachedSelector;
    }

    @Override
    public IndexedInts getRow()
    {
      return currentSelector().getRow();
    }

    @Override
    public ValueMatcher makeValueMatcher(@Nullable String value)
    {
      // Generation-aware: rebuild the matcher when the clustering value itself changes (group transition).
      return new ClusteringValueMatcher(() -> currentSelector().makeValueMatcher(value));
    }

    @Override
    public ValueMatcher makeValueMatcher(
        DruidPredicateFactory predicateFactory
    )
    {
      return new ClusteringValueMatcher(() -> currentSelector().makeValueMatcher(predicateFactory));
    }

    /**
     * Generation-aware matcher for the clustering-column path. The constant value itself changes between groups,
     * so a held matcher must re-resolve from the current per-generation constant selector.
     */
    private final class ClusteringValueMatcher implements ValueMatcher
    {
      private final Supplier<ValueMatcher> factory;
      private long cachedGeneration = -1;
      private ValueMatcher cachedMatcher;

      private ClusteringValueMatcher(Supplier<ValueMatcher> factory)
      {
        this.factory = factory;
      }

      private ValueMatcher current()
      {
        final long gen = parent.getGeneration();
        if (cachedGeneration != gen) {
          cachedMatcher = factory.get();
          cachedGeneration = gen;
        }
        return cachedMatcher;
      }

      @Override
      public boolean matches(boolean includeUnknown)
      {
        return current().matches(includeUnknown);
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("clustering-matcher", idx);
      }
    }

    @Override
    public int getValueCardinality()
    {
      // The per-group constant selector reports cardinality 1 and always returns id 0, but that id is NOT stable
      // across the concatenating cursor: id 0 resolves to a different clustering value in each group. Forwarding it
      // would let the group-by engine take the dictionary-id-keyed (array) path and silently conflate every group
      // into the single id-0 bucket.
      return DimensionDictionarySelector.CARDINALITY_UNKNOWN;
    }

    @Nullable
    @Override
    public String lookupName(int id)
    {
      return currentSelector().lookupName(id);
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
      return null;
    }

    @Nullable
    @Override
    public Object getObject()
    {
      return currentSelector().getObject();
    }

    @Override
    public Class<?> classOfObject()
    {
      return currentSelector().classOfObject();
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("clusteringIndex", idx);
    }
  }

  /**
   * Value selector for a clustering column. Caches a {@link ConstantExprEvalSelector} built from the group's typed
   * value; on group transition, the cache rebuilds against the new value. Mirrors
   * {@code EvalUnwrappingColumnValueSelector} in {@code ExpressionSelectors}.
   */
  private static final class ClusteringColumnValueSelector implements ColumnValueSelector<Object>
  {
    private final ClusteringColumnSelectorFactory parent;
    private final int idx;
    private final ExpressionType expressionType;
    private long cachedGeneration = -1;
    private ConstantExprEvalSelector cachedSelector;

    private ClusteringColumnValueSelector(ClusteringColumnSelectorFactory parent, int idx, ColumnType columnType)
    {
      this.parent = parent;
      this.idx = idx;
      this.expressionType = ExpressionType.fromColumnTypeStrict(columnType);
    }

    private ConstantExprEvalSelector currentSelector()
    {
      final long currentGeneration = parent.getGeneration();
      if (cachedGeneration == currentGeneration) {
        return cachedSelector;
      }
      cachedSelector = new ConstantExprEvalSelector(ExprEval.ofType(expressionType, parent.currentValue(idx)));
      cachedGeneration = currentGeneration;
      return cachedSelector;
    }

    @Override
    public double getDouble()
    {
      return currentSelector().getDouble();
    }

    @Override
    public float getFloat()
    {
      return currentSelector().getFloat();
    }

    @Override
    public long getLong()
    {
      return currentSelector().getLong();
    }

    @Override
    public boolean isNull()
    {
      return currentSelector().isNull();
    }

    @Nullable
    @Override
    public Object getObject()
    {
      return currentSelector().getObject().value();
    }

    @Override
    public Class<Object> classOfObject()
    {
      return Object.class;
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("clusteringIndex", idx);
    }
  }

  /**
   * Stable {@link DimensionSelector} for a non-clustering column. Each call delegates to the parent factory's
   * current underlying delegate; on group transition, the cached inner selector is rebuilt against the new delegate.
   * For the single-group case, the cache fills once and never invalidates.
   *
   * <p>Value matchers returned by {@link #makeValueMatcher(String)} /
   * {@link #makeValueMatcher(DruidPredicateFactory)} are also generation-aware; they re-resolve their inner matcher
   * from the current delegate on group transition, so callers that hold a matcher across transitions observe the
   * new group's data the same way the selector itself does.
   */
  private static final class DelegatingDimensionSelector implements DimensionSelector
  {
    private final ClusteringColumnSelectorFactory parent;
    private final DimensionSpec spec;
    private long cachedGeneration = -1;
    private DimensionSelector cachedInner;

    private DelegatingDimensionSelector(ClusteringColumnSelectorFactory parent, DimensionSpec spec)
    {
      this.parent = parent;
      this.spec = spec;
    }

    private DimensionSelector currentInner()
    {
      final long currentGeneration = parent.getGeneration();
      if (cachedGeneration != currentGeneration) {
        cachedInner = parent.getDelegate().makeDimensionSelector(spec);
        cachedGeneration = currentGeneration;
      }
      return cachedInner;
    }

    @Override
    public IndexedInts getRow()
    {
      return currentInner().getRow();
    }

    @Override
    public ValueMatcher makeValueMatcher(@Nullable String value)
    {
      return new DelegatingValueMatcher(() -> currentInner().makeValueMatcher(value));
    }

    @Override
    public ValueMatcher makeValueMatcher(DruidPredicateFactory predicateFactory)
    {
      return new DelegatingValueMatcher(() -> currentInner().makeValueMatcher(predicateFactory));
    }

    /**
     * Generation-aware {@link ValueMatcher}: re-resolves its inner matcher from the current
     * {@link DimensionSelector} on each group transition. Non-static inner class so it can read the outer
     * selector's {@code parent.getGeneration()} and trigger a rebuild via the supplier.
     */
    private final class DelegatingValueMatcher implements ValueMatcher
    {
      private final Supplier<ValueMatcher> factory;
      private long cachedGeneration = -1;
      private ValueMatcher cachedMatcher;

      private DelegatingValueMatcher(Supplier<ValueMatcher> factory)
      {
        this.factory = factory;
      }

      private ValueMatcher current()
      {
        final long gen = parent.getGeneration();
        if (cachedGeneration != gen) {
          cachedMatcher = factory.get();
          cachedGeneration = gen;
        }
        return cachedMatcher;
      }

      @Override
      public boolean matches(boolean includeUnknown)
      {
        return current().matches(includeUnknown);
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("delegating-matcher", spec.getDimension());
      }
    }

    @Override
    public int getValueCardinality()
    {
      // The dictionary is per cluster group and NOT stable across the concatenating cursor (the same local id means
      // different values in different groups). Reporting CARDINALITY_UNKNOWN forces query engines onto the
      // value-based (rather than dictionary-id-keyed) path, which is correct across groups; a dictionary-id-keyed
      // group-by would otherwise conflate distinct values that share an id. lookupName() still resolves per-row
      // against the current group, so value-based grouping reads the right value.
      return DimensionDictionarySelector.CARDINALITY_UNKNOWN;
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
      // Per-group dictionaries cannot be enumerated in advance across the concatenating cursor.
      return false;
    }

    @Nullable
    @Override
    public IdLookup idLookup()
    {
      // No stable id<->name mapping across groups; callers must resolve by value.
      return null;
    }

    @Nullable
    @Override
    public Object getObject()
    {
      return currentInner().getObject();
    }

    @Override
    public Class<?> classOfObject()
    {
      return currentInner().classOfObject();
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("delegating", spec.getDimension());
    }
  }

  /**
   * Stable {@link ColumnValueSelector} for a non-clustering column. Same delegating-with-generation-cache pattern as
   * {@link DelegatingDimensionSelector}.
   */
  private static final class DelegatingColumnValueSelector implements ColumnValueSelector<Object>
  {
    private final ClusteringColumnSelectorFactory parent;
    private final String columnName;
    private long cachedGeneration = -1;
    private ColumnValueSelector cachedInner;

    private DelegatingColumnValueSelector(ClusteringColumnSelectorFactory parent, String columnName)
    {
      this.parent = parent;
      this.columnName = columnName;
    }

    @SuppressWarnings("unchecked")
    private ColumnValueSelector<Object> currentInner()
    {
      final long currentGeneration = parent.getGeneration();
      if (cachedGeneration != currentGeneration) {
        cachedInner = parent.getDelegate().makeColumnValueSelector(columnName);
        cachedGeneration = currentGeneration;
      }
      return (ColumnValueSelector<Object>) cachedInner;
    }

    @Override
    public double getDouble()
    {
      return currentInner().getDouble();
    }

    @Override
    public float getFloat()
    {
      return currentInner().getFloat();
    }

    @Override
    public long getLong()
    {
      return currentInner().getLong();
    }

    @Override
    public boolean isNull()
    {
      return currentInner().isNull();
    }

    @Nullable
    @Override
    public Object getObject()
    {
      return currentInner().getObject();
    }

    @Override
    public Class<?> classOfObject()
    {
      return currentInner().classOfObject();
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("delegating", columnName);
    }
  }
}
