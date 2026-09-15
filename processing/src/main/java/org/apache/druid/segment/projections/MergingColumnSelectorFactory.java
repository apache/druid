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
import java.util.List;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;

/**
 * {@link ColumnSelectorFactory} for a time-ordered k-way merge across per-cluster-group cursors (see
 * {@code MergingClusterGroupCursor}). Unlike {@link ClusteringColumnSelectorFactory}, which swaps a single delegate
 * on each <em>group</em> transition and rebuilds selectors via a generation counter, this factory pre-builds one
 * inner selector <em>per group</em> for each requested column and, on every access, dispatches to whichever group is
 * currently winning the merge (which changes per <em>row</em>).
 *
 * <p>Clustering columns are handled directly rather than dispatched as they are constant within a group, so this
 * factory returns the winning group's clustering value as a per-group constant. Non-clustering columns dispatch to the
 * winning group's selector. A clustering column whose name is also a query virtual column's output is NOT served as the
 * constant: it dispatches to the winning group (whose factory resolves the virtual column), so a shadowing VC observes
 * the computed value rather than the constant. Names remapped away (a query VC equivalent to a materialized column)
 * never reach this factory: the enclosing {@code RemapColumnSelectorFactory} rewrites them to their materialized
 * target first.
 *
 * <p>Because the merge interleaves groups row-by-row, per-group-local dictionary ids are never stable across the
 * merged stream, so {@link #getColumnCapabilities} advertises non-dictionary-encoded for every column (forcing
 * value-based grouping, correct across groups) exactly as {@link ClusteringColumnSelectorFactory} does. The row id is
 * set from the merge's output-row counter rather than any delegate's, since the merge emits exactly one output row
 * per advance.
 */
public class MergingColumnSelectorFactory implements ColumnSelectorFactory
{
  // Per-group factories, indexed by group. Entries may be null for groups whose cursor was null/absent; such groups
  // never win the merge, so their slots are never dispatched to.
  private final ColumnSelectorFactory[] groupFactories;
  private final RowSignature clusteringColumns;
  private final List<Object[]> clusteringValuesByGroup;
  private final VirtualColumns queryVirtualColumns;
  // Index of the group currently winning the merge (the row being exposed). Valid while the cursor is not done.
  private final IntSupplier currentGroup;
  // First non-null group factory; a valid stand-in for every group's non-clustering capabilities (schema-homogeneous).
  @Nullable
  private final ColumnSelectorFactory representative;
  // Row id minted from the merge's output-row counter (one per emitted row), forwarded to callers for caching.
  private final RowIdSupplier rowIdSupplier;

  public MergingColumnSelectorFactory(
      ColumnSelectorFactory[] groupFactories,
      RowSignature clusteringColumns,
      List<Object[]> clusteringValuesByGroup,
      VirtualColumns queryVirtualColumns,
      IntSupplier currentGroup,
      LongSupplier currentRowId
  )
  {
    this.groupFactories = groupFactories;
    this.clusteringColumns = clusteringColumns;
    this.clusteringValuesByGroup = clusteringValuesByGroup;
    this.queryVirtualColumns = queryVirtualColumns;
    this.currentGroup = currentGroup;
    this.representative = firstNonNull(groupFactories);
    this.rowIdSupplier = currentRowId::getAsLong;
  }

  /**
   * Index of {@code name} in the clustering columns when it should be served as this group's clustering constant, or
   * {@code -1} otherwise. A clustering column shadowed by a query virtual column of the same output name is NOT served
   * as the constant (returns {@code -1}); it dispatches to the winning group so the computed value wins. Mirrors
   * {@link ClusteringColumnSelectorFactory}'s {@code servesClusteringConstant}.
   */
  private int clusteringConstantIndex(String name)
  {
    final int idx = clusteringColumns.indexOf(name);
    return idx >= 0 && !queryVirtualColumns.exists(name) ? idx : -1;
  }

  @Nullable
  private static ColumnSelectorFactory firstNonNull(ColumnSelectorFactory[] factories)
  {
    for (ColumnSelectorFactory factory : factories) {
      if (factory != null) {
        return factory;
      }
    }
    return null;
  }

  /**
   * Resolve the current winning group's per-group entry (selector/matcher). {@code currentGroup} only ever points at
   * a non-empty (hence non-null) group while the merge cursor is not done, and the {@link org.apache.druid.segment.Cursor}
   * contract requires callers to check {@code isDone()} before reading selectors, so a null here means that contract
   * was violated (a read past exhaustion). Fail fast rather than NPE opaquely.
   */
  private <T> T requireCurrent(T[] perGroup)
  {
    final int group = currentGroup.getAsInt();
    final T current = perGroup[group];
    if (current == null) {
      throw DruidException.defensive(
          "No entry for current cluster group [%s]; merge selectors must not be read after isDone()",
          group
      );
    }
    return current;
  }

  @Override
  public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
  {
    final int clusteringIdx = clusteringConstantIndex(dimensionSpec.getDimension());
    final DimensionSelector[] perGroup = new DimensionSelector[groupFactories.length];
    for (int i = 0; i < groupFactories.length; i++) {
      if (clusteringIdx >= 0) {
        // Clustering column: the winning group's constant value, decorated with any extraction fn.
        final Object value = clusteringValuesByGroup.get(i)[clusteringIdx];
        perGroup[i] = DimensionSelector.constant(
            value == null ? null : String.valueOf(value),
            dimensionSpec.getExtractionFn()
        );
      } else if (groupFactories[i] != null) {
        perGroup[i] = groupFactories[i].makeDimensionSelector(dimensionSpec);
      }
    }
    return new MergingDimensionSelector(perGroup, dimensionSpec);
  }

  @Override
  public ColumnValueSelector makeColumnValueSelector(String columnName)
  {
    final int clusteringIdx = clusteringConstantIndex(columnName);
    final ColumnValueSelector[] perGroup = new ColumnValueSelector[groupFactories.length];
    if (clusteringIdx >= 0) {
      // Clustering column: the winning group's constant value, as a typed value selector.
      final ExpressionType type =
          ExpressionType.fromColumnTypeStrict(clusteringColumns.getColumnType(clusteringIdx).orElseThrow());
      for (int i = 0; i < groupFactories.length; i++) {
        perGroup[i] = constantClusteringValueSelector(type, clusteringValuesByGroup.get(i)[clusteringIdx]);
      }
    } else {
      for (int i = 0; i < groupFactories.length; i++) {
        if (groupFactories[i] != null) {
          perGroup[i] = groupFactories[i].makeColumnValueSelector(columnName);
        }
      }
    }
    return new MergingColumnValueSelector(perGroup, columnName);
  }

  /**
   * A constant {@link ColumnValueSelector} for a clustering column's per-group value, unwrapping the {@link ExprEval}
   * so {@code getObject()} yields the raw typed value (matching an ordinary column selector).
   */
  private static ColumnValueSelector<?> constantClusteringValueSelector(ExpressionType type, @Nullable Object value)
  {
    final ConstantExprEvalSelector eval = new ConstantExprEvalSelector(ExprEval.ofType(type, value));
    return new ColumnValueSelector<>()
    {
      @Override
      public double getDouble()
      {
        return eval.getDouble();
      }

      @Override
      public float getFloat()
      {
        return eval.getFloat();
      }

      @Override
      public long getLong()
      {
        return eval.getLong();
      }

      @Override
      public boolean isNull()
      {
        return eval.isNull();
      }

      @Nullable
      @Override
      public Object getObject()
      {
        return eval.getObject().value();
      }

      @Override
      public Class<?> classOfObject()
      {
        return Object.class;
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        eval.inspectRuntimeShape(inspector);
      }
    };
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    final int clusteringIdx = clusteringConstantIndex(column);
    if (clusteringIdx >= 0) {
      // Clustering columns are exposed as per-group constants; report simple type-based capabilities (never
      // dictionary-encoded across the merge), exactly as ClusteringColumnSelectorFactory does.
      final ColumnType type = clusteringColumns.getColumnType(clusteringIdx).orElseThrow();
      if (type.is(ValueType.STRING)) {
        return ColumnCapabilitiesImpl.createSimpleSingleValueStringColumnCapabilities();
      }
      return ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(type);
    }
    if (representative == null) {
      return null;
    }
    // Precondition: every cluster group shares one schema (the sub-indexes are the same table split by clustering
    // key), so the first non-null group is a valid stand-in for all groups' capabilities of any given column.
    final ColumnCapabilities capabilities = representative.getColumnCapabilities(column);
    if (capabilities == null) {
      return null;
    }
    // Per-group-local dictionary ids are not stable across the merged stream (the same id means different values in
    // different groups), so advertise non-dictionary-encoded to force value-based grouping, which is correct across
    // groups.
    return ColumnCapabilitiesImpl.copyOf(capabilities)
                                 .setDictionaryEncoded(false)
                                 .setDictionaryValuesSorted(false)
                                 .setDictionaryValuesUnique(false)
                                 .setHasBitmapIndexes(false);
  }

  @Nullable
  @Override
  public RowIdSupplier getRowIdSupplier()
  {
    return rowIdSupplier;
  }

  private final class MergingDimensionSelector implements DimensionSelector
  {
    private final DimensionSelector[] perGroup;
    private final DimensionSpec spec;

    private MergingDimensionSelector(DimensionSelector[] perGroup, DimensionSpec spec)
    {
      this.perGroup = perGroup;
      this.spec = spec;
    }

    private DimensionSelector current()
    {
      return requireCurrent(perGroup);
    }

    @Override
    public IndexedInts getRow()
    {
      return current().getRow();
    }

    @Override
    public ValueMatcher makeValueMatcher(@Nullable String value)
    {
      final ValueMatcher[] matchers = new ValueMatcher[perGroup.length];
      for (int i = 0; i < perGroup.length; i++) {
        if (perGroup[i] != null) {
          matchers[i] = perGroup[i].makeValueMatcher(value);
        }
      }
      return new MergingValueMatcher(matchers);
    }

    @Override
    public ValueMatcher makeValueMatcher(DruidPredicateFactory predicateFactory)
    {
      final ValueMatcher[] matchers = new ValueMatcher[perGroup.length];
      for (int i = 0; i < perGroup.length; i++) {
        if (perGroup[i] != null) {
          matchers[i] = perGroup[i].makeValueMatcher(predicateFactory);
        }
      }
      return new MergingValueMatcher(matchers);
    }

    @Override
    public int getValueCardinality()
    {
      // Per-group dictionaries are not stable across the merged stream; CARDINALITY_UNKNOWN forces value-based
      // grouping (see class javadoc and ClusteringColumnSelectorFactory).
      return DimensionDictionarySelector.CARDINALITY_UNKNOWN;
    }

    @Nullable
    @Override
    public String lookupName(int id)
    {
      return current().lookupName(id);
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
      return current().getObject();
    }

    @Override
    public Class<?> classOfObject()
    {
      return current().classOfObject();
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("merging", spec.getDimension());
    }

    /**
     * Dispatches to the winning group's matcher per row. Pre-built per group so a matcher held across the merge
     * observes each row's winning group without rebuilding.
     */
    private final class MergingValueMatcher implements ValueMatcher
    {
      private final ValueMatcher[] matchers;

      private MergingValueMatcher(ValueMatcher[] matchers)
      {
        this.matchers = matchers;
      }

      @Override
      public boolean matches(boolean includeUnknown)
      {
        return requireCurrent(matchers).matches(includeUnknown);
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("merging-matcher", spec.getDimension());
      }
    }
  }

  private final class MergingColumnValueSelector implements ColumnValueSelector<Object>
  {
    private final ColumnValueSelector[] perGroup;
    private final String columnName;

    private MergingColumnValueSelector(ColumnValueSelector[] perGroup, String columnName)
    {
      this.perGroup = perGroup;
      this.columnName = columnName;
    }

    private ColumnValueSelector current()
    {
      return requireCurrent(perGroup);
    }

    @Override
    public double getDouble()
    {
      return current().getDouble();
    }

    @Override
    public float getFloat()
    {
      return current().getFloat();
    }

    @Override
    public long getLong()
    {
      return current().getLong();
    }

    @Override
    public boolean isNull()
    {
      return current().isNull();
    }

    @Nullable
    @Override
    public Object getObject()
    {
      return current().getObject();
    }

    @Override
    public Class<?> classOfObject()
    {
      return current().classOfObject();
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("merging", columnName);
    }
  }
}
