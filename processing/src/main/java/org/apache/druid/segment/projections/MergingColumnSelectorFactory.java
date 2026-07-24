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
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.RowIdSupplier;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;

/**
 * {@link ColumnSelectorFactory} for a time-ordered k-way merge across per-cluster-group cursors (see
 * {@code MergingClusterGroupCursor}). Unlike {@link ClusteringColumnSelectorFactory}, which swaps a single delegate
 * on each <em>group</em> transition and rebuilds selectors via a generation counter, this factory pre-builds one
 * inner selector <em>per group</em> for each requested column and, on every access, dispatches to whichever group is
 * currently winning the merge (which changes per <em>row</em>).
 *
 * <p>Because the merge interleaves groups row-by-row, per-group-local dictionary ids are never stable across the
 * merged stream, so {@link #getColumnCapabilities} strips dictionary encoding for every column (forcing value-based
 * grouping, correct across groups) exactly as {@link ClusteringColumnSelectorFactory} does for non-clustering columns.
 * The row id is minted from the merge's output-row counter rather than any delegate's, since the merge emits exactly
 * one output row per advance.
 */
public class MergingColumnSelectorFactory implements ColumnSelectorFactory
{
  // Per-group factories, indexed by group. Entries may be null for groups whose cursor was null/absent; such groups
  // never win the merge, so their slots are never dispatched to.
  private final ColumnSelectorFactory[] groupFactories;
  // Index of the group currently winning the merge (the row being exposed). Valid while the cursor is not done.
  private final IntSupplier currentGroup;
  // First non-null group factory; a valid stand-in for every group's capabilities (see getColumnCapabilities).
  @Nullable
  private final ColumnSelectorFactory representative;
  // Row id minted from the merge's output-row counter (one per emitted row), forwarded to callers for caching.
  private final RowIdSupplier rowIdSupplier;

  public MergingColumnSelectorFactory(
      ColumnSelectorFactory[] groupFactories,
      IntSupplier currentGroup,
      LongSupplier currentRowId
  )
  {
    this.groupFactories = groupFactories;
    this.currentGroup = currentGroup;
    this.representative = firstNonNull(groupFactories);
    this.rowIdSupplier = currentRowId::getAsLong;
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
    final DimensionSelector[] perGroup = new DimensionSelector[groupFactories.length];
    for (int i = 0; i < groupFactories.length; i++) {
      if (groupFactories[i] != null) {
        perGroup[i] = groupFactories[i].makeDimensionSelector(dimensionSpec);
      }
    }
    return new MergingDimensionSelector(perGroup, dimensionSpec);
  }

  @Override
  public ColumnValueSelector makeColumnValueSelector(String columnName)
  {
    final ColumnValueSelector[] perGroup = new ColumnValueSelector[groupFactories.length];
    for (int i = 0; i < groupFactories.length; i++) {
      if (groupFactories[i] != null) {
        perGroup[i] = groupFactories[i].makeColumnValueSelector(columnName);
      }
    }
    return new MergingColumnValueSelector(perGroup, columnName);
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
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
