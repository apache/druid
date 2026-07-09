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

package org.apache.druid.segment;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.data.Offset;
import org.apache.druid.segment.projections.ClusterGroupQueryPlan;
import org.apache.druid.segment.projections.ClusteredValueGroupsBaseTableSchema;
import org.apache.druid.segment.projections.ClusteringColumnSelectorFactory;
import org.apache.druid.segment.projections.ClusteringVectorColumnSelectorFactory;
import org.apache.druid.segment.projections.Projections;
import org.apache.druid.segment.projections.QueryableProjection;
import org.apache.druid.segment.projections.SingleGroupClusteringColumnSelectorFactory;
import org.apache.druid.segment.projections.SingleGroupClusteringVectorColumnSelectorFactory;
import org.apache.druid.segment.projections.TableClusterGroupSpec;
import org.apache.druid.segment.vector.ConcatenatingVectorCursor;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.ReadableVectorInspector;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorCursor;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorOffset;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;

public class QueryableIndexCursorFactory implements ResidentCursorFactory
{
  private final QueryableIndex index;
  private final TimeBoundaryInspector timeBoundaryInspector;

  /**
   * Constructor that accepts a {@link QueryableIndexTimeBoundaryInspector} that is in use elsewhere, promoting
   * efficient re-use.
   */
  public QueryableIndexCursorFactory(QueryableIndex index, TimeBoundaryInspector timeBoundaryInspector)
  {
    this.index = index;
    this.timeBoundaryInspector = timeBoundaryInspector;
  }

  /**
   * Constructor that creates a new {@link QueryableIndexTimeBoundaryInspector}.
   */
  public QueryableIndexCursorFactory(QueryableIndex index)
  {
    this(index, QueryableIndexTimeBoundaryInspector.create(index));
  }

  @Override
  public CursorHolder makeCursorHolder(CursorBuildSpec spec)
  {
    QueryableProjection<QueryableIndex> projection = index.getProjection(spec);
    if (projection != null) {
      return makeAggregateProjectionCursorHolder(projection);
    }

    // Cluster-group dispatch runs after aggregate-projection match, before the regular base-table fallback
    final ClusteredValueGroupsBaseTableSchema clusterSummary = index.getClusteredBaseSummary();
    if (clusterSummary != null) {
      return makeClusteredCursorHolder(spec);
    }

    // No projections, no clustering, regular full-segment cursor.
    return new QueryableIndexCursorHolder(index, spec, timeBoundaryInspector);
  }

  public CursorHolder makeCursorHolderForProjection(
      CursorBuildSpec spec,
      @Nullable QueryableProjection<QueryableIndex> projection
  )
  {
    if (projection != null) {
      return makeAggregateProjectionCursorHolder(projection);
    }

    // Cluster-group dispatch runs after aggregate-projection match, before the regular base-table fallback
    final ClusteredValueGroupsBaseTableSchema clusterSummary = index.getClusteredBaseSummary();
    if (clusterSummary != null) {
      return makeClusteredCursorHolder(spec);
    }

    // No projections, no clustering, regular full-segment cursor.
    return new QueryableIndexCursorHolder(index, spec, timeBoundaryInspector);
  }

  private CursorHolder makeAggregateProjectionCursorHolder(QueryableProjection<QueryableIndex> projection)
  {
    return new QueryableIndexCursorHolder(
        projection.getRowSelector(),
        projection.getCursorBuildSpec(),
        QueryableIndexTimeBoundaryInspector.create(projection.getRowSelector())
    )
    {
      @Override
      protected ColumnSelectorFactory makeColumnSelectorFactoryForOffset(
          ColumnCache columnCache,
          Offset baseOffset
      )
      {
        return projection.wrapColumnSelectorFactory(
            super.makeColumnSelectorFactoryForOffset(columnCache, baseOffset)
        );
      }

      @Override
      protected VectorColumnSelectorFactory makeVectorColumnSelectorFactoryForOffset(
          ColumnCache columnCache,
          VectorOffset baseOffset
      )
      {
        return projection.wrapVectorColumnSelectorFactory(
            super.makeVectorColumnSelectorFactoryForOffset(columnCache, baseOffset)
        );
      }

      @Override
      public boolean isPreAggregated()
      {
        return true;
      }

      @Nullable
      @Override
      public List<AggregatorFactory> getAggregatorsForPreAggregated()
      {
        return projection.getCursorBuildSpec().getAggregators();
      }
    };
  }

  private CursorHolder makeClusteredCursorHolder(CursorBuildSpec spec)
  {
    return makeClusteredCursorHolder(
        spec,
        Projections.planClusterGroupQuery(new ArrayList<>(index.getClusterGroupSchemas()), spec)
    );
  }

  /**
   * Build a clustered-base-table cursor holder from an already-computed {@link ClusterGroupQueryPlan}. Exposed so the
   * partial (on-demand) cursor factory can plan the cluster groups once — to decide which group bundles to download —
   * and reuse the same plan to build the holder, rather than re-running {@link Projections#planClusterGroupQuery}.
   */
  public CursorHolder makeClusteredCursorHolder(CursorBuildSpec spec, ClusterGroupQueryPlan plan)
  {
    if (plan.survivingGroups().isEmpty()) {
      return EmptyCursorHolder.INSTANCE;
    }

    if (plan.survivingGroups().size() == 1) {
      return makeSingleGroupClusteredCursorHolder(spec, plan, plan.survivingGroups().getFirst());
    }
    return makeMultiGroupClusteredCursorHolder(spec, plan);
  }

  private CursorHolder makeSingleGroupClusteredCursorHolder(
      CursorBuildSpec spec,
      ClusterGroupQueryPlan plan,
      TableClusterGroupSpec valueGroup
  )
  {
    final QueryableIndex groupIndex = index.getClusterGroupQueryableIndex(valueGroup);
    if (groupIndex == null) {
      throw DruidException.defensive(
          "No cluster-group sub-index resolvable for clustering values "
          + Arrays.toString(valueGroup.lookupClusteringValues())
      );
    }

    return new QueryableIndexCursorHolder(
        groupIndex,
        plan.rebuildCursorBuildSpec(spec, valueGroup),
        QueryableIndexTimeBoundaryInspector.create(groupIndex),
        valueGroup.getSummary().getOrdering()
    )
    {
      @Override
      protected ColumnSelectorFactory makeColumnSelectorFactoryForOffset(
          ColumnCache columnCache,
          Offset baseOffset
      )
      {
        return new SingleGroupClusteringColumnSelectorFactory(
            super.makeColumnSelectorFactoryForOffset(columnCache, baseOffset),
            valueGroup.getSummary().getClusteringColumns(),
            valueGroup.lookupClusteringValues()
        );
      }

      @Override
      protected VectorColumnSelectorFactory makeVectorColumnSelectorFactoryForOffset(
          ColumnCache columnCache,
          VectorOffset baseOffset
      )
      {
        return new SingleGroupClusteringVectorColumnSelectorFactory(
            super.makeVectorColumnSelectorFactoryForOffset(columnCache, baseOffset),
            valueGroup.getSummary().getClusteringColumns(),
            valueGroup.lookupClusteringValues()
        );
      }
    };
  }

  /**
   * Build a cursor holder that walks multiple matching cluster groups back-to-back via
   * {@link ConcatenatingCursor}. Each per-group {@link CursorHolder} is built lazily inside the cursor's group
   * transition, so a query that finishes early (e.g., LIMIT-bounded) doesn't open every group's offset.
   */
  private CursorHolder makeMultiGroupClusteredCursorHolder(
      CursorBuildSpec spec,
      ClusterGroupQueryPlan plan
  )
  {
    final List<TableClusterGroupSpec> matching = plan.survivingGroups();
    // All matching specs share the same parent summary (they came out of one segment); grab a reference for
    // getOrdering() and clusteringColumns below.
    final ClusteredValueGroupsBaseTableSchema clusterSummary = matching.get(0).getSummary();
    final RowSignature clusteringColumns = clusterSummary.getClusteringColumns();
    final List<Object[]> clusteringValuesByGroup = new ArrayList<>(matching.size());
    final List<Supplier<CursorHolder>> holderSuppliers = new ArrayList<>(matching.size());
    // lifecycle management closer for per-group CursorHolders
    final Closer closer = Closer.create();
    for (TableClusterGroupSpec valueGroup : matching) {
      clusteringValuesByGroup.add(valueGroup.lookupClusteringValues());
      final QueryableIndex groupIndex = index.getClusterGroupQueryableIndex(valueGroup);
      if (groupIndex == null) {
        throw DruidException.defensive(
            "No cluster-group sub-index resolvable for clustering values "
            + Arrays.toString(valueGroup.lookupClusteringValues())
        );
      }
      final CursorBuildSpec groupSpec = plan.rebuildCursorBuildSpec(spec, valueGroup);
      holderSuppliers.add(
          Suppliers.memoize(
              () -> closer.register(
                  new QueryableIndexCursorHolder(
                      groupIndex,
                      groupSpec,
                      QueryableIndexTimeBoundaryInspector.create(groupIndex)
                  )
              )
          )
      );
    }

    // Initial wrapper state uses the first group's clustering values + a throwing placeholder delegate. The
    // ConcatenatingCursor immediately calls setDelegate on init (before any selector is exposed). The vector
    // wrapper carries the query-level max vector size from the build spec, the placeholder delegate can't be
    // queried for sizing, and the value is constant across groups anyway.
    final int vectorSize = spec.getQueryContext().getVectorSize();
    final ClusteringColumnSelectorFactory wrapperFactory = new ClusteringColumnSelectorFactory(
        ClusteringColumnSelectorFactory.UNINITIALIZED_DELEGATE,
        clusteringColumns,
        clusteringValuesByGroup.get(0)
    );
    final ClusteringVectorColumnSelectorFactory vectorWrapperFactory = new ClusteringVectorColumnSelectorFactory(
        UNINITIALIZED_VECTOR_DELEGATE,
        clusteringColumns,
        clusteringValuesByGroup.get(0),
        vectorSize
    );

    final ConcatenatingCursor cursor = new ConcatenatingCursor(
        holderSuppliers,
        clusteringValuesByGroup,
        wrapperFactory
    );
    final ConcatenatingVectorCursor vectorCursor = new ConcatenatingVectorCursor(
        holderSuppliers,
        clusteringValuesByGroup,
        vectorWrapperFactory
    );

    // each group gets a different rewritten filter, so the conservative thing to do here is require the original query
    // filter's value matcher to be vectorizable. This works because every per-group filter is a sub-structure of the
    // original filter (clustering leaves fold to constant TRUE/FALSE, other leaves pass through unchanged)
    final Filter queryFilter = spec.getFilter();
    final boolean filterCanVectorize =
        queryFilter == null || queryFilter.canVectorizeMatcher(spec.getVirtualColumns().wrapInspector(this));
    // we still check that the first holder is vectorizable to make sure all the non-filter parts can be vectorized
    final boolean canVectorize = filterCanVectorize && holderSuppliers.get(0).get().canVectorize();

    return new CursorHolder()
    {
      @Override
      public Cursor asCursor()
      {
        return cursor;
      }

      @Override
      public VectorCursor asVectorCursor()
      {
        return vectorCursor;
      }

      @Override
      public boolean canVectorize()
      {
        return canVectorize;
      }

      @Override
      public List<OrderBy> getOrdering()
      {
        // Cluster groups are written in clustering-value order (writer-enforced; see ClusteredValueGroupsBaseTableSchema),
        // and within each group rows are sorted by the segment ordering's tail (clustering prefix dropped). So
        // back-to-back walking yields rows in the full segment ordering; the writer-side contract makes the
        // concatenation order-preserving without any merge work at read time.
        return clusterSummary.getOrdering();
      }

      @Override
      public void close()
      {
        try {
          closer.close();
        }
        catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  /**
   * Vector counterpart of {@link ClusteringColumnSelectorFactory#UNINITIALIZED_DELEGATE}. Replaced by
   * {@link ConcatenatingVectorCursor}'s lazy init before the wrapper is exposed.
   */
  private static final VectorColumnSelectorFactory UNINITIALIZED_VECTOR_DELEGATE = new VectorColumnSelectorFactory()
  {
    @Override
    public ReadableVectorInspector getReadableVectorInspector()
    {
      throw DruidException.defensive("ConcatenatingVectorCursor delegate accessed before initialization");
    }

    @Override
    public SingleValueDimensionVectorSelector makeSingleValueDimensionSelector(DimensionSpec dimensionSpec)
    {
      throw DruidException.defensive("ConcatenatingVectorCursor delegate accessed before initialization");
    }

    @Override
    public MultiValueDimensionVectorSelector makeMultiValueDimensionSelector(DimensionSpec dimensionSpec)
    {
      throw DruidException.defensive("ConcatenatingVectorCursor delegate accessed before initialization");
    }

    @Override
    public VectorValueSelector makeValueSelector(String column)
    {
      throw DruidException.defensive("ConcatenatingVectorCursor delegate accessed before initialization");
    }

    @Override
    public VectorObjectSelector makeObjectSelector(String column)
    {
      throw DruidException.defensive("ConcatenatingVectorCursor delegate accessed before initialization");
    }

    @Nullable
    @Override
    public ColumnCapabilities getColumnCapabilities(String column)
    {
      return null;
    }
  };

  @Override
  public RowSignature getRowSignature()
  {
    final ClusteredValueGroupsBaseTableSchema clusterSummary = index.getClusteredBaseSummary();
    if (clusterSummary != null) {
      return getClusteredRowSignature(clusterSummary);
    }

    final LinkedHashSet<String> columns = new LinkedHashSet<>();

    for (final OrderBy orderBy : index.getOrdering()) {
      columns.add(orderBy.getColumnName());
    }

    // Add __time after the defined ordering, if __time wasn't part of it.
    columns.add(ColumnHolder.TIME_COLUMN_NAME);
    columns.addAll(index.getColumnNames());

    final RowSignature.Builder builder = RowSignature.builder();
    for (final String column : columns) {
      final ColumnType columnType = ColumnType.fromCapabilities(index.getColumnCapabilities(column));

      // index.getOrdering() may include columns that don't exist, such as if they were omitted due to
      // being 100% nulls. Don't add those to the row signature.
      if (columnType != null) {
        builder.add(column, columnType);
      }
    }

    return builder.build();
  }

  /**
   * Build the row signature for a clustered segment. Top-level columns are empty, so column types are sourced from:
   *   - the summary's clustering {@link RowSignature} for clustering columns;
   *   - the first cluster group's sub-index for everything else (all groups share the same data-column shape).
   */
  private RowSignature getClusteredRowSignature(ClusteredValueGroupsBaseTableSchema clusterSummary)
  {
    final LinkedHashSet<String> columns = new LinkedHashSet<>();

    for (final OrderBy orderBy : clusterSummary.getOrdering()) {
      columns.add(orderBy.getColumnName());
    }
    columns.add(ColumnHolder.TIME_COLUMN_NAME);
    columns.addAll(clusterSummary.getColumnNames());

    final RowSignature.Builder builder = RowSignature.builder();
    for (final String column : columns) {
      final ColumnType columnType = ColumnType.fromCapabilities(index.getColumnCapabilities(column));
      if (columnType != null) {
        builder.add(column, columnType);
      }
    }
    return builder.build();
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    return index.getColumnCapabilities(column);
  }
}
