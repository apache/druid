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

package org.apache.druid.segment.incremental;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Iterables;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ConcatenatingCursor;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.EmptyCursorHolder;
import org.apache.druid.segment.ResidentCursorFactory;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.projections.ClusterGroupQueryPlan;
import org.apache.druid.segment.projections.ClusteredValueGroupsBaseTableSchema;
import org.apache.druid.segment.projections.ClusteringColumnSelectorFactory;
import org.apache.druid.segment.projections.Projections;
import org.apache.druid.segment.projections.QueryableProjection;
import org.apache.druid.segment.projections.TableClusterGroupSpec;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class IncrementalIndexCursorFactory implements ResidentCursorFactory
{
  private static final ColumnCapabilities.CoercionLogic COERCE_LOGIC =
      new ColumnCapabilities.CoercionLogic()
      {
        @Override
        public boolean dictionaryEncoded()
        {
          return false;
        }

        @Override
        public boolean dictionaryValuesSorted()
        {
          return false;
        }

        @Override
        public boolean dictionaryValuesUnique()
        {
          return true;
        }

        @Override
        public boolean multipleValues()
        {
          return false;
        }

        @Override
        public boolean hasNulls()
        {
          return false;
        }
      };

  private final IncrementalIndex index;

  public IncrementalIndexCursorFactory(IncrementalIndex index)
  {
    this.index = index;
  }

  @Override
  public CursorHolder makeCursorHolder(CursorBuildSpec spec)
  {
    final QueryableProjection<IncrementalIndexRowSelector> projection = index.getProjection(spec);
    if (projection == null) {
      // Clustered base tables keep their rows in per-tuple cluster groups, not the base facts holder (which is
      // empty), so a plain IncrementalIndexCursorHolder over the base index would yield no rows. Dispatch to the
      // clustered path, mirroring historical equivalent.
      if (index instanceof OnheapIncrementalIndex) {
        final OnHeapClusteredBaseTable clusteredBaseTable = ((OnheapIncrementalIndex) index).getClusteredBaseTable();
        if (clusteredBaseTable != null) {
          return makeClusteredCursorHolder(spec, clusteredBaseTable);
        }
      }
      return new IncrementalIndexCursorHolder(index, spec);
    } else {
      // currently we only have aggregated projections, so isPreAggregated is always true
      return new IncrementalIndexCursorHolder(
          projection.getRowSelector(),
          projection.getCursorBuildSpec()
      )
      {
        @Override
        public ColumnSelectorFactory makeSelectorFactory(CursorBuildSpec buildSpec, IncrementalIndexRowHolder currEntry)
        {
          return projection.wrapColumnSelectorFactory(super.makeSelectorFactory(buildSpec, currEntry));
        }

        @Override
        public boolean isPreAggregated()
        {
          return true;
        }

        @Override
        public List<AggregatorFactory> getAggregatorsForPreAggregated()
        {
          return projection.getCursorBuildSpec().getAggregators();
        }
      };
    }
  }

  /**
   * Build a cursor holder for clustered base table using {@link Projections#planClusterGroupQuery}
   */
  private CursorHolder makeClusteredCursorHolder(CursorBuildSpec spec, OnHeapClusteredBaseTable clusteredBaseTable)
  {
    final ClusteredValueGroupsBaseTableSchema summary = clusteredBaseTable.toMetadataSchema();
    final ClusterGroupQueryPlan plan = Projections.planClusterGroupQuery(summary.getClusterGroups(), spec);
    final List<TableClusterGroupSpec> surviving = plan.survivingGroups();

    if (surviving.isEmpty()) {
      return EmptyCursorHolder.INSTANCE;
    }

    final RowSignature clusteringColumns = summary.getClusteringColumns();
    final List<Object[]> clusteringValuesByGroup = new ArrayList<>(surviving.size());
    final List<Supplier<CursorHolder>> holderSuppliers = new ArrayList<>(surviving.size());
    final Closer closer = Closer.create();
    for (TableClusterGroupSpec valueGroup : surviving) {
      final OnHeapClusterGroup group = clusteredBaseTable.getGroupForClusteringValues(
          valueGroup.lookupClusteringValues()
      );
      if (group == null) {
        throw DruidException.defensive(
            "No cluster group for clustering values [%s]",
            Arrays.toString(valueGroup.lookupClusteringValues())
        );
      }
      clusteringValuesByGroup.add(valueGroup.lookupClusteringValues());
      final CursorBuildSpec groupSpec = plan.rebuildCursorBuildSpec(spec, valueGroup);
      holderSuppliers.add(
          Suppliers.memoize(() -> closer.register(new IncrementalIndexCursorHolder(group, groupSpec)))
      );
    }

    // The wrapper starts with a throwing placeholder delegate; ConcatenatingCursor swaps in each group's real
    // selector factory (and clustering constants) on init, before any selector is exposed.
    final ClusteringColumnSelectorFactory wrapperFactory = new ClusteringColumnSelectorFactory(
        ClusteringColumnSelectorFactory.UNINITIALIZED_DELEGATE,
        clusteringColumns,
        clusteringValuesByGroup.get(0)
    );
    final ConcatenatingCursor cursor = new ConcatenatingCursor(holderSuppliers, clusteringValuesByGroup, wrapperFactory);

    return new CursorHolder()
    {
      @Override
      public Cursor asCursor()
      {
        return cursor;
      }

      @Override
      public List<OrderBy> getOrdering()
      {
        // Groups are walked in clustering-value order and each group's rows follow the segment ordering with the
        // clustering prefix dropped, so the concatenated output is in the full segment ordering.
        return summary.getOrdering();
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

  @Override
  public RowSignature getRowSignature()
  {
    final RowSignature.Builder builder = RowSignature.builder();

    for (final String column : Iterables.concat(index.getDimensionNames(true), index.getMetricNames())) {
      builder.add(column, ColumnType.fromCapabilities(index.getColumnCapabilities(column)));
    }

    return builder.build();
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    return snapshotColumnCapabilities(index, column);
  }

  static ColumnCapabilities snapshotColumnCapabilities(IncrementalIndexRowSelector selector, String column)
  {
    return ColumnCapabilitiesImpl.snapshot(
        selector.getColumnCapabilities(column),
        COERCE_LOGIC
    );
  }
}
