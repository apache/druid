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

import org.apache.druid.query.Order;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.projections.ClusteringColumnSelectorFactory;

import javax.annotation.Nullable;

/**
 * An {@link IncrementalIndexColumnSelectorFactory} that also serves a cluster group's clustering columns as per-group
 * constants. A clustered base table stores each cluster group's rows WITHOUT the (constant) clustering columns, so the
 * group's row selector cannot resolve them directly.
 * <p>
 * The clustering-constant interception only applies when the requested name is NOT owned by a query virtual column. The
 * superclass resolves virtual columns before physical columns, so a query VC whose output name shadows a clustering
 * column must win: intercepting it here would make that VC — and any other retained VC that reads it — silently read
 * the per-group constant instead of the computed value.
 */
final class ClusteringAwareIncrementalIndexColumnSelectorFactory extends IncrementalIndexColumnSelectorFactory
{
  private final RowSignature clusteringColumns;
  private final VirtualColumns queryVirtualColumns;
  /**
   * Serves the clustering columns as per-group constants. Its delegate is intentionally the throwing placeholder: this
   * factory is only ever asked for clustering columns (and only when no query VC shadows them), and
   * {@link ClusteringColumnSelectorFactory} serves those from the group's constant tuple without touching its delegate.
   */
  private final ClusteringColumnSelectorFactory clusteringConstants;

  ClusteringAwareIncrementalIndexColumnSelectorFactory(
      IncrementalIndexRowSelector rowSelector,
      IncrementalIndexRowHolder rowHolder,
      CursorBuildSpec cursorBuildSpec,
      Order timeOrder,
      RowSignature clusteringColumns,
      Object[] clusteringValues
  )
  {
    super(rowSelector, rowHolder, cursorBuildSpec, timeOrder);
    this.clusteringColumns = clusteringColumns;
    this.queryVirtualColumns = cursorBuildSpec.getVirtualColumns();
    this.clusteringConstants = new ClusteringColumnSelectorFactory(
        ClusteringColumnSelectorFactory.UNINITIALIZED_DELEGATE,
        clusteringColumns,
        clusteringValues
    );
  }

  /**
   * Whether {@code name} should be served as this group's clustering constant: only when it is a clustering column AND
   * no query virtual column owns the name. A query VC that shares a clustering column's name shadows it (the superclass
   * resolves virtual columns before physical columns), so deferring to the superclass lets that VC — and anything that
   * reads it through this factory — observe the computed value rather than the constant.
   */
  private boolean servesClusteringConstant(String name)
  {
    return clusteringColumns.indexOf(name) >= 0 && !queryVirtualColumns.exists(name);
  }

  @Override
  public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
  {
    if (servesClusteringConstant(dimensionSpec.getDimension())) {
      return clusteringConstants.makeDimensionSelector(dimensionSpec);
    }
    return super.makeDimensionSelector(dimensionSpec);
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(String columnName)
  {
    if (servesClusteringConstant(columnName)) {
      return clusteringConstants.makeColumnValueSelector(columnName);
    }
    return super.makeColumnValueSelector(columnName);
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String columnName)
  {
    if (servesClusteringConstant(columnName)) {
      return clusteringConstants.getColumnCapabilities(columnName);
    }
    return super.getColumnCapabilities(columnName);
  }
}
