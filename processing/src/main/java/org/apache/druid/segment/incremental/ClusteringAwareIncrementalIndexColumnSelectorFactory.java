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
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.projections.ClusteringColumnSelectorFactory;

import javax.annotation.Nullable;

/**
 * An {@link IncrementalIndexColumnSelectorFactory} that also serves a cluster group's clustering columns as per-group
 * constants. A clustered base table stores each cluster group's rows WITHOUT the (constant) clustering columns, so the
 * group's row selector cannot resolve them directly.
 */
final class ClusteringAwareIncrementalIndexColumnSelectorFactory extends IncrementalIndexColumnSelectorFactory
{
  private final RowSignature clusteringColumns;
  /**
   * Serves the clustering columns as per-group constants. Its delegate is intentionally the throwing placeholder: this
   * factory is only ever asked for clustering columns, and {@link ClusteringColumnSelectorFactory} serves those from
   * the group's constant tuple without touching its delegate.
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
    this.clusteringConstants = new ClusteringColumnSelectorFactory(
        ClusteringColumnSelectorFactory.UNINITIALIZED_DELEGATE,
        clusteringColumns,
        clusteringValues
    );
  }

  @Override
  public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
  {
    if (clusteringColumns.indexOf(dimensionSpec.getDimension()) < 0) {
      return super.makeDimensionSelector(dimensionSpec);
    }
    return clusteringConstants.makeDimensionSelector(dimensionSpec);
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(String columnName)
  {
    if (clusteringColumns.indexOf(columnName) < 0) {
      return super.makeColumnValueSelector(columnName);
    }
    return clusteringConstants.makeColumnValueSelector(columnName);
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String columnName)
  {
    if (clusteringColumns.indexOf(columnName) < 0) {
      return super.getColumnCapabilities(columnName);
    }
    return clusteringConstants.getColumnCapabilities(columnName);
  }
}
