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

import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;

/**
 * Capabilities reported by a selector factory that may read several cluster groups, such as
 * {@link ClusteringColumnSelectorFactory}, {@link ClusteringVectorColumnSelectorFactory}, and
 * {@link MergingColumnSelectorFactory}.
 * <p>
 * Clustering columns are reported as constants, unless a query virtual column shadows them. Other columns are reported
 * from an inspector covering every group, with the query virtual columns applied, and never as dictionary-encoded:
 * each group has its own dictionary, so dictionary ids are not stable across groups, and an engine that keyed on them
 * would conflate distinct values from different groups.
 */
final class MultiGroupColumnInspector implements ColumnInspector
{
  private final RowSignature clusteringColumns;
  private final VirtualColumns queryVirtualColumns;
  private final ColumnInspector allGroupsInspector;

  /**
   * Creates an inspector for a cursor with the given clustering columns and query virtual columns.
   * {@code allGroupsInspector} must cover every cluster group the cursor may read, including its clustering columns,
   * which query virtual columns may read.
   */
  MultiGroupColumnInspector(
      RowSignature clusteringColumns,
      VirtualColumns queryVirtualColumns,
      ColumnInspector allGroupsInspector
  )
  {
    this.clusteringColumns = clusteringColumns;
    this.queryVirtualColumns = queryVirtualColumns;
    this.allGroupsInspector = queryVirtualColumns.wrapInspector(allGroupsInspector);
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    final int clusteringIndex = clusteringColumns.indexOf(column);
    if (clusteringIndex >= 0 && !queryVirtualColumns.exists(column)) {
      return ClusteredColumnInspector.clusteringColumnCapabilities(
          clusteringColumns.getColumnType(clusteringIndex).orElseThrow()
      );
    }

    final ColumnCapabilities capabilities = allGroupsInspector.getColumnCapabilities(column);
    if (capabilities == null) {
      return null;
    }
    return ColumnCapabilitiesImpl.copyOf(capabilities)
                                 .setDictionaryEncoded(false)
                                 .setDictionaryValuesSorted(false)
                                 .setDictionaryValuesUnique(false)
                                 .setHasBitmapIndexes(false);
  }
}
