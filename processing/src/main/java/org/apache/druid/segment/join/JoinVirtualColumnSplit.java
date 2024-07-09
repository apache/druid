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

package org.apache.druid.segment.join;

import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.filter.JoinFilterSplit;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

public class JoinVirtualColumnSplit
{
  /**
   * Split {@link VirtualColumns} into those which can have inputs which exist on the base table and those which can
   * be deferred until after the join due to not being present in the base table and thus all null
   */
  public static JoinVirtualColumnSplit split(
      RowSignature baseTableSignature,
      VirtualColumns virtualColumns,
      JoinFilterSplit filterSplit
  )
  {
    final Comparator<VirtualColumn> sorter = Comparator.comparing(VirtualColumn::getOutputName);
    final SortedSet<VirtualColumn> preJoinVirtualColumns = new TreeSet<>(sorter);
    final SortedSet<VirtualColumn> postJoinVirtualColumns = new TreeSet<>(sorter);
    final Set<String> baseColumnNames = new HashSet<>(baseTableSignature.getColumnNames());
    final Set<String> baseFilterColumns = filterSplit.getBaseTableFilter()
                                                     .map(Filter::getRequiredColumns)
                                                     .orElse(Collections.emptySet());
    final Set<String> unresolvedNames = new HashSet<>();

    final Set<VirtualColumn> unresolvedSet = new HashSet<>();
    // do a first pass to eliminate those which depend directly on the base table or are required by base table filters
    for (VirtualColumn virtualColumn : virtualColumns.getVirtualColumns()) {
      if (baseColumnNames.containsAll(virtualColumn.requiredColumns())) {
        // virtual column depends on physical columns, or virtual columns which have already been added to the 'pre' set
        preJoinVirtualColumns.add(virtualColumn);
        baseColumnNames.add(virtualColumn.getOutputName());
      } else if (baseFilterColumns.contains(virtualColumn.getOutputName())) {
        // virtual column is required by a base table filter clause
        preJoinVirtualColumns.add(virtualColumn);
        baseColumnNames.add(virtualColumn.getOutputName());
        // also resolve all virtual column dependencies it has and add them to the base table set
        final Set<VirtualColumn> dependencies = resolveDependencies(virtualColumns, virtualColumn);
        for (VirtualColumn dep : dependencies) {
          preJoinVirtualColumns.add(dep);
          postJoinVirtualColumns.remove(dep);
          unresolvedSet.remove(dep);
          baseColumnNames.add(dep.getOutputName());
        }
      } else {
        unresolvedSet.add(virtualColumn);
        unresolvedNames.add(virtualColumn.getOutputName());
      }
    }
    final Deque<VirtualColumn> unresolved = new ArrayDeque<>(unresolvedSet);
    // divide any remaining virtual columns into the 'pre' and 'post' sets.
    while (!unresolved.isEmpty()) {
      final VirtualColumn virtualColumn = unresolved.poll();
      // when a virtual column is added to the 'pre' group, it is also added to baseColumnNames, so if all the inputs
      // are available there we add to that group
      if (baseColumnNames.containsAll(virtualColumn.requiredColumns())) {
        preJoinVirtualColumns.add(virtualColumn);
        baseColumnNames.add(virtualColumn.getOutputName());
        unresolvedNames.remove(virtualColumn.getOutputName());
      } else {
        // if no required inputs are still pending resolution, we add them to the 'post' join set. However, if
        // there are still pending dependency virtual columns to resolve, add back to the queue to try to resolve
        // the next virtual column into the 'pre' or 'post' sets and come back to this one later
        boolean hasUndecidedDepencies = false;
        for (String columnName : virtualColumn.requiredColumns()) {
          if (unresolvedNames.contains(columnName)) {
            hasUndecidedDepencies = true;
            break;
          }
        }
        if (hasUndecidedDepencies) {
          unresolved.add(virtualColumn);
        } else {
          unresolvedNames.remove(virtualColumn.getOutputName());
          postJoinVirtualColumns.add(virtualColumn);
        }
      }
    }
    return new JoinVirtualColumnSplit(new ArrayList<>(preJoinVirtualColumns), new ArrayList<>(postJoinVirtualColumns));
  }

  static Set<VirtualColumn> resolveDependencies(VirtualColumns virtualColumns, VirtualColumn virtualColumn)
  {
    final Deque<VirtualColumn> unresolved = new ArrayDeque<>();
    final Set<VirtualColumn> dependencies = new HashSet<>();
    unresolved.add(virtualColumn);
    while (!unresolved.isEmpty()) {
      final VirtualColumn next = unresolved.poll();
      for (String columnName : next.requiredColumns()) {
        VirtualColumn dependency = virtualColumns.getVirtualColumn(columnName);
        if (dependency != null) {
          unresolved.add(dependency);
          dependencies.add(dependency);
        }
      }
    }
    return dependencies;
  }

  private final List<VirtualColumn> preJoinVirtualColumns;
  private final List<VirtualColumn> postJoinVirtualColumns;

  private JoinVirtualColumnSplit(
      List<VirtualColumn> preJoinVirtualColumns,
      List<VirtualColumn> postJoinVirtualColumns
  )
  {
    this.preJoinVirtualColumns = preJoinVirtualColumns;
    this.postJoinVirtualColumns = postJoinVirtualColumns;
  }

  /**
   * List of {@link VirtualColumn} to compute on the base table
   */
  public List<VirtualColumn> getPreJoinVirtualColumns()
  {
    return preJoinVirtualColumns;
  }

  /**
   * List of {@link VirtualColumn} to compute on the join table
   */
  public List<VirtualColumn> getPostJoinVirtualColumns()
  {
    return postJoinVirtualColumns;
  }
}
