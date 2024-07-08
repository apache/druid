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

import javax.annotation.Nullable;
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
      @Nullable Filter baseTableFilter
  )
  {
    final Comparator<VirtualColumn> sorter = Comparator.comparing(VirtualColumn::getOutputName);
    final SortedSet<VirtualColumn> preJoinVirtualColumns = new TreeSet<>(sorter);
    final SortedSet<VirtualColumn> postJoinVirtualColumns = new TreeSet<>(sorter);
    final Set<String> baseColumnNames = new HashSet<>(baseTableSignature.getColumnNames());
    final Set<String> baseFilterColumns = baseTableFilter == null
                                                       ? Collections.emptySet()
                                                       : new HashSet<>(baseTableFilter.getRequiredColumns());
    final Set<String> unresolvedNames = new HashSet<>();

    final Set<VirtualColumn> unresolvedSet = new HashSet<>();
    // do a first pass to eliminate those which depend directly on the base table
    for (VirtualColumn vc : virtualColumns.getVirtualColumns()) {
      if (baseColumnNames.containsAll(vc.requiredColumns()) || baseFilterColumns.contains(vc.getOutputName())) {
        preJoinVirtualColumns.add(vc);
        baseColumnNames.add(vc.getOutputName());
        final Set<VirtualColumn> dependencies = resolveDependencies(virtualColumns, vc);
        for (VirtualColumn dep : dependencies) {
          preJoinVirtualColumns.add(dep);
          postJoinVirtualColumns.remove(dep);
          unresolvedSet.remove(dep);
          baseColumnNames.add(dep.getOutputName());
        }
      } else {
        unresolvedSet.add(vc);
        unresolvedNames.add(vc.getOutputName());
      }
    }
    final Deque<VirtualColumn> unresolved = new ArrayDeque<>(unresolvedSet);
    while (!unresolved.isEmpty()) {
      final VirtualColumn virtualColumn = unresolved.poll();
      if (baseColumnNames.containsAll(virtualColumn.requiredColumns())) {
        preJoinVirtualColumns.add(virtualColumn);
        baseColumnNames.add(virtualColumn.getOutputName());
        unresolvedNames.remove(virtualColumn.getOutputName());
      } else {
        boolean hasUndecidedDepencies = false;
        for (String columnName : virtualColumn.requiredColumns()) {
          if (unresolvedNames.contains(columnName)) {
            hasUndecidedDepencies = true;
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

  public List<VirtualColumn> getPreJoinVirtualColumns()
  {
    return preJoinVirtualColumns;
  }

  public List<VirtualColumn> getPostJoinVirtualColumns()
  {
    return postJoinVirtualColumns;
  }
}
