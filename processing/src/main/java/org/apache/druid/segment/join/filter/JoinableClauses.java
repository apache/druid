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

package org.apache.druid.segment.join.filter;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.planning.PreJoinableClause;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.join.JoinPrefixUtils;
import org.apache.druid.segment.join.Joinable;
import org.apache.druid.segment.join.JoinableClause;
import org.apache.druid.segment.join.JoinableFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class JoinableClauses
{
  @Nonnull
  private final List<JoinableClause> joinableClauses;

  /**
   * Builds a list of {@link JoinableClause} corresponding to a list of {@link PreJoinableClause}. This will call
   * {@link JoinableFactory#build} on each one and therefore may be an expensive operation.
   */
  public static JoinableClauses createClauses(
      final List<PreJoinableClause> preClauses,
      final JoinableFactory joinableFactory
  )
  {
    // Since building a JoinableClause can be expensive, check for prefix conflicts before building
    checkPreJoinableClausesForDuplicatesAndShadowing(preClauses);

    List<JoinableClause> joinableClauses = preClauses.stream().map(preJoinableClause -> {
      final Optional<Joinable> joinable = joinableFactory.build(
          preJoinableClause.maybeUnwrapRestrictedDataSource(),
          preJoinableClause.getCondition()
      );

      return new JoinableClause(
          preJoinableClause.getPrefix(),
          joinable.orElseThrow(() -> new ISE("dataSource is not joinable: %s", preJoinableClause.getDataSource())),
          preJoinableClause.getJoinType(),
          preJoinableClause.getCondition()
      );
    }).collect(Collectors.toList());
    return new JoinableClauses(joinableClauses);
  }

  /**
   * Wraps the provided list of pre-built {@link JoinableClause}. This is an inexpensive operation.
   */
  public static JoinableClauses fromList(final List<JoinableClause> clauses)
  {
    return new JoinableClauses(clauses);
  }

  private JoinableClauses(@Nonnull List<JoinableClause> joinableClauses)
  {
    this.joinableClauses = joinableClauses;
  }

  @Nonnull
  public List<JoinableClause> getJoinableClauses()
  {
    return joinableClauses;
  }

  /**
   * Retrieve subset of virtual columns which require inputs which are only present on the join table, either
   * directly or through other virtual columns.
   */
  public Set<VirtualColumn> getPostJoinVirtualColumns(
      final VirtualColumns virtualColumns
  )
  {
    final Set<VirtualColumn> postJoinVirtualColumns = new HashSet<>();
    final Map<String, Boolean> postJoinByName = new HashMap<>();
    for (VirtualColumn virtualColumn : virtualColumns.getVirtualColumns()) {
      if (isPostJoinVirtualColumn(virtualColumn, virtualColumns, postJoinByName)) {
        postJoinVirtualColumns.add(virtualColumn);
      }
    }
    return postJoinVirtualColumns;
  }

  public boolean areSomeColumnsFromJoin(
      Collection<String> columns
  )
  {
    for (String column : columns) {
      if (getColumnFromJoinIfExists(column) != null) {
        return true;
      }
    }
    return false;
  }

  @Nullable
  public JoinableClause getColumnFromJoinIfExists(
      String column
  )
  {
    for (JoinableClause joinableClause : joinableClauses) {
      if (joinableClause.includesColumn(column)) {
        return joinableClause;
      }
    }

    return null;
  }

  /**
   * Whether a virtual column requires a join table column, directly or through other virtual columns in
   * {@code virtualColumns}. Memoizes results in {@code postJoinByName}. Recursion terminates because
   * {@link VirtualColumns} rejects dependency cycles.
   */
  private boolean isPostJoinVirtualColumn(
      final VirtualColumn virtualColumn,
      final VirtualColumns virtualColumns,
      final Map<String, Boolean> postJoinByName
  )
  {
    final Boolean memoized = postJoinByName.get(virtualColumn.getOutputName());
    if (memoized != null) {
      return memoized;
    }

    boolean postJoin = areSomeColumnsFromJoin(virtualColumn.requiredColumns());
    if (!postJoin) {
      for (String column : virtualColumn.requiredColumns()) {
        final VirtualColumn dependency = virtualColumns.getVirtualColumn(column);
        if (dependency != null && isPostJoinVirtualColumn(dependency, virtualColumns, postJoinByName)) {
          postJoin = true;
          break;
        }
      }
    }

    postJoinByName.put(virtualColumn.getOutputName(), postJoin);
    return postJoin;
  }

  private static void checkPreJoinableClausesForDuplicatesAndShadowing(
      final List<PreJoinableClause> preJoinableClauses
  )
  {
    List<String> prefixes = new ArrayList<>();
    for (PreJoinableClause clause : preJoinableClauses) {
      prefixes.add(clause.getPrefix());
    }

    JoinPrefixUtils.checkPrefixesForDuplicatesAndShadowing(prefixes);
  }
}
