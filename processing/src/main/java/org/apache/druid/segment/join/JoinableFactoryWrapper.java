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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.segment.filter.FalseFilter;
import org.apache.druid.segment.filter.Filters;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A wrapper class over {@link JoinableFactory} for working with {@link Joinable} related classes.
 */
public class JoinableFactoryWrapper
{

  public static final byte JOIN_OPERATION = 0x1;

  private final JoinableFactory joinableFactory;

  @Inject
  public JoinableFactoryWrapper(final JoinableFactory joinableFactory)
  {
    this.joinableFactory = Preconditions.checkNotNull(joinableFactory, "joinableFactory");
  }

  /**
   * Converts any join clauses to filters that can be converted, and returns the rest as-is.
   * <p>
   * See {@link #convertJoinToFilter} for details on the logic.
   */
  public static Pair<List<Filter>, List<JoinableClause>> convertJoinsToFilters(
      final List<JoinableClause> clauses,
      final Set<String> requiredColumns,
      final int maxNumFilterValues
  )
  {
    final List<Filter> filterList = new ArrayList<>();
    final List<JoinableClause> clausesToUse = new ArrayList<>();

    // Join clauses may depend on other, earlier join clauses.
    // We track that using a Multiset, because we'll need to remove required columns one by one as we convert clauses,
    // and multiple clauses may refer to the same column.
    final Multiset<String> columnsRequiredByJoinClauses = HashMultiset.create();

    for (JoinableClause clause : clauses) {
      for (String column : clause.getCondition().getRequiredColumns()) {
        columnsRequiredByJoinClauses.add(column, 1);
      }
    }

    Set<String> rightPrefixes = clauses.stream().map(JoinableClause::getPrefix).collect(Collectors.toSet());
    // Walk through the list of clauses, picking off any from the start of the list that can be converted to filters.
    boolean atStart = true;
    for (JoinableClause clause : clauses) {
      if (atStart) {
        // Remove this clause from columnsRequiredByJoinClauses. It's ok if it relies on itself.
        for (String column : clause.getCondition().getRequiredColumns()) {
          columnsRequiredByJoinClauses.remove(column, 1);
        }

        final JoinClauseToFilterConversion joinClauseToFilterConversion =
            convertJoinToFilter(
                clause,
                Sets.union(requiredColumns, columnsRequiredByJoinClauses.elementSet()),
                maxNumFilterValues,
                rightPrefixes
            );

        // add the converted filter to the filter list
        if (joinClauseToFilterConversion.getConvertedFilter() != null) {
          filterList.add(joinClauseToFilterConversion.getConvertedFilter());
        }
        // if the converted filter is partial, keep the join clause too
        if (!joinClauseToFilterConversion.isJoinClauseFullyConverted()) {
          clausesToUse.add(clause);
          atStart = false;
        }
      } else {
        clausesToUse.add(clause);
      }
    }

    return Pair.of(filterList, clausesToUse);
  }

  /**
   * Converts a join clause into an "in" filter if possible.
   * <p>
   * The requirements are:
   * <p>
   * - it must be an INNER equi-join
   * - the right-hand columns referenced by the condition must not have any duplicate values. If there are duplicates
   * values in the column, then the join is tried to be converted to a filter while maintaining the join clause on top
   * as well for correct results.
   * - no columns from the right-hand side can appear in "requiredColumns". If the columns from right side are required
   * (ie they are directly or indirectly projected in the join output), then the join is tried to be converted to a
   * filter while maintaining the join clause on top as well for correct results.
   *
   * @return {@link JoinClauseToFilterConversion} object which contains the converted filter for the clause and a boolean
   * to represent whether the converted filter encapsulates the whole clause or not. More semantics of the object are
   * present in the class level docs.
   */
  @VisibleForTesting
  static JoinClauseToFilterConversion convertJoinToFilter(
      final JoinableClause clause,
      final Set<String> requiredColumns,
      final int maxNumFilterValues,
      final Set<String> rightPrefixes
  )
  {
    if (clause.getJoinType() == JoinType.INNER
        && clause.getCondition().getNonEquiConditions().isEmpty()
        && clause.getCondition().getEquiConditions().size() > 0) {
      final List<Filter> filters = new ArrayList<>();
      int numValues = maxNumFilterValues;
      // if the right side columns are required, the clause cannot be fully converted
      boolean joinClauseFullyConverted = requiredColumns.stream().noneMatch(clause::includesColumn);

      for (final Equality condition : clause.getCondition().getEquiConditions()) {
        final String leftColumn = condition.getLeftExpr().getBindingIfIdentifier();

        if (leftColumn == null) {
          return new JoinClauseToFilterConversion(null, false);
        }

        // don't add a filter on any right side table columns. only filter on left base table is supported as of now.
        if (rightPrefixes.stream().anyMatch(leftColumn::startsWith)) {
          joinClauseFullyConverted = false;
          continue;
        }

        Joinable.ColumnValuesWithUniqueFlag columnValuesWithUniqueFlag =
            clause.getJoinable().getNonNullColumnValues(condition.getRightColumn(), numValues);
        // For an empty values set, isAllUnique flag will be true only if the column had no non-null values.
        if (columnValuesWithUniqueFlag.getColumnValues().isEmpty()) {
          if (columnValuesWithUniqueFlag.isAllUnique()) {
            return new JoinClauseToFilterConversion(FalseFilter.instance(), true);
          } else {
            joinClauseFullyConverted = false;
          }
          continue;
        }

        numValues -= columnValuesWithUniqueFlag.getColumnValues().size();
        filters.add(Filters.toFilter(new InDimFilter(leftColumn, columnValuesWithUniqueFlag.getColumnValues())));
        if (!columnValuesWithUniqueFlag.isAllUnique()) {
          joinClauseFullyConverted = false;
        }
      }

      return new JoinClauseToFilterConversion(Filters.maybeAnd(filters).orElse(null), joinClauseFullyConverted);
    }

    return new JoinClauseToFilterConversion(null, false);
  }

  public JoinableFactory getJoinableFactory()
  {
    return joinableFactory;
  }

  /**
   * Encapsulates the conversion which happened for a joinable clause.
   * convertedFilter represents the filter which got generated from the conversion.
   * joinClauseFullyConverted represents whether convertedFilter fully encapsulated the joinable clause or not.
   * Encapsulation of the clause means that the filter can replace the whole joinable clause.
   * <p>
   * If convertedFilter is null and joinClauseFullyConverted is true, it means that all parts of the joinable clause can
   * be broken into filters. Further, all the clause conditions are on columns where the right side is only null values.
   * In that case, we replace joinable with a FalseFilter.
   * If convertedFilter is null and joinClauseFullyConverted is false, it means that no parts of the joinable clause can
   * be broken into filters.
   * If convertedFilter is non-null, then joinClauseFullyConverted represents whether the filter encapsulates the clause
   * which was converted.
   */
  private static class JoinClauseToFilterConversion
  {
    private final @Nullable Filter convertedFilter;
    private final boolean joinClauseFullyConverted;

    public JoinClauseToFilterConversion(@Nullable Filter convertedFilter, boolean joinClauseFullyConverted)
    {
      this.convertedFilter = convertedFilter;
      this.joinClauseFullyConverted = joinClauseFullyConverted;
    }

    @Nullable
    public Filter getConvertedFilter()
    {
      return convertedFilter;
    }

    public boolean isJoinClauseFullyConverted()
    {
      return joinClauseFullyConverted;
    }
  }
}
