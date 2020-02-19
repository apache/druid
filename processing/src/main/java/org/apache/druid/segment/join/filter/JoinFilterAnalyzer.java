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

import com.google.common.collect.ImmutableList;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.filter.AndFilter;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.filter.OrFilter;
import org.apache.druid.segment.filter.SelectorFilter;
import org.apache.druid.segment.join.Equality;
import org.apache.druid.segment.join.HashJoinSegmentStorageAdapter;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.segment.join.JoinableClause;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * When there is a filter in a join query, we can sometimes improve performance by applying parts of the filter
 * when we first read from the base table instead of after the join.
 *
 * This class provides a {@link #splitFilter(HashJoinSegmentStorageAdapter, Set, Filter, boolean)} method that
 * takes a filter and splits it into a portion that should be applied to the base table prior to the join, and a
 * portion that should be applied after the join.
 *
 * The first step of the filter splitting is to convert the filter into
 * https://en.wikipedia.org/wiki/Conjunctive_normal_form (an AND of ORs). This allows us to consider each
 * OR clause independently as a candidate for filter push down to the base table.
 *
 * A filter clause can be pushed down if it meets one of the following conditions:
 * - The filter only applies to columns from the base table
 * - The filter applies to columns from the join table, and we determine that the filter can be rewritten
 *   into a filter on columns from the base table
 *
 * For the second case, where we rewrite filter clauses, the rewritten clause can be less selective than the original,
 * so we preserve the original clause in the post-join filtering phase.
 */
public class JoinFilterAnalyzer
{
  private static final String PUSH_DOWN_VIRTUAL_COLUMN_NAME_BASE = "JOIN-FILTER-PUSHDOWN-VIRTUAL-COLUMN-";
  private static final ColumnSelectorFactory ALL_NULL_COLUMN_SELECTOR_FACTORY = new AllNullColumnSelectorFactory();

  /**
   * Analyze a filter and return a JoinFilterSplit indicating what parts of the filter should be applied pre-join
   * and post-join.
   *
   * @param hashJoinSegmentStorageAdapter The storage adapter that is being queried
   * @param baseColumnNames               Set of names of columns that belong to the base table,
   *                                      including pre-join virtual columns
   * @param originalFilter                Original filter from the query
   * @param enableFilterPushDown          Whether to enable filter push down
   * @return A JoinFilterSplit indicating what parts of the filter should be applied pre-join
   *         and post-join.
   */
  public static JoinFilterSplit splitFilter(
      HashJoinSegmentStorageAdapter hashJoinSegmentStorageAdapter,
      Set<String> baseColumnNames,
      @Nullable Filter originalFilter,
      boolean enableFilterPushDown
  )
  {
    if (originalFilter == null) {
      return new JoinFilterSplit(
          null,
          null,
          ImmutableList.of()
      );
    }

    if (!enableFilterPushDown) {
      return new JoinFilterSplit(
          null,
          originalFilter,
          ImmutableList.of()
      );
    }

    Filter normalizedFilter = Filters.convertToCNF(originalFilter);

    // build the prefix and equicondition maps
    // We should check that the prefixes do not duplicate or shadow each other. This is not currently implemented,
    // but this is tracked at https://github.com/apache/druid/issues/9329
    Map<String, Set<Expr>> equiconditions = new HashMap<>();
    Map<String, JoinableClause> prefixes = new HashMap<>();
    for (JoinableClause clause : hashJoinSegmentStorageAdapter.getClauses()) {
      prefixes.put(clause.getPrefix(), clause);
      for (Equality equality : clause.getCondition().getEquiConditions()) {
        Set<Expr> exprsForRhs = equiconditions.computeIfAbsent(
            clause.getPrefix() + equality.getRightColumn(),
            (rhs) -> {
              return new HashSet<>();
            }
        );
        exprsForRhs.add(equality.getLeftExpr());
      }
    }

    // List of candidates for pushdown
    // CNF normalization will generate either
    // - an AND filter with multiple subfilters
    // - or a single non-AND subfilter which cannot be split further
    List<Filter> normalizedOrClauses;
    if (normalizedFilter instanceof AndFilter) {
      normalizedOrClauses = ((AndFilter) normalizedFilter).getFilters();
    } else {
      normalizedOrClauses = Collections.singletonList(normalizedFilter);
    }

    // Pushdown filters, rewriting if necessary
    List<Filter> leftFilters = new ArrayList<>();
    List<Filter> rightFilters = new ArrayList<>();
    List<VirtualColumn> pushDownVirtualColumns = new ArrayList<>();
    Map<String, Optional<List<JoinFilterColumnCorrelationAnalysis>>> correlationCache = new HashMap<>();

    for (Filter orClause : normalizedOrClauses) {
      JoinFilterAnalysis joinFilterAnalysis = analyzeJoinFilterClause(
          baseColumnNames,
          orClause,
          prefixes,
          equiconditions,
          correlationCache
      );
      if (joinFilterAnalysis.isCanPushDown()) {
        leftFilters.add(joinFilterAnalysis.getPushDownFilter().get());
        if (!joinFilterAnalysis.getPushDownVirtualColumns().isEmpty()) {
          pushDownVirtualColumns.addAll(joinFilterAnalysis.getPushDownVirtualColumns());
        }
      }
      if (joinFilterAnalysis.isRetainAfterJoin()) {
        rightFilters.add(joinFilterAnalysis.getOriginalFilter());
      }
    }

    return new JoinFilterSplit(
        Filters.and(leftFilters),
        Filters.and(rightFilters),
        pushDownVirtualColumns
    );
  }



  /**
   * Analyze a filter clause from a filter that is in conjunctive normal form (AND of ORs).
   * The clause is expected to be an OR filter or a leaf filter.
   *
   * @param baseColumnNames  Set of names of columns that belong to the base table, including pre-join virtual columns
   * @param filterClause     Individual filter clause (an OR filter or a leaf filter) from a filter that is in CNF
   * @param prefixes         Map of table prefixes
   * @param equiconditions   Equicondition map
   * @param correlationCache Cache of column correlation analyses.
   *
   * @return a JoinFilterAnalysis that contains a possible filter rewrite and information on how to handle the filter.
   */
  private static JoinFilterAnalysis analyzeJoinFilterClause(
      Set<String> baseColumnNames,
      Filter filterClause,
      Map<String, JoinableClause> prefixes,
      Map<String, Set<Expr>> equiconditions,
      Map<String, Optional<List<JoinFilterColumnCorrelationAnalysis>>> correlationCache
  )
  {
    // NULL matching conditions are not currently pushed down.
    // They require special consideration based on the join type, and for simplicity of the initial implementation
    // this is not currently handled.
    if (filterMatchesNull(filterClause)) {
      return JoinFilterAnalysis.createNoPushdownFilterAnalysis(filterClause);
    }

    // Currently we only support rewrites of selector filters and selector filters within OR filters.
    if (filterClause instanceof SelectorFilter) {
      return rewriteSelectorFilter(
          baseColumnNames,
          (SelectorFilter) filterClause,
          prefixes,
          equiconditions,
          correlationCache
      );
    }

    if (filterClause instanceof OrFilter) {
      return rewriteOrFilter(
          baseColumnNames,
          (OrFilter) filterClause,
          prefixes,
          equiconditions,
          correlationCache
      );
    }

    for (String requiredColumn : filterClause.getRequiredColumns()) {
      if (!baseColumnNames.contains(requiredColumn)) {
        return JoinFilterAnalysis.createNoPushdownFilterAnalysis(filterClause);
      }
    }
    return new JoinFilterAnalysis(
        false,
        filterClause,
        filterClause,
        ImmutableList.of()
    );
  }

  /**
   * Potentially rewrite the subfilters of an OR filter so that the whole OR filter can be pushed down to
   * the base table.
   *
   * @param baseColumnNames  Set of names of columns that belong to the base table, including pre-join virtual columns
   * @param orFilter         OrFilter to be rewritten
   * @param prefixes         Map of table prefixes to clauses
   * @param equiconditions   Map of equiconditions
   * @param correlationCache Column correlation analysis cache. This will be potentially modified by adding
   *                         any new column correlation analyses to the cache.
   *
   * @return A JoinFilterAnalysis indicating how to handle the potentially rewritten filter
   */
  private static JoinFilterAnalysis rewriteOrFilter(
      Set<String> baseColumnNames,
      OrFilter orFilter,
      Map<String, JoinableClause> prefixes,
      Map<String, Set<Expr>> equiconditions,
      Map<String, Optional<List<JoinFilterColumnCorrelationAnalysis>>> correlationCache
  )
  {
    boolean retainRhs = false;

    List<Filter> newFilters = new ArrayList<>();
    for (Filter filter : orFilter.getFilters()) {
      boolean allBaseColumns = true;
      for (String requiredColumn : filter.getRequiredColumns()) {
        if (!baseColumnNames.contains(requiredColumn)) {
          allBaseColumns = false;
        }
      }

      if (!allBaseColumns) {
        retainRhs = true;
        if (filter instanceof SelectorFilter) {
          JoinFilterAnalysis rewritten = rewriteSelectorFilter(
              baseColumnNames,
              (SelectorFilter) filter,
              prefixes,
              equiconditions,
              correlationCache
          );
          if (!rewritten.isCanPushDown()) {
            return JoinFilterAnalysis.createNoPushdownFilterAnalysis(orFilter);
          } else {
            newFilters.add(rewritten.getPushDownFilter().get());
          }
        } else {
          return JoinFilterAnalysis.createNoPushdownFilterAnalysis(orFilter);
        }
      } else {
        newFilters.add(filter);
      }
    }

    return new JoinFilterAnalysis(
        retainRhs,
        orFilter,
        new OrFilter(newFilters),
        ImmutableList.of()
    );
  }

  /**
   * Rewrites a selector filter on a join table into an IN filter on the base table.
   *
   * @param baseColumnNames  Set of names of columns that belong to the base table, including pre-join virtual
   *                         columns
   * @param selectorFilter   SelectorFilter to be rewritten
   * @param prefixes         Map of join table prefixes to clauses
   * @param equiconditions   Map of equiconditions
   * @param correlationCache Cache of column correlation analyses. This will be potentially modified by adding
   *                         any new column correlation analyses to the cache.
   *
   * @return A JoinFilterAnalysis that indicates how to handle the potentially rewritten filter
   */
  private static JoinFilterAnalysis rewriteSelectorFilter(
      Set<String> baseColumnNames,
      SelectorFilter selectorFilter,
      Map<String, JoinableClause> prefixes,
      Map<String, Set<Expr>> equiconditions,
      Map<String, Optional<List<JoinFilterColumnCorrelationAnalysis>>> correlationCache
  )
  {
    String filteringColumn = selectorFilter.getDimension();
    for (Map.Entry<String, JoinableClause> prefixAndClause : prefixes.entrySet()) {
      if (prefixAndClause.getValue().includesColumn(filteringColumn)) {
        Optional<List<JoinFilterColumnCorrelationAnalysis>> correlations = correlationCache.computeIfAbsent(
            prefixAndClause.getKey(),
            p -> findCorrelatedBaseTableColumns(
                baseColumnNames,
                p,
                prefixes.get(p),
                equiconditions
            )
        );

        if (!correlations.isPresent()) {
          return JoinFilterAnalysis.createNoPushdownFilterAnalysis(selectorFilter);
        }

        List<Filter> newFilters = new ArrayList<>();
        List<VirtualColumn> pushdownVirtualColumns = new ArrayList<>();

        for (JoinFilterColumnCorrelationAnalysis correlationAnalysis : correlations.get()) {
          if (correlationAnalysis.supportsPushDown()) {
            Set<String> correlatedValues = getCorrelatedValuesForPushDown(
                selectorFilter.getDimension(),
                selectorFilter.getValue(),
                correlationAnalysis.getJoinColumn(),
                prefixAndClause.getValue()
            );

            if (correlatedValues.isEmpty()) {
              return JoinFilterAnalysis.createNoPushdownFilterAnalysis(selectorFilter);
            }

            for (String correlatedBaseColumn : correlationAnalysis.getBaseColumns()) {
              Filter rewrittenFilter = new InDimFilter(
                  correlatedBaseColumn,
                  correlatedValues,
                  null,
                  null
              ).toFilter();
              newFilters.add(rewrittenFilter);
            }

            for (Expr correlatedBaseExpr : correlationAnalysis.getBaseExpressions()) {
              // We need to create a virtual column for the expressions when pushing down.
              // Note that this block is never entered right now, since correlationAnalysis.supportsPushDown()
              // will return false if there any correlated expressions on the base table.
              // Pushdown of such filters is disabled until the expressions system supports converting an expression
              // into a String representation that can be reparsed into the same expression.
              // https://github.com/apache/druid/issues/9326 tracks this expressions issue.
              String vcName = getCorrelatedBaseExprVirtualColumnName(pushdownVirtualColumns.size());

              VirtualColumn correlatedBaseExprVirtualColumn = new ExpressionVirtualColumn(
                  vcName,
                  correlatedBaseExpr,
                  ValueType.STRING
              );
              pushdownVirtualColumns.add(correlatedBaseExprVirtualColumn);

              Filter rewrittenFilter = new InDimFilter(
                  vcName,
                  correlatedValues,
                  null,
                  null
              ).toFilter();
              newFilters.add(rewrittenFilter);
            }
          }
        }

        if (newFilters.isEmpty()) {
          return JoinFilterAnalysis.createNoPushdownFilterAnalysis(selectorFilter);
        }

        return new JoinFilterAnalysis(
            true,
            selectorFilter,
            Filters.and(newFilters),
            pushdownVirtualColumns
        );
      }
    }

    // We're not filtering directly on a column from one of the join tables, but
    // we might be filtering on a post-join virtual column (which won't have a join prefix). We cannot
    // push down such filters, so check that the filtering column appears in the set of base column names (which
    // includes pre-join virtual columns).
    if (baseColumnNames.contains(filteringColumn)) {
      return new JoinFilterAnalysis(
          false,
          selectorFilter,
          selectorFilter,
          ImmutableList.of()
      );
    } else {
      return JoinFilterAnalysis.createNoPushdownFilterAnalysis(selectorFilter);
    }
  }

  private static String getCorrelatedBaseExprVirtualColumnName(int counter)
  {
    // May want to have this check other column names to absolutely prevent name conflicts
    return PUSH_DOWN_VIRTUAL_COLUMN_NAME_BASE + counter;
  }

  /**
   * Helper method for rewriting filters on join table columns into filters on base table columns.
   *
   * @param filterColumn           A join table column that we're filtering on
   * @param filterValue            The value to filter on
   * @param correlatedJoinColumn   A join table column that appears as the RHS of an equicondition, which we can correlate
   *                               with a column on the base table
   * @param clauseForFilteredTable The joinable clause that corresponds to the join table being filtered on
   *
   * @return A list of values of the correlatedJoinColumn that appear in rows where filterColumn = filterValue
   * Returns an empty set if we cannot determine the correlated values.
   */
  private static Set<String> getCorrelatedValuesForPushDown(
      String filterColumn,
      String filterValue,
      String correlatedJoinColumn,
      JoinableClause clauseForFilteredTable
  )
  {
    String filterColumnNoPrefix = filterColumn.substring(clauseForFilteredTable.getPrefix().length());
    String correlatedColumnNoPrefix = correlatedJoinColumn.substring(clauseForFilteredTable.getPrefix().length());

    return clauseForFilteredTable.getJoinable().getCorrelatedColumnValues(
        filterColumnNoPrefix,
        filterValue,
        correlatedColumnNoPrefix
    );
  }

  /**
   * For each rhs column that appears in the equiconditions for a table's JoinableClause,
   * we try to determine what base table columns are related to the rhs column through the total set of equiconditions.
   * We do this by searching backwards through the chain of join equiconditions using the provided equicondition map.
   *
   * For example, suppose we have 3 tables, A,B,C, joined with the following conditions, where A is the base table:
   *   A.joinColumn == B.joinColumn
   *   B.joinColum == C.joinColumn
   *
   * We would determine that C.joinColumn is correlated with A.joinColumn: we first see that
   * C.joinColumn is linked to B.joinColumn which in turn is linked to A.joinColumn
   *
   * Suppose we had the following join conditions instead:
   *   f(A.joinColumn) == B.joinColumn
   *   B.joinColum == C.joinColumn
   * In this case, the JoinFilterColumnCorrelationAnalysis for C.joinColumn would be linked to f(A.joinColumn).
   *
   * Suppose we had the following join conditions instead:
   *   A.joinColumn == B.joinColumn
   *   f(B.joinColum) == C.joinColumn
   *
   * Because we cannot reverse the function f() applied to the second table B in all cases,
   * we cannot relate C.joinColumn to A.joinColumn, and we would not generate a correlation for C.joinColumn
   *
   * @param baseColumnNames      Set of names of columns that belong to the base table, including pre-join virtual
   *                             columns
   * @param tablePrefix          Prefix for a join table
   * @param clauseForTablePrefix Joinable clause for the prefix
   * @param equiConditions       Map of equiconditions, keyed by the right hand columns
   *
   * @return A list of correlatation analyses for the equicondition RHS columns that reside in the table associated with
   * the tablePrefix
   */
  private static Optional<List<JoinFilterColumnCorrelationAnalysis>> findCorrelatedBaseTableColumns(
      Set<String> baseColumnNames,
      String tablePrefix,
      JoinableClause clauseForTablePrefix,
      Map<String, Set<Expr>> equiConditions
  )
  {
    JoinConditionAnalysis jca = clauseForTablePrefix.getCondition();

    Set<String> rhsColumns = new HashSet<>();
    for (Equality eq : jca.getEquiConditions()) {
      rhsColumns.add(tablePrefix + eq.getRightColumn());
    }

    List<JoinFilterColumnCorrelationAnalysis> correlations = new ArrayList<>();

    for (String rhsColumn : rhsColumns) {
      Set<String> correlatedBaseColumns = new HashSet<>();
      Set<Expr> correlatedBaseExpressions = new HashSet<>();

      getCorrelationForRHSColumn(
          baseColumnNames,
          equiConditions,
          rhsColumn,
          correlatedBaseColumns,
          correlatedBaseExpressions
      );

      if (correlatedBaseColumns.isEmpty() && correlatedBaseExpressions.isEmpty()) {
        return Optional.empty();
      }

      correlations.add(
          new JoinFilterColumnCorrelationAnalysis(
              rhsColumn,
              correlatedBaseColumns,
              correlatedBaseExpressions
          )
      );
    }

    List<JoinFilterColumnCorrelationAnalysis> dedupCorrelations = eliminateCorrelationDuplicates(correlations);

    return Optional.of(dedupCorrelations);
  }

  /**
   * Helper method for {@link #findCorrelatedBaseTableColumns} that determines correlated base table columns
   * and/or expressions for a single RHS column and adds them to the provided sets as it traverses the
   * equicondition column relationships.
   *
   * @param baseColumnNames  Set of names of columns that belong to the base table, including pre-join virtual columns
   * @param equiConditions Map of equiconditions, keyed by the right hand columns
   * @param rhsColumn RHS column to find base table correlations for
   * @param correlatedBaseColumns Set of correlated base column names for the provided RHS column. Will be modified.
   * @param correlatedBaseExpressions Set of correlated base column expressions for the provided RHS column. Will be
   *                                  modified.
   */
  private static void getCorrelationForRHSColumn(
      Set<String> baseColumnNames,
      Map<String, Set<Expr>> equiConditions,
      String rhsColumn,
      Set<String> correlatedBaseColumns,
      Set<Expr> correlatedBaseExpressions
  )
  {
    String findMappingFor = rhsColumn;
    Set<Expr> lhsExprs = equiConditions.get(findMappingFor);
    if (lhsExprs == null) {
      return;
    }

    for (Expr lhsExpr : lhsExprs) {
      String identifier = lhsExpr.getBindingIfIdentifier();
      if (identifier == null) {
        // We push down if the function only requires base table columns
        Expr.BindingDetails bindingDetails = lhsExpr.analyzeInputs();
        Set<String> requiredBindings = bindingDetails.getRequiredBindings();
        if (!requiredBindings.stream().allMatch(requiredBinding -> baseColumnNames.contains(requiredBinding))) {
          break;
        }
        correlatedBaseExpressions.add(lhsExpr);
      } else {
        // simple identifier, see if we can correlate it with a column on the base table
        findMappingFor = identifier;
        if (baseColumnNames.contains(identifier)) {
          correlatedBaseColumns.add(findMappingFor);
        } else {
          getCorrelationForRHSColumn(
              baseColumnNames,
              equiConditions,
              findMappingFor,
              correlatedBaseColumns,
              correlatedBaseExpressions
          );
        }
      }
    }
  }

  /**
   * Given a list of JoinFilterColumnCorrelationAnalysis, prune the list so that we only have one
   * JoinFilterColumnCorrelationAnalysis for each unique combination of base columns.
   *
   * Suppose we have a join condition like the following, where A is the base table:
   *   A.joinColumn == B.joinColumn && A.joinColumn == B.joinColumn2
   *
   * We only need to consider one correlation to A.joinColumn since B.joinColumn and B.joinColumn2 must
   * have the same value in any row that matches the join condition.
   *
   * In the future this method could consider which column correlation should be preserved based on availability of
   * indices and other heuristics.
   *
   * When push down of filters with LHS expressions in the join condition is supported, this method should also
   * consider expressions.
   *
   * @param originalList Original list of column correlation analyses.
   * @return Pruned list of column correlation analyses.
   */
  private static List<JoinFilterColumnCorrelationAnalysis> eliminateCorrelationDuplicates(
      List<JoinFilterColumnCorrelationAnalysis> originalList
  )
  {
    Map<List<String>, JoinFilterColumnCorrelationAnalysis> uniquesMap = new HashMap<>();
    for (JoinFilterColumnCorrelationAnalysis jca : originalList) {
      uniquesMap.put(jca.getBaseColumns(), jca);
    }

    return new ArrayList<>(uniquesMap.values());
  }

  private static boolean filterMatchesNull(Filter filter)
  {
    ValueMatcher valueMatcher = filter.makeMatcher(ALL_NULL_COLUMN_SELECTOR_FACTORY);
    return valueMatcher.matches();
  }
}
