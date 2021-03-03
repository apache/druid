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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.filter.FalseFilter;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.filter.OrFilter;
import org.apache.druid.segment.filter.SelectorFilter;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;

import java.util.ArrayList;
import java.util.Collection;
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
 * The first step of the filter splitting is to convert the filter into
 * https://en.wikipedia.org/wiki/Conjunctive_normal_form (an AND of ORs). This allows us to consider each
 * OR clause independently as a candidate for filter push down to the base table.
 *
 * A filter clause can be pushed down if it meets one of the following conditions:
 * - The filter only applies to columns from the base table
 * - The filter applies to columns from the join table, and we determine that the filter can be rewritten
 * into a filter on columns from the base table
 *
 * For the second case, where we rewrite filter clauses, the rewritten clause can be less selective than the original,
 * so we preserve the original clause in the post-join filtering phase.
 *
 * The starting point for join analysis is the {@link #computeJoinFilterPreAnalysis} method. This method should be
 * called before performing any per-segment join query work. This method converts the query filter into
 * conjunctive normal form, and splits the CNF clauses into a portion that only references base table columns and
 * a portion that references join table columns. For the filter clauses that apply to join table columns, the
 * pre-analysis step computes the information necessary for rewriting such filters into filters on base table columns.
 *
 * The result of this pre-analysis method should be passed into the next step of join filter analysis, described below.
 *
 * The {@link #splitFilter(JoinFilterPreAnalysis)} method takes the pre-analysis result and optionally applies the
 * filter rewrite and push down operations on a per-segment level.
 */
public class JoinFilterAnalyzer
{
  private static final String PUSH_DOWN_VIRTUAL_COLUMN_NAME_BASE = "JOIN-FILTER-PUSHDOWN-VIRTUAL-COLUMN-";

  /**
   * Before making per-segment filter splitting decisions, we first do a pre-analysis step
   * where we convert the query filter (if any) into conjunctive normal form and then
   * determine the structure of RHS filter rewrites (if any), since this information is shared across all
   * per-segment operations.
   *
   * See {@link JoinFilterPreAnalysis} for details on the result of this pre-analysis step.
   *
   * @param key All the information needed to pre-analyze a filter
   *
   * @return A JoinFilterPreAnalysis containing information determined in this pre-analysis step.
   */
  public static JoinFilterPreAnalysis computeJoinFilterPreAnalysis(final JoinFilterPreAnalysisKey key)
  {
    final List<VirtualColumn> preJoinVirtualColumns = new ArrayList<>();
    final List<VirtualColumn> postJoinVirtualColumns = new ArrayList<>();

    final JoinableClauses joinableClauses = JoinableClauses.fromList(key.getJoinableClauses());
    joinableClauses.splitVirtualColumns(key.getVirtualColumns(), preJoinVirtualColumns, postJoinVirtualColumns);

    final JoinFilterPreAnalysis.Builder preAnalysisBuilder =
        new JoinFilterPreAnalysis.Builder(key, postJoinVirtualColumns);

    if (key.getFilter() == null || !key.getRewriteConfig().isEnableFilterPushDown()) {
      return preAnalysisBuilder.build();
    }

    List<Filter> normalizedOrClauses = Filters.toNormalizedOrClauses(key.getFilter());

    List<Filter> normalizedBaseTableClauses = new ArrayList<>();
    List<Filter> normalizedJoinTableClauses = new ArrayList<>();

    for (Filter orClause : normalizedOrClauses) {
      Set<String> reqColumns = orClause.getRequiredColumns();
      if (joinableClauses.areSomeColumnsFromJoin(reqColumns) || areSomeColumnsFromPostJoinVirtualColumns(
          postJoinVirtualColumns,
          reqColumns
      )) {
        normalizedJoinTableClauses.add(orClause);
      } else {
        normalizedBaseTableClauses.add(orClause);
      }
    }
    preAnalysisBuilder
        .withNormalizedBaseTableClauses(normalizedBaseTableClauses)
        .withNormalizedJoinTableClauses(normalizedJoinTableClauses);
    if (!key.getRewriteConfig().isEnableFilterRewrite()) {
      return preAnalysisBuilder.build();
    }

    // build the equicondition map, used for determining how the tables are connected through joins
    Equiconditions equiconditions = preAnalysisBuilder.computeEquiconditionsFromJoinableClauses();

    JoinFilterCorrelations correlations = JoinFilterCorrelations.computeJoinFilterCorrelations(
        normalizedJoinTableClauses,
        equiconditions,
        joinableClauses,
        key.getRewriteConfig().isEnableRewriteValueColumnFilters(),
        key.getRewriteConfig().getFilterRewriteMaxSize()
    );

    return preAnalysisBuilder.withCorrelations(correlations).build();
  }

  /**
   * @param joinFilterPreAnalysis The pre-analysis computed by {@link #computeJoinFilterPreAnalysis)}
   *
   * @return A JoinFilterSplit indicating what parts of the filter should be applied pre-join and post-join
   */
  public static JoinFilterSplit splitFilter(
      JoinFilterPreAnalysis joinFilterPreAnalysis
  )
  {
    if (joinFilterPreAnalysis.getOriginalFilter() == null || !joinFilterPreAnalysis.isEnableFilterPushDown()) {
      return new JoinFilterSplit(
          null,
          joinFilterPreAnalysis.getOriginalFilter(),
          ImmutableSet.of()
      );
    }

    // Pushdown filters, rewriting if necessary
    List<Filter> leftFilters = new ArrayList<>();
    List<Filter> rightFilters = new ArrayList<>();
    Map<Expr, VirtualColumn> pushDownVirtualColumnsForLhsExprs = new HashMap<>();

    for (Filter baseTableFilter : joinFilterPreAnalysis.getNormalizedBaseTableClauses()) {
      if (!Filters.filterMatchesNull(baseTableFilter)) {
        leftFilters.add(baseTableFilter);
      } else {
        rightFilters.add(baseTableFilter);
      }
    }

    for (Filter orClause : joinFilterPreAnalysis.getNormalizedJoinTableClauses()) {
      JoinFilterAnalysis joinFilterAnalysis = analyzeJoinFilterClause(
          orClause,
          joinFilterPreAnalysis,
          pushDownVirtualColumnsForLhsExprs
      );
      if (joinFilterAnalysis.isCanPushDown()) {
        //noinspection OptionalGetWithoutIsPresent isCanPushDown checks isPresent
        leftFilters.add(joinFilterAnalysis.getPushDownFilter().get());
      }
      if (joinFilterAnalysis.isRetainAfterJoin()) {
        rightFilters.add(joinFilterAnalysis.getOriginalFilter());
      }
    }

    return new JoinFilterSplit(
        Filters.maybeAnd(leftFilters).orElse(null),
        Filters.maybeAnd(rightFilters).orElse(null),
        new HashSet<>(pushDownVirtualColumnsForLhsExprs.values())
    );
  }


  /**
   * Analyze a filter clause from a filter that is in conjunctive normal form (AND of ORs).
   * The clause is expected to be an OR filter or a leaf filter.
   *
   * @param filterClause                      Individual filter clause (an OR filter or a leaf filter) from a filter that is in CNF
   * @param joinFilterPreAnalysis             The pre-analysis computed by {@link #computeJoinFilterPreAnalysis)}
   * @param pushDownVirtualColumnsForLhsExprs Used when there are LHS expressions in the join equiconditions.
   *                                          If we rewrite an RHS filter such that it applies to the LHS expression instead,
   *                                          because the expression existed only in the equicondition, we must create a virtual column
   *                                          on the LHS with the same expression in order to apply the filter.
   *                                          The specific rewriting methods such as {@link #rewriteSelectorFilter} will use this
   *                                          as a cache for virtual columns that they need to created, keyed by the expression, so that
   *                                          they can avoid creating redundant virtual columns.
   *
   * @return a JoinFilterAnalysis that contains a possible filter rewrite and information on how to handle the filter.
   */
  private static JoinFilterAnalysis analyzeJoinFilterClause(
      Filter filterClause,
      JoinFilterPreAnalysis joinFilterPreAnalysis,
      Map<Expr, VirtualColumn> pushDownVirtualColumnsForLhsExprs
  )
  {
    // NULL matching conditions are not currently pushed down.
    // They require special consideration based on the join type, and for simplicity of the initial implementation
    // this is not currently handled.
    if (!joinFilterPreAnalysis.isEnableFilterRewrite() || Filters.filterMatchesNull(filterClause)) {
      return JoinFilterAnalysis.createNoPushdownFilterAnalysis(filterClause);
    }

    if (filterClause instanceof OrFilter) {
      return rewriteOrFilter(
          (OrFilter) filterClause,
          joinFilterPreAnalysis,
          pushDownVirtualColumnsForLhsExprs
      );
    }

    if (joinFilterPreAnalysis.getEquiconditions().doesFilterSupportDirectJoinFilterRewrite(filterClause)) {
      return rewriteFilterDirect(
          filterClause,
          joinFilterPreAnalysis,
          pushDownVirtualColumnsForLhsExprs
      );
    }

    // Currently we only support rewrites of selector filters and selector filters within OR filters.
    if (filterClause instanceof SelectorFilter) {
      return rewriteSelectorFilter(
          (SelectorFilter) filterClause,
          joinFilterPreAnalysis,
          pushDownVirtualColumnsForLhsExprs
      );
    }

    return JoinFilterAnalysis.createNoPushdownFilterAnalysis(filterClause);
  }

  private static JoinFilterAnalysis rewriteFilterDirect(
      Filter filterClause,
      JoinFilterPreAnalysis joinFilterPreAnalysis,
      Map<Expr, VirtualColumn> pushDownVirtualColumnsForLhsExprs
  )
  {
    if (!filterClause.supportsRequiredColumnRewrite()) {
      return JoinFilterAnalysis.createNoPushdownFilterAnalysis(filterClause);
    }

    List<Filter> newFilters = new ArrayList<>();

    // we only support direct rewrites of filters that reference a single column
    String reqColumn = filterClause.getRequiredColumns().iterator().next();

    List<JoinFilterColumnCorrelationAnalysis> correlationAnalyses = joinFilterPreAnalysis.getCorrelationsByDirectFilteringColumn()
                                                                                         .get(reqColumn);

    if (correlationAnalyses == null) {
      return JoinFilterAnalysis.createNoPushdownFilterAnalysis(filterClause);
    }

    for (JoinFilterColumnCorrelationAnalysis correlationAnalysis : correlationAnalyses) {
      if (correlationAnalysis.supportsPushDown()) {
        for (String correlatedBaseColumn : correlationAnalysis.getBaseColumns()) {
          Filter rewrittenFilter = filterClause.rewriteRequiredColumns(ImmutableMap.of(
              reqColumn,
              correlatedBaseColumn
          ));
          newFilters.add(rewrittenFilter);
        }

        for (Expr correlatedBaseExpr : correlationAnalysis.getBaseExpressions()) {
          // We need to create a virtual column for the expressions when pushing down
          VirtualColumn pushDownVirtualColumn = pushDownVirtualColumnsForLhsExprs.computeIfAbsent(
              correlatedBaseExpr,
              (expr) -> {
                String vcName = getCorrelatedBaseExprVirtualColumnName(pushDownVirtualColumnsForLhsExprs.size());
                return new ExpressionVirtualColumn(
                    vcName,
                    correlatedBaseExpr,
                    ValueType.STRING
                );
              }
          );

          Filter rewrittenFilter = filterClause.rewriteRequiredColumns(ImmutableMap.of(
              reqColumn,
              pushDownVirtualColumn.getOutputName()
          ));
          newFilters.add(rewrittenFilter);
        }
      }
    }

    if (newFilters.isEmpty()) {
      return JoinFilterAnalysis.createNoPushdownFilterAnalysis(filterClause);
    }

    return new JoinFilterAnalysis(
        false,
        filterClause,
        Filters.maybeAnd(newFilters).orElse(null)
    );
  }

  /**
   * Potentially rewrite the subfilters of an OR filter so that the whole OR filter can be pushed down to
   * the base table.
   *
   * @param orFilter                          OrFilter to be rewritten
   * @param joinFilterPreAnalysis             The pre-analysis computed by {@link #computeJoinFilterPreAnalysis)}
   * @param pushDownVirtualColumnsForLhsExprs See comments on {@link #analyzeJoinFilterClause}
   *
   * @return A JoinFilterAnalysis indicating how to handle the potentially rewritten filter
   */
  private static JoinFilterAnalysis rewriteOrFilter(
      OrFilter orFilter,
      JoinFilterPreAnalysis joinFilterPreAnalysis,
      Map<Expr, VirtualColumn> pushDownVirtualColumnsForLhsExprs
  )
  {
    List<Filter> newFilters = new ArrayList<>();
    boolean retainRhs = false;

    for (Filter filter : orFilter.getFilters()) {
      if (!joinFilterPreAnalysis.getJoinableClauses().areSomeColumnsFromJoin(filter.getRequiredColumns())) {
        newFilters.add(filter);
        continue;
      }

      JoinFilterAnalysis rewritten = null;
      if (joinFilterPreAnalysis.getEquiconditions()
                               .doesFilterSupportDirectJoinFilterRewrite(filter)
      ) {
        rewritten = rewriteFilterDirect(
            filter,
            joinFilterPreAnalysis,
            pushDownVirtualColumnsForLhsExprs
        );
      } else if (filter instanceof SelectorFilter) {
        retainRhs = true;
        // We could optimize retainRhs handling further by introducing a "filter to retain" property to the
        // analysis, and only keeping the subfilters that need to be retained
        rewritten = rewriteSelectorFilter(
            (SelectorFilter) filter,
            joinFilterPreAnalysis,
            pushDownVirtualColumnsForLhsExprs
        );
      }

      if (rewritten == null || !rewritten.isCanPushDown()) {
        return JoinFilterAnalysis.createNoPushdownFilterAnalysis(orFilter);
      } else {
        //noinspection OptionalGetWithoutIsPresent isCanPushDown checks isPresent
        newFilters.add(rewritten.getPushDownFilter().get());
      }
    }

    return new JoinFilterAnalysis(
        retainRhs,
        orFilter,
        Filters.maybeOr(newFilters).orElse(null)
    );
  }

  /**
   * Rewrites a selector filter on a join table into an IN filter on the base table.
   *
   * @param selectorFilter                    SelectorFilter to be rewritten
   * @param joinFilterPreAnalysis             The pre-analysis computed by {@link #computeJoinFilterPreAnalysis)}
   * @param pushDownVirtualColumnsForLhsExprs See comments on {@link #analyzeJoinFilterClause}
   *
   * @return A JoinFilterAnalysis that indicates how to handle the potentially rewritten filter
   */
  private static JoinFilterAnalysis rewriteSelectorFilter(
      SelectorFilter selectorFilter,
      JoinFilterPreAnalysis joinFilterPreAnalysis,
      Map<Expr, VirtualColumn> pushDownVirtualColumnsForLhsExprs
  )
  {
    List<Filter> newFilters = new ArrayList<>();

    String filteringColumn = selectorFilter.getDimension();
    String filteringValue = selectorFilter.getValue();

    if (areSomeColumnsFromPostJoinVirtualColumns(
        joinFilterPreAnalysis.getPostJoinVirtualColumns(),
        selectorFilter.getRequiredColumns()
    )) {
      return JoinFilterAnalysis.createNoPushdownFilterAnalysis(selectorFilter);
    }

    if (!joinFilterPreAnalysis.getJoinableClauses().areSomeColumnsFromJoin(selectorFilter.getRequiredColumns())) {
      return new JoinFilterAnalysis(
          false,
          selectorFilter,
          selectorFilter
      );
    }

    List<JoinFilterColumnCorrelationAnalysis> correlationAnalyses = joinFilterPreAnalysis.getCorrelationsByFilteringColumn()
                                                                                         .get(filteringColumn);

    if (correlationAnalyses == null) {
      return JoinFilterAnalysis.createNoPushdownFilterAnalysis(selectorFilter);
    }

    for (JoinFilterColumnCorrelationAnalysis correlationAnalysis : correlationAnalyses) {
      if (correlationAnalysis.supportsPushDown()) {
        Optional<Set<String>> correlatedValues = correlationAnalysis.getCorrelatedValuesMap().get(
            Pair.of(filteringColumn, filteringValue)
        );

        if (!correlatedValues.isPresent()) {
          return JoinFilterAnalysis.createNoPushdownFilterAnalysis(selectorFilter);
        }

        Set<String> newFilterValues = correlatedValues.get();
        // in nothing => match nothing
        if (newFilterValues.isEmpty()) {
          return new JoinFilterAnalysis(
              true,
              selectorFilter,
              FalseFilter.instance()
          );
        }

        for (String correlatedBaseColumn : correlationAnalysis.getBaseColumns()) {
          Filter rewrittenFilter = new InDimFilter(
              correlatedBaseColumn,
              newFilterValues
          ).toFilter();
          newFilters.add(rewrittenFilter);
        }

        for (Expr correlatedBaseExpr : correlationAnalysis.getBaseExpressions()) {
          // We need to create a virtual column for the expressions when pushing down
          VirtualColumn pushDownVirtualColumn = pushDownVirtualColumnsForLhsExprs.computeIfAbsent(
              correlatedBaseExpr,
              (expr) -> {
                String vcName = getCorrelatedBaseExprVirtualColumnName(pushDownVirtualColumnsForLhsExprs.size());
                return new ExpressionVirtualColumn(
                    vcName,
                    correlatedBaseExpr,
                    ValueType.STRING
                );
              }
          );

          Filter rewrittenFilter = new InDimFilter(
              pushDownVirtualColumn.getOutputName(),
              newFilterValues
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
        Filters.maybeAnd(newFilters).orElse(null)
    );
  }

  private static String getCorrelatedBaseExprVirtualColumnName(int counter)
  {
    // May want to have this check other column names to absolutely prevent name conflicts
    return PUSH_DOWN_VIRTUAL_COLUMN_NAME_BASE + counter;
  }

  private static boolean isColumnFromPostJoinVirtualColumns(
      List<VirtualColumn> postJoinVirtualColumns,
      String column
  )
  {
    for (VirtualColumn postJoinVirtualColumn : postJoinVirtualColumns) {
      if (column.equals(postJoinVirtualColumn.getOutputName())) {
        return true;
      }
    }
    return false;
  }

  private static boolean areSomeColumnsFromPostJoinVirtualColumns(
      List<VirtualColumn> postJoinVirtualColumns,
      Collection<String> columns
  )
  {
    for (String column : columns) {
      if (isColumnFromPostJoinVirtualColumns(postJoinVirtualColumns, column)) {
        return true;
      }
    }
    return false;
  }
}
