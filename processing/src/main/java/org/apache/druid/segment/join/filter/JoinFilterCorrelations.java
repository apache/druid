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

import org.apache.druid.java.util.common.Pair;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.join.Equality;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.segment.join.JoinableClause;
import org.apache.druid.segment.join.filter.rewrite.RhsRewriteCandidate;
import org.apache.druid.segment.join.filter.rewrite.RhsRewriteCandidates;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * A wrapper class for correlation analyses of different filters involved in the query. It contains:
 *
 * - A mapping of RHS filtering columns -> List<JoinFilterColumnCorrelationAnalysis>, used for filter rewrites
 * - A second mapping of RHS filtering columns -> List<JoinFilterColumnCorrelationAnalysis>, used for direct filter rewrites
 */
public class JoinFilterCorrelations
{
  private final Map<String, List<JoinFilterColumnCorrelationAnalysis>> correlationsByFilteringColumn;
  private final Map<String, List<JoinFilterColumnCorrelationAnalysis>> correlationsByDirectFilteringColumn;

  private JoinFilterCorrelations(
      Map<String, List<JoinFilterColumnCorrelationAnalysis>> correlationsByFilteringColumn,
      Map<String, List<JoinFilterColumnCorrelationAnalysis>> correlationsByDirectFilteringColumn
  )
  {
    this.correlationsByFilteringColumn = correlationsByFilteringColumn;
    this.correlationsByDirectFilteringColumn = correlationsByDirectFilteringColumn;
  }

  public Map<String, List<JoinFilterColumnCorrelationAnalysis>> getCorrelationsByFilteringColumn()
  {
    return correlationsByFilteringColumn;
  }

  public Map<String, List<JoinFilterColumnCorrelationAnalysis>> getCorrelationsByDirectFilteringColumn()
  {
    return correlationsByDirectFilteringColumn;
  }

  public static JoinFilterCorrelations computeJoinFilterCorrelations(
      List<Filter> normalizedJoinTableClauses,
      Equiconditions equiconditions,
      JoinableClauses joinableClauses,
      boolean enableRewriteValueColumnFilters,
      long filterRewriteMaxSize
  )
  {
    RhsRewriteCandidates rhsRewriteCandidates =
        RhsRewriteCandidates.getRhsRewriteCandidates(normalizedJoinTableClauses, equiconditions, joinableClauses);

    // Build a map of RHS table prefix -> JoinFilterColumnCorrelationAnalysis based on the RHS rewrite candidates
    Map<String, Optional<Map<String, JoinFilterColumnCorrelationAnalysis>>> correlationsByPrefix = new HashMap<>();
    Map<String, Optional<JoinFilterColumnCorrelationAnalysis>> directRewriteCorrelations = new HashMap<>();
    for (RhsRewriteCandidate rhsRewriteCandidate : rhsRewriteCandidates.getRhsRewriteCandidates()) {
      if (rhsRewriteCandidate.isDirectRewrite()) {
        directRewriteCorrelations.computeIfAbsent(
            rhsRewriteCandidate.getRhsColumn(),
            c -> {
              Optional<Map<String, JoinFilterColumnCorrelationAnalysis>> correlatedBaseTableColumns =
                  findCorrelatedBaseTableColumns(
                      joinableClauses,
                      c,
                      rhsRewriteCandidate,
                      equiconditions
                  );
              if (!correlatedBaseTableColumns.isPresent()) {
                return Optional.empty();
              } else {
                JoinFilterColumnCorrelationAnalysis baseColumnAnalysis = correlatedBaseTableColumns.get().get(c);
                // for direct rewrites, there will only be one analysis keyed by the RHS column
                assert (baseColumnAnalysis != null);
                return Optional.of(correlatedBaseTableColumns.get().get(c));
              }
            }
        );
      } else {
        correlationsByPrefix.computeIfAbsent(
            rhsRewriteCandidate.getJoinableClause().getPrefix(),
            p -> findCorrelatedBaseTableColumns(
                joinableClauses,
                p,
                rhsRewriteCandidate,
                equiconditions
            )
        );
      }
    }

    // Using the RHS table prefix -> JoinFilterColumnCorrelationAnalysis created in the previous step,
    // build a map of rhsFilterColumn -> Pair(rhsFilterColumn, rhsFilterValue) -> correlatedValues for specific filter pair
    // The Pair(rhsFilterColumn, rhsFilterValue) -> correlatedValues mappings are stored in the
    // JoinFilterColumnCorrelationAnalysis objects, which are shared across all rhsFilterColumn entries that belong
    // to the same RHS table.
    //
    // The value is a List<JoinFilterColumnCorrelationAnalysis> instead of a single value because a table can be joined
    // to another via multiple columns.
    // (See JoinFilterAnalyzerTest.test_filterPushDown_factToRegionOneColumnToTwoRHSColumnsAndFilterOnRHS for an example)
    Map<String, List<JoinFilterColumnCorrelationAnalysis>> correlationsByFilteringColumn = new LinkedHashMap<>();
    Map<String, List<JoinFilterColumnCorrelationAnalysis>> correlationsByDirectFilteringColumn = new LinkedHashMap<>();
    for (RhsRewriteCandidate rhsRewriteCandidate : rhsRewriteCandidates.getRhsRewriteCandidates()) {
      if (rhsRewriteCandidate.isDirectRewrite()) {
        List<JoinFilterColumnCorrelationAnalysis> perColumnCorrelations =
            correlationsByDirectFilteringColumn.computeIfAbsent(
                rhsRewriteCandidate.getRhsColumn(),
                (rhsCol) -> new ArrayList<>()
            );
        perColumnCorrelations.add(
            directRewriteCorrelations.get(rhsRewriteCandidate.getRhsColumn()).get()
        );
        continue;
      }

      Optional<Map<String, JoinFilterColumnCorrelationAnalysis>> correlationsForPrefix = correlationsByPrefix.get(
          rhsRewriteCandidate.getJoinableClause().getPrefix()
      );
      if (correlationsForPrefix.isPresent()) {
        for (Map.Entry<String, JoinFilterColumnCorrelationAnalysis> correlationForPrefix : correlationsForPrefix.get()
                                                                                                                .entrySet()) {
          List<JoinFilterColumnCorrelationAnalysis> perColumnCorrelations =
              correlationsByFilteringColumn.computeIfAbsent(
                  rhsRewriteCandidate.getRhsColumn(),
                  (rhsCol) -> new ArrayList<>()
              );
          perColumnCorrelations.add(correlationForPrefix.getValue());
          correlationForPrefix.getValue().getCorrelatedValuesMap().computeIfAbsent(
              Pair.of(rhsRewriteCandidate.getRhsColumn(), rhsRewriteCandidate.getValueForRewrite()),
              (rhsVal) -> {
                Optional<Set<String>> correlatedValues = getCorrelatedValuesForPushDown(
                    rhsRewriteCandidate.getRhsColumn(),
                    rhsRewriteCandidate.getValueForRewrite(),
                    correlationForPrefix.getValue().getJoinColumn(),
                    rhsRewriteCandidate.getJoinableClause(),
                    enableRewriteValueColumnFilters,
                    filterRewriteMaxSize
                );
                return correlatedValues;
              }
          );
        }
      } else {
        correlationsByFilteringColumn.put(rhsRewriteCandidate.getRhsColumn(), null);
      }
    }

    // Go through each per-column analysis list and prune duplicates
    for (Map.Entry<String, List<JoinFilterColumnCorrelationAnalysis>> correlation : correlationsByFilteringColumn
        .entrySet()) {
      if (correlation.getValue() != null) {
        List<JoinFilterColumnCorrelationAnalysis> dedupList = eliminateCorrelationDuplicates(
            correlation.getValue()
        );
        correlationsByFilteringColumn.put(correlation.getKey(), dedupList);
      }
    }
    for (Map.Entry<String, List<JoinFilterColumnCorrelationAnalysis>> correlation : correlationsByDirectFilteringColumn
        .entrySet()) {
      if (correlation.getValue() != null) {
        List<JoinFilterColumnCorrelationAnalysis> dedupList = eliminateCorrelationDuplicates(
            correlation.getValue()
        );
        correlationsByDirectFilteringColumn.put(correlation.getKey(), dedupList);
      }
    }
    return new JoinFilterCorrelations(correlationsByFilteringColumn, correlationsByDirectFilteringColumn);
  }


  /**
   * Given a list of JoinFilterColumnCorrelationAnalysis, prune the list so that we only have one
   * JoinFilterColumnCorrelationAnalysis for each unique combination of base columns.
   * <p>
   * Suppose we have a join condition like the following, where A is the base table:
   * A.joinColumn == B.joinColumn && A.joinColumn == B.joinColumn2
   * <p>
   * We only need to consider one correlation to A.joinColumn since B.joinColumn and B.joinColumn2 must
   * have the same value in any row that matches the join condition.
   * <p>
   * In the future this method could consider which column correlation should be preserved based on availability of
   * indices and other heuristics.
   * <p>
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
    Map<Set<String>, JoinFilterColumnCorrelationAnalysis> uniquesMap = new HashMap<>();

    for (JoinFilterColumnCorrelationAnalysis jca : originalList) {
      Set<String> mapKey = new HashSet<>(jca.getBaseColumns());
      for (Expr expr : jca.getBaseExpressions()) {
        mapKey.add(expr.stringify());
      }

      uniquesMap.put(mapKey, jca);
    }

    return new ArrayList<>(uniquesMap.values());
  }


  /**
   * Helper method for rewriting filters on join table columns into filters on base table columns.
   *
   * @param filterColumn           A join table column that we're filtering on
   * @param filterValue            The value to filter on
   * @param correlatedJoinColumn   A join table column that appears as the RHS of an equicondition, which we can correlate
   *                               with a column on the base table
   * @param clauseForFilteredTable The joinable clause that corresponds to the join table being filtered on
   * @return A list of values of the correlatedJoinColumn that appear in rows where filterColumn = filterValue
   * Returns absent if we cannot determine the correlated values.
   */
  private static Optional<Set<String>> getCorrelatedValuesForPushDown(
      String filterColumn,
      String filterValue,
      String correlatedJoinColumn,
      JoinableClause clauseForFilteredTable,
      boolean enableRewriteValueColumnFilters,
      long filterRewriteMaxSize
  )
  {
    String filterColumnNoPrefix = filterColumn.substring(clauseForFilteredTable.getPrefix().length());
    String correlatedColumnNoPrefix = correlatedJoinColumn.substring(clauseForFilteredTable.getPrefix().length());

    return clauseForFilteredTable.getJoinable().getCorrelatedColumnValues(
        filterColumnNoPrefix,
        filterValue,
        correlatedColumnNoPrefix,
        filterRewriteMaxSize,
        enableRewriteValueColumnFilters
    );
  }

  /**
   * For each rhs column that appears in the equiconditions for a table's JoinableClause,
   * we try to determine what base table columns are related to the rhs column through the total set of equiconditions.
   * We do this by searching backwards through the chain of join equiconditions using the provided equicondition map.
   * <p>
   * For example, suppose we have 3 tables, A,B,C, joined with the following conditions, where A is the base table:
   * A.joinColumn == B.joinColumn
   * B.joinColum == C.joinColumnenableRewriteValueColumnFilters
   * <p>
   * We would determine that C.joinColumn is correlated with A.joinColumn: we first see that
   * C.joinColumn is linked to B.joinColumn which in turn is linked to A.joinColumn
   * <p>
   * Suppose we had the following join conditions instead:
   * f(A.joinColumn) == B.joinColumn
   * B.joinColum == C.joinColumn
   * In this case, the JoinFilterColumnCorrelationAnalysis for C.joinColumn would be linked to f(A.joinColumn).
   * <p>
   * Suppose we had the following join conditions instead:
   * A.joinColumn == B.joinColumn
   * f(B.joinColum) == C.joinColumn
   * <p>
   * Because we cannot reverse the function f() applied to the second table B in all cases,
   * we cannot relate C.joinColumn to A.joinColumn, and we would not generate a correlation for C.joinColumn
   *
   * @param joinableClauses     List of joinable clauses for the query
   * @param tablePrefix         Prefix for a join table
   * @param rhsRewriteCandidate RHS rewrite candidate that we find correlated base table columns for
   * @param equiConditions      Map of equiconditions, keyed by the right hand columns
   * @return A list of correlatation analyses for the equicondition RHS columns that reside in the table associated with
   * the tablePrefix
   */
  private static Optional<Map<String, JoinFilterColumnCorrelationAnalysis>> findCorrelatedBaseTableColumns(
      JoinableClauses joinableClauses,
      String tablePrefix,
      RhsRewriteCandidate rhsRewriteCandidate,
      Equiconditions equiConditions
  )
  {
    JoinableClause clauseForTablePrefix = rhsRewriteCandidate.getJoinableClause();
    JoinConditionAnalysis jca = clauseForTablePrefix.getCondition();

    Set<String> rhsColumns = new HashSet<>();
    if (rhsRewriteCandidate.isDirectRewrite()) {
      // If we filter on a RHS join column, we only need to consider that column from the RHS side
      rhsColumns.add(rhsRewriteCandidate.getRhsColumn());
    } else {
      for (Equality eq : jca.getEquiConditions()) {
        rhsColumns.add(tablePrefix + eq.getRightColumn());
      }
    }

    Map<String, JoinFilterColumnCorrelationAnalysis> correlations = new LinkedHashMap<>();

    for (String rhsColumn : rhsColumns) {
      Set<String> correlatedBaseColumns = new HashSet<>();
      Set<Expr> correlatedBaseExpressions = new HashSet<>();

      getCorrelationForRHSColumn(
          joinableClauses,
          equiConditions,
          rhsColumn,
          correlatedBaseColumns,
          correlatedBaseExpressions
      );

      if (correlatedBaseColumns.isEmpty() && correlatedBaseExpressions.isEmpty()) {
        continue;
      }

      correlations.put(
          rhsColumn,
          new JoinFilterColumnCorrelationAnalysis(
              rhsColumn,
              correlatedBaseColumns,
              correlatedBaseExpressions
          )
      );
    }

    if (correlations.size() == 0) {
      return Optional.empty();
    } else {
      return Optional.of(correlations);
    }
  }

  /**
   * Helper method for {@link #findCorrelatedBaseTableColumns} that determines correlated base table columns
   * and/or expressions for a single RHS column and adds them to the provided sets as it traverses the
   * equicondition column relationships.
   *
   * @param equiConditions            Map of equiconditions, keyed by the right hand columns
   * @param rhsColumn                 RHS column to find base table correlations for
   * @param correlatedBaseColumns     Set of correlated base column names for the provided RHS column. Will be modified.
   * @param correlatedBaseExpressions Set of correlated base column expressions for the provided RHS column. Will be
   *                                  modified.
   */
  private static void getCorrelationForRHSColumn(
      JoinableClauses joinableClauses,
      Equiconditions equiConditions,
      String rhsColumn,
      Set<String> correlatedBaseColumns,
      Set<Expr> correlatedBaseExpressions
  )
  {
    String findMappingFor = rhsColumn;
    Set<Expr> lhsExprs = equiConditions.getLhsExprs(findMappingFor);
    if (lhsExprs == null) {
      return;
    }

    for (Expr lhsExpr : lhsExprs) {
      String identifier = lhsExpr.getBindingIfIdentifier();
      if (identifier == null) {
        // We push down if the function only requires base table columns
        Expr.BindingDetails bindingDetails = lhsExpr.analyzeInputs();
        Set<String> requiredBindings = bindingDetails.getRequiredBindings();

        if (joinableClauses.areSomeColumnsFromJoin(requiredBindings)) {
          break;
        }
        correlatedBaseExpressions.add(lhsExpr);
      } else {
        // simple identifier, see if we can correlate it with a column on the base table
        findMappingFor = identifier;
        if (joinableClauses.getColumnFromJoinIfExists(identifier) == null) {
          correlatedBaseColumns.add(findMappingFor);
        } else {
          getCorrelationForRHSColumn(
              joinableClauses,
              equiConditions,
              findMappingFor,
              correlatedBaseColumns,
              correlatedBaseExpressions
          );
        }
      }
    }
  }
}
