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

import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.filter.AndFilter;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.filter.InFilter;
import org.apache.druid.segment.filter.OrFilter;
import org.apache.druid.segment.filter.SelectorFilter;
import org.apache.druid.segment.join.lookup.LookupColumnSelectorFactory;
import org.apache.druid.segment.join.lookup.LookupJoinable;
import org.apache.druid.segment.join.table.IndexedTable;
import org.apache.druid.segment.join.table.IndexedTableJoinable;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class JoinFilterAnalyzer
{
  private static final String PUSH_DOWN_VIRTUAL_COLUMN_NAME_BASE = "JOIN-FILTER-PUSHDOWN-VIRTUAL-COLUMN-";

  public static JoinFilterSplit splitFilter(
      Filter originalFilter,
      HashJoinSegmentStorageAdapter baseAdapter,
      List<JoinableClause> clauses
  )
  {
    Filter normalizedFilter = Filters.convertToCNF(originalFilter);

    // build the prefix and equicondition maps
    Map<String, Expr> equiconditions = new HashMap<>();
    Map<String, JoinableClause> prefixes = new HashMap<>();
    for (JoinableClause clause : clauses) {
      prefixes.put(clause.getPrefix(), clause);
      for (Equality equality : clause.getCondition().getEquiConditions()) {
        equiconditions.put(clause.getPrefix() + equality.getRightColumn(), equality.getLeftExpr());
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
      normalizedOrClauses = new ArrayList<>();
      normalizedOrClauses.add(normalizedFilter);
    }

    // Pushdown filters, rewriting if necessary
    List<Filter> leftFilters = new ArrayList<>();
    List<Filter> rightFilters = new ArrayList<>();
    List<VirtualColumn> pushDownVirtualColumns = new ArrayList<>();
    Map<String, List<JoinFilterColumnCorrelationAnalysis>> correlationCache = new HashMap<>();

    for (Filter orClause : normalizedOrClauses) {
      JoinFilterAnalysis joinFilterAnalysis = analyzeJoinFilterClause(
          baseAdapter,
          orClause,
          prefixes,
          equiconditions,
          correlationCache
      );
      if (joinFilterAnalysis.isCanPushDown()) {
        leftFilters.add(joinFilterAnalysis.getPushdownFilter());
        if (joinFilterAnalysis.getPushdownVirtualColumns() != null) {
          pushDownVirtualColumns.addAll(joinFilterAnalysis.getPushdownVirtualColumns());
        }
      }
      if (joinFilterAnalysis.isRetainAfterJoin()) {
        rightFilters.add(joinFilterAnalysis.getOriginalFilter());
      }
    }

    return new JoinFilterSplit(
        leftFilters.isEmpty() ? null : leftFilters.size() == 1 ? leftFilters.get(0) : new AndFilter(leftFilters),
        rightFilters.isEmpty() ? null : rightFilters.size() == 1 ? rightFilters.get(0) : new AndFilter(rightFilters),
        pushDownVirtualColumns
    );
  }

  /**
   * Holds the result of splitting a filter into:
   * - a portion that can be pushed down to the base table
   * - a portion that will be applied post-join
   * - additional virtual columns that need to be created on the base table to support the pushed down filters.
   */
  public static class JoinFilterSplit
  {
    final Filter baseTableFilter;
    final Filter joinTableFilter;
    final List<VirtualColumn> pushDownVirtualColumns;

    public JoinFilterSplit(
        Filter baseTableFilter,
        @Nullable Filter joinTableFilter,
        List<VirtualColumn> pushDownVirtualColumns
    )
    {
      this.baseTableFilter = baseTableFilter;
      this.joinTableFilter = joinTableFilter;
      this.pushDownVirtualColumns = pushDownVirtualColumns;
    }

    public Filter getBaseTableFilter()
    {
      return baseTableFilter;
    }

    public Filter getJoinTableFilter()
    {
      return joinTableFilter;
    }

    public List<VirtualColumn> getPushDownVirtualColumns()
    {
      return pushDownVirtualColumns;
    }

    @Override
    public String toString()
    {
      return "JoinFilterSplit{" +
             "baseTableFilter=" + baseTableFilter +
             ", joinTableFilter=" + joinTableFilter +
             ", pushDownVirtualColumns=" + pushDownVirtualColumns +
             '}';
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      JoinFilterSplit that = (JoinFilterSplit) o;
      return Objects.equals(getBaseTableFilter(), that.getBaseTableFilter()) &&
             Objects.equals(getJoinTableFilter(), that.getJoinTableFilter()) &&
             Objects.equals(getPushDownVirtualColumns(), that.getPushDownVirtualColumns());
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(getBaseTableFilter(), getJoinTableFilter(), getPushDownVirtualColumns());
    }
  }

  /**
   * Analyze a single filter clause from a filter that is in conjunctive normal form (AND of ORs),
   * returning a JoinFilterAnalysis that contains a possible filter rewrite and information on how to handle the filter.
   *
   * @param adapter          Adapter for the join
   * @param filterClause     Individual filter clause from a filter that is in CNF
   * @param prefixes         Map of table prefixes
   * @param equiconditions   Equicondition map
   * @param correlationCache Cache of column correlation analyses
   *
   * @return a JoinFilterAnalysis that contains a possible filter rewrite and information on how to handle the filter.
   */
  private static JoinFilterAnalysis analyzeJoinFilterClause(
      HashJoinSegmentStorageAdapter adapter,
      Filter filterClause,
      Map<String, JoinableClause> prefixes,
      Map<String, Expr> equiconditions,
      Map<String, List<JoinFilterColumnCorrelationAnalysis>> correlationCache
  )
  {
    // NULL matching conditions are not currently pushed down
    if (filterMatchesNull(filterClause)) {
      return JoinFilterAnalysis.createNoPushdownFilterAnalysis(filterClause);
    }

    // Currently we only support rewrites of selector filters and selector filters within OR filters.
    if (filterClause instanceof SelectorFilter) {
      return rewriteSelectorFilter(
          adapter,
          filterClause,
          prefixes,
          equiconditions,
          correlationCache
      );
    }

    if (filterClause instanceof OrFilter) {
      return rewriteOrFilter(
          adapter,
          (OrFilter) filterClause,
          prefixes,
          equiconditions,
          correlationCache
      );
    }

    for (String requiredColumn : filterClause.getRequiredColumns()) {
      if (!adapter.isBaseColumn(requiredColumn)) {
        return JoinFilterAnalysis.createNoPushdownFilterAnalysis(filterClause);
      }
    }
    return new JoinFilterAnalysis(
        true,
        false,
        filterClause,
        filterClause,
        null
    );
  }

  /**
   * Potentially rewrite the subfilters of an OR filter so that the whole OR filter can be pushed down to
   * the base table.
   *
   * @param adapter          Adapter for the join
   * @param orFilter         OrFilter to be rewritten
   * @param prefixes         Map of table prefixes to clauses
   * @param equiconditions   Map of equiconditions
   * @param correlationCache Column correlation analysis cache
   *
   * @return A JoinFilterAnalysis indicating how to handle the potentially rewritten filter
   */
  @Nullable
  private static JoinFilterAnalysis rewriteOrFilter(
      HashJoinSegmentStorageAdapter adapter,
      OrFilter orFilter,
      Map<String, JoinableClause> prefixes,
      Map<String, Expr> equiconditions,
      Map<String, List<JoinFilterColumnCorrelationAnalysis>> correlationCache
  )
  {
    boolean retainRhs = false;

    List<Filter> newFilters = new ArrayList<>();
    for (Filter filter : orFilter.getFilters()) {
      boolean allBaseColumns = true;
      for (String requiredColumn : filter.getRequiredColumns()) {
        if (!adapter.isBaseColumn(requiredColumn)) {
          allBaseColumns = false;
        }
      }

      if (!allBaseColumns) {
        retainRhs = true;
        if (filter instanceof SelectorFilter) {
          JoinFilterAnalysis rewritten = rewriteSelectorFilter(
              adapter,
              filter,
              prefixes,
              equiconditions,
              correlationCache
          );
          if (!rewritten.isCanPushDown()) {
            return JoinFilterAnalysis.createNoPushdownFilterAnalysis(orFilter);
          } else {
            newFilters.add(rewritten.getPushdownFilter());
          }
        } else {
          return JoinFilterAnalysis.createNoPushdownFilterAnalysis(orFilter);
        }
      } else {
        newFilters.add(filter);
      }
    }

    return new JoinFilterAnalysis(
        true,
        retainRhs,
        orFilter,
        new OrFilter(newFilters),
        null
    );
  }

  /**
   * Rewrites a selector filter on a join table into an IN filter on the base table.
   *
   * @param baseAdapter      The adapter for the join
   * @param filter           Filter to be rewritten
   * @param prefixes         Map of join table prefixes to clauses
   * @param equiconditions   Map of equiconditions
   * @param correlationCache Cache of column correlation analyses
   *
   * @return A JoinFilterAnalysis that indicates how to handle the potentially rewritten filter
   */
  private static JoinFilterAnalysis rewriteSelectorFilter(
      HashJoinSegmentStorageAdapter baseAdapter,
      Filter filter,
      Map<String, JoinableClause> prefixes,
      Map<String, Expr> equiconditions,
      Map<String, List<JoinFilterColumnCorrelationAnalysis>> correlationCache
  )
  {
    assert (filter instanceof SelectorFilter);
    SelectorFilter selectorFilter = (SelectorFilter) filter;

    String filteringColumn = selectorFilter.getDimension();
    for (Map.Entry<String, JoinableClause> prefixAndClause : prefixes.entrySet()) {
      if (filteringColumn.startsWith(prefixAndClause.getKey())) {
        List<JoinFilterColumnCorrelationAnalysis> correlations = correlationCache.computeIfAbsent(
            prefixAndClause.getKey(),
            p -> findCorrelatedBaseTableColumns(
                baseAdapter,
                p,
                prefixes.get(p),
                equiconditions
            )
        );

        if (correlations == null) {
          return JoinFilterAnalysis.createNoPushdownFilterAnalysis(filter);
        }

        List<Filter> newFilters = new ArrayList<>();
        List<VirtualColumn> pushdownVirtualColumns = new ArrayList<>();

        for (JoinFilterColumnCorrelationAnalysis correlationAnalysis : correlations) {
          if (correlationAnalysis.supportsPushDown()) {
            List<String> correlatedValues = getCorrelatedValuesForPushDown(
                selectorFilter.getDimension(),
                selectorFilter.getValue(),
                correlationAnalysis.getJoinColumn(),
                prefixAndClause.getValue()
            );

            if (correlatedValues == null) {
              return JoinFilterAnalysis.createNoPushdownFilterAnalysis(selectorFilter);
            }

            for (String correlatedBaseColumn : correlationAnalysis.getBaseColumns()) {
              InFilter rewrittenFilter = (InFilter) new InDimFilter(
                  correlatedBaseColumn,
                  correlatedValues,
                  null,
                  null
              ).toFilter();
              newFilters.add(rewrittenFilter);
            }

            for (Expr correlatedBaseExpr : correlationAnalysis.getBaseExpressions()) {
              // need to create a virtual column for the expressions when pushing down
              String vcName = getCorrelatedBaseExprVirtualColumnName(pushdownVirtualColumns.size());

              VirtualColumn correlatedBaseExprVirtualColumn = new ExpressionVirtualColumn(
                  vcName,
                  correlatedBaseExpr,
                  ValueType.STRING
              );
              pushdownVirtualColumns.add(correlatedBaseExprVirtualColumn);

              InFilter rewrittenFilter = (InFilter) new InDimFilter(
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
          return JoinFilterAnalysis.createNoPushdownFilterAnalysis(filter);
        }

        return new JoinFilterAnalysis(
            true,
            true,
            filter,
            newFilters.size() == 1 ? newFilters.get(0) : new AndFilter(newFilters),
            pushdownVirtualColumns
        );
      }
    }
    return new JoinFilterAnalysis(
        true,
        false,
        filter,
        filter,
        null
    );
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
   * Returns null if we cannot determine the correlated values.
   */
  @Nullable
  private static List<String> getCorrelatedValuesForPushDown(
      String filterColumn,
      String filterValue,
      String correlatedJoinColumn,
      JoinableClause clauseForFilteredTable
  )
  {
    String filterColumnNoPrefix = filterColumn.substring(clauseForFilteredTable.getPrefix().length());
    String correlatedColumnNoPrefix = correlatedJoinColumn.substring(clauseForFilteredTable.getPrefix().length());

    // would be good to allow non-key column indices on the Joinables for better perf
    if (clauseForFilteredTable.getJoinable() instanceof LookupJoinable) {
      LookupJoinable lookupJoinable = (LookupJoinable) clauseForFilteredTable.getJoinable();
      List<String> correlatedValues;
      if (LookupColumnSelectorFactory.KEY_COLUMN.equals(filterColumnNoPrefix)) {
        if (LookupColumnSelectorFactory.KEY_COLUMN.equals(correlatedColumnNoPrefix)) {
          correlatedValues = ImmutableList.of(filterValue);
        } else {
          correlatedValues = ImmutableList.of(lookupJoinable.getExtractor().apply(filterColumnNoPrefix));
        }
      } else {
        if (LookupColumnSelectorFactory.VALUE_COLUMN.equals(correlatedColumnNoPrefix)) {
          correlatedValues = ImmutableList.of(filterValue);
        } else {
          correlatedValues = lookupJoinable.getExtractor().unapply(filterValue);
        }
      }
      return correlatedValues;
    }

    if (clauseForFilteredTable.getJoinable() instanceof IndexedTableJoinable) {
      IndexedTableJoinable indexedTableJoinable = (IndexedTableJoinable) clauseForFilteredTable.getJoinable();
      IndexedTable indexedTable = indexedTableJoinable.getTable();

      int filterColumnPosition = indexedTable.allColumns().indexOf(filterColumnNoPrefix);
      int correlatedColumnPosition = indexedTable.allColumns().indexOf(correlatedColumnNoPrefix);

      if (filterColumnPosition < 0 || correlatedColumnPosition < 0) {
        return null;
      }

      if (indexedTable.keyColumns().contains(filterColumnNoPrefix)) {
        IndexedTable.Index index = indexedTable.columnIndex(filterColumnPosition);
        IndexedTable.Reader reader = indexedTable.columnReader(correlatedColumnPosition);
        IntList rowIndex = index.find(filterValue);
        List<String> correlatedValues = new ArrayList<>();
        for (int i = 0; i < rowIndex.size(); i++) {
          int rowNum = rowIndex.getInt(i);
          correlatedValues.add(reader.read(rowNum).toString());
        }
        return correlatedValues;
      } else {
        IndexedTable.Reader dimNameReader = indexedTable.columnReader(filterColumnPosition);
        IndexedTable.Reader correlatedColumnReader = indexedTable.columnReader(correlatedColumnPosition);
        Set<String> correlatedValueSet = new HashSet<>();
        for (int i = 0; i < indexedTable.numRows(); i++) {
          if (filterValue.equals(dimNameReader.read(i).toString())) {
            correlatedValueSet.add(correlatedColumnReader.read(i).toString());
          }
        }

        return new ArrayList<>(correlatedValueSet);
      }
    }

    return null;
  }

  /**
   * For all RHS columns that appear in the join's equiconditions, correlate them with base table columns if possible.
   *
   * @param adapter              The adapter for the join. Used to determine if a column is a base table column.
   * @param tablePrefix          Prefix for a join table
   * @param clauseForTablePrefix Joinable clause for the prefix
   * @param equiconditions       Map of equiconditions, keyed by the right hand columns
   *
   * @return A list of correlatation analyses for the equicondition RHS columns that reside in the table associated with
   * the tablePrefix
   */
  @Nullable
  private static List<JoinFilterColumnCorrelationAnalysis> findCorrelatedBaseTableColumns(
      HashJoinSegmentStorageAdapter adapter,
      String tablePrefix,
      JoinableClause clauseForTablePrefix,
      Map<String, Expr> equiconditions
  )
  {
    JoinConditionAnalysis jca = clauseForTablePrefix.getCondition();

    List<String> rhsColumns = new ArrayList<>();
    for (Equality eq : jca.getEquiConditions()) {
      rhsColumns.add(tablePrefix + eq.getRightColumn());
    }

    List<JoinFilterColumnCorrelationAnalysis> correlations = new ArrayList<>();

    for (String rhsColumn : rhsColumns) {
      List<String> correlatedBaseColumns = new ArrayList<>();
      List<Expr> correlatedBaseExpressions = new ArrayList<>();
      boolean terminate = false;

      String findMappingFor = rhsColumn;
      while (!terminate) {
        Expr lhs = equiconditions.get(findMappingFor);
        if (lhs == null) {
          break;
        }
        String identifier = lhs.getBindingIfIdentifier();
        if (identifier == null) {
          // We push down if the function only requires base table columns
          Expr.BindingDetails bindingDetails = lhs.analyzeInputs();
          Set<String> requiredBindings = bindingDetails.getRequiredBindings();
          for (String requiredBinding : requiredBindings) {
            if (!adapter.isBaseColumn(requiredBinding)) {
              return null;
            }
          }

          terminate = true;
          correlatedBaseExpressions.add(lhs);
        } else {
          // simple identifier, see if we can correlate it with a column on the base table
          findMappingFor = identifier;
          if (adapter.isBaseColumn(identifier)) {
            terminate = true;
            correlatedBaseColumns.add(findMappingFor);
          }
        }
      }

      if (correlatedBaseColumns.isEmpty() && correlatedBaseExpressions.isEmpty()) {
        return null;
      }

      correlations.add(
          new JoinFilterColumnCorrelationAnalysis(
              rhsColumn,
              correlatedBaseColumns,
              correlatedBaseExpressions
          )
      );
    }

    return correlations;
  }

  private static boolean filterMatchesNull(Filter filter)
  {
    ValueMatcher valueMatcher = filter.makeMatcher(new AllNullColumnSelectorFactory());
    return valueMatcher.matches();
  }

  private static class AllNullColumnSelectorFactory implements ColumnSelectorFactory
  {
    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      return DimensionSelector.constant(null);
    }

    @Override
    public ColumnValueSelector<?> makeColumnValueSelector(String columnName)
    {
      return NilColumnValueSelector.instance();
    }

    @Override
    public ColumnCapabilities getColumnCapabilities(String columnName)
    {
      return null;
    }
  }

  /**
   * Holds information about:
   * - whether a filter can be pushed down
   * - if it needs to be retained after the join,
   * - a reference to the original filter
   * - a potentially rewritten filter to be pushed down to the base table
   * - a list of virtual columns that need to be created on the base table to support the pushed down filter
   */
  private static class JoinFilterAnalysis
  {
    private final boolean canPushDown;
    private final boolean retainAfterJoin;
    private final Filter originalFilter;
    private final Filter pushdownFilter;
    private final List<VirtualColumn> pushdownVirtualColumns;

    public JoinFilterAnalysis(
        boolean canPushDown,
        boolean retainAfterJoin,
        Filter originalFilter,
        @Nullable Filter pushdownFilter,
        @Nullable List<VirtualColumn> pushdownVirtualColumns
    )
    {
      this.canPushDown = canPushDown;
      this.retainAfterJoin = retainAfterJoin;
      this.originalFilter = originalFilter;
      this.pushdownFilter = pushdownFilter;
      this.pushdownVirtualColumns = pushdownVirtualColumns;
    }

    public boolean isCanPushDown()
    {
      return canPushDown;
    }

    public boolean isRetainAfterJoin()
    {
      return retainAfterJoin;
    }

    public Filter getOriginalFilter()
    {
      return originalFilter;
    }

    @Nullable
    public Filter getPushdownFilter()
    {
      return pushdownFilter;
    }

    @Nullable
    public List<VirtualColumn> getPushdownVirtualColumns()
    {
      return pushdownVirtualColumns;
    }

    /**
     * Utility method for generating an analysis that represents: "Filter cannot be pushed down"
     *
     * @param originalFilter The original filter which cannot be pushed down
     *
     * @return analysis that represents: "Filter cannot be pushed down"
     */
    public static JoinFilterAnalysis createNoPushdownFilterAnalysis(Filter originalFilter)
    {
      return new JoinFilterAnalysis(
          false,
          true,
          originalFilter,
          null,
          null
      );
    }
  }

  /**
   * Represents an analysis of what base table columns, if any, can be correlated with a column that will
   * be filtered on.
   * <p>
   * For example, if we're joining on a base table via the equiconditions (id = j.id AND f(id2) = j.id2),
   * then we can correlate j.id with id (base table column) and j.id2 with f(id2) (a base table expression).
   */
  private static class JoinFilterColumnCorrelationAnalysis
  {
    private final String joinColumn;
    private final List<String> baseColumns;
    private final List<Expr> baseExpressions;

    public JoinFilterColumnCorrelationAnalysis(
        String joinColumn,
        List<String> baseColumns,
        List<Expr> baseExpressions
    )
    {
      this.joinColumn = joinColumn;
      this.baseColumns = baseColumns;
      this.baseExpressions = baseExpressions;
    }

    public String getJoinColumn()
    {
      return joinColumn;
    }

    public List<String> getBaseColumns()
    {
      return baseColumns;
    }

    public List<Expr> getBaseExpressions()
    {
      return baseExpressions;
    }

    public boolean supportsPushDown()
    {
      return !baseColumns.isEmpty() || !baseExpressions.isEmpty();
    }
  }

}
