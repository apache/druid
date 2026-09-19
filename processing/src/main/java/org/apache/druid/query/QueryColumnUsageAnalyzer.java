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

package org.apache.druid.query;

import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.planning.JoinDataSourceAnalysis;
import org.apache.druid.query.planning.PreJoinableClause;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.union.UnionQuery;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.join.JoinPrefixUtils;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Analyzes a native {@link Query} to determine, per base table, which columns it references and in
 * what role (projection, filter, group-by, aggregation, join key). Columns are read from the query
 * plan -- not by parsing SQL -- and attributed to base tables across joins (using the planner's join
 * prefixes), unions, sub-queries, and datasource wrappers (restricted/filtered/unnest). Columns with
 * no base table (for example lookups or inline data) are dropped rather than fabricated.
 *
 * <p>This is intended as a single, planner-aligned source of truth for "which columns does this query
 * read, and how" so that lineage emitters, column-statistics/optimizer work, and other consumers do
 * not each re-derive it and drift from the parser/planner.
 *
 * <p>It is complementary to {@link Query#getRequiredColumns()}: that method returns a flat,
 * un-attributed set, may be {@code null}, unconditionally injects {@code __time}, does not expand
 * virtual columns to their base columns, and (by contract) excludes join-condition and pushed-down
 * filter columns. This analyzer attributes per base table, assigns usage roles, expands virtual
 * columns, and includes join-condition / {@code leftFilter} / filtered-aggregator predicate columns.
 */
public final class QueryColumnUsageAnalyzer
{
  private QueryColumnUsageAnalyzer()
  {
  }

  /**
   * The role in which a column is referenced by a query. Names align with OpenLineage transformation
   * subtypes where they exist (GROUP_BY, AGGREGATION, FILTER, JOIN); PROJECTION denotes a
   * selected/projected column.
   */
  public enum ColumnUsage
  {
    PROJECTION,
    GROUP_BY,
    AGGREGATION,
    FILTER,
    JOIN
  }

  /**
   * Returns a map of base-table name to (column name to the set of roles it was used in) for the given
   * query. Returns an empty map when no base-table columns can be determined (unsupported query type,
   * {@code SELECT *} with no filter, or a datasource with no base tables). Never returns {@code null}.
   *
   * <p>Result maps are sorted by table and column, and role sets iterate in enum-declaration order, so
   * the output is deterministic. This method reads the query structure only and performs no I/O. The
   * returned maps and role sets are freshly allocated, mutable, and owned by the caller.
   */
  public static Map<String, Map<String, EnumSet<ColumnUsage>>> analyze(Query<?> query)
  {
    Map<String, Map<String, EnumSet<ColumnUsage>>> result = new TreeMap<>();
    collectInto(result, query);
    return result;
  }

  /**
   * Walks {@code query}'s column-bearing parts and attributes the referenced columns to the base
   * tables of its datasource. {@link UnionQuery} (whose {@code getDataSource()} is undefined) is
   * handled by recursing into each of its branch queries -- {@code getDataSources()} would discard the
   * branches' dimensions/aggregators/filters, so the full branch queries are used instead.
   */
  private static void collectInto(Map<String, Map<String, EnumSet<ColumnUsage>>> result, Query<?> query)
  {
    if (query instanceof UnionQuery) {
      for (Query<?> branch : ((UnionQuery) query).getQueries()) {
        collectInto(result, branch);
      }
      return;
    }
    attribute(result, query.getDataSource(), collectColumnRoles(query));
  }

  /**
   * Attributes {@code roles} (column to roles, expressed in {@code dataSource}'s output namespace) to
   * the underlying base tables, recursing through the datasource tree. Sub-queries are recursed into
   * via {@link #collectInto} (their columns come from their own parts, not the outer references).
   */
  private static void attribute(
      Map<String, Map<String, EnumSet<ColumnUsage>>> result,
      DataSource dataSource,
      Map<String, EnumSet<ColumnUsage>> roles
  )
  {
    if (dataSource instanceof TableDataSource) {
      // Also covers GlobalTableDataSource (a TableDataSource subclass); it is a real named table, so
      // handling it here is intentional. Keep this branch ahead of the wrapper branches below.
      String table = ((TableDataSource) dataSource).getName();
      for (Map.Entry<String, EnumSet<ColumnUsage>> entry : roles.entrySet()) {
        addRoles(result, table, entry.getKey(), entry.getValue());
      }
    } else if (dataSource instanceof RestrictedDataSource) {
      attribute(result, ((RestrictedDataSource) dataSource).getBase(), roles);
    } else if (dataSource instanceof FilteredDataSource) {
      // FilteredDataSource carries a pushed-down filter over its base; its columns are FILTER usages.
      FilteredDataSource filtered = (FilteredDataSource) dataSource;
      Map<String, EnumSet<ColumnUsage>> baseRoles = copyRoles(roles);
      addFilterColumns(baseRoles, null, filtered.getFilter());
      attribute(result, filtered.getBase(), baseRoles);
    } else if (dataSource instanceof UnnestDataSource) {
      UnnestDataSource unnest = (UnnestDataSource) dataSource;
      VirtualColumn unnestColumn = unnest.getVirtualColumn();
      Map<String, EnumSet<ColumnUsage>> baseRoles = copyRoles(roles);
      // The unnest output column is synthetic (not a base column). Move whatever roles it was used in
      // (GROUP_BY / FILTER / PROJECTION) onto the underlying source column(s), and fold in any predicate
      // pushed onto the unnested value (unnestFilter) as FILTER. Never attribute to the synthetic column.
      EnumSet<ColumnUsage> outputRoles = baseRoles.remove(unnestColumn.getOutputName());
      EnumSet<ColumnUsage> sourceRoles =
          outputRoles != null ? EnumSet.copyOf(outputRoles) : EnumSet.noneOf(ColumnUsage.class);
      DimFilter unnestFilter = unnest.getUnnestFilter();
      if (unnestFilter != null) {
        for (String column : unnestFilter.getRequiredColumns()) {
          if (column.equals(unnestColumn.getOutputName())) {
            sourceRoles.add(ColumnUsage.FILTER);
          } else {
            // A predicate on a base pass-through column rather than the unnested value.
            baseRoles.computeIfAbsent(column, k -> EnumSet.noneOf(ColumnUsage.class)).add(ColumnUsage.FILTER);
          }
        }
      }
      if (sourceRoles.isEmpty()) {
        // The unnested value is not otherwise referenced, but unnest still reads the source column.
        sourceRoles.add(ColumnUsage.PROJECTION);
      }
      for (String required : unnestColumn.requiredColumns()) {
        baseRoles.computeIfAbsent(required, k -> EnumSet.noneOf(ColumnUsage.class)).addAll(sourceRoles);
      }
      attribute(result, unnest.getBase(), baseRoles);
    } else if (dataSource instanceof UnionDataSource) {
      // Union members share the same output schema, so the referenced columns apply to each.
      for (DataSource member : dataSource.getChildren()) {
        attribute(result, member, roles);
      }
    } else if (dataSource instanceof QueryDataSource) {
      // The outer references are this sub-query's OUTPUT columns, not base columns; the sub-query's
      // own base-table columns are captured by recursing into its parts.
      collectInto(result, ((QueryDataSource) dataSource).getQuery());
    } else if (dataSource instanceof JoinDataSource) {
      attributeJoin(result, (JoinDataSource) dataSource, roles);
    }
    // LookupDataSource, InlineDataSource and any other shape have no base table: drop (never fabricate).
  }

  /**
   * Splits {@code roles} (plus join-condition and base-table-filter columns) across the base
   * datasource and each joinable clause using the planner's join prefixes, then recurses. Clauses are
   * matched longest-prefix-first; right-side columns arrive already prefixed and are un-prefixed before
   * attribution to the clause's datasource (which may itself be a table, join, or sub-query).
   */
  private static void attributeJoin(
      Map<String, Map<String, EnumSet<ColumnUsage>>> result,
      JoinDataSource join,
      Map<String, EnumSet<ColumnUsage>> roles
  )
  {
    JoinDataSourceAnalysis analysis = JoinDataSourceAnalysis.constructAnalysis(join);
    DataSource base = analysis.getBaseDataSource();
    List<PreJoinableClause> clauses = new ArrayList<>(analysis.getPreJoinableClauses());
    // Longest prefix first so that, e.g., "j0.x" matches clause "j0." rather than a shorter prefix.
    clauses.sort((a, b) -> Integer.compare(b.getPrefix().length(), a.getPrefix().length()));

    Map<String, EnumSet<ColumnUsage>> all = copyRoles(roles);
    for (PreJoinableClause clause : clauses) {
      for (String column : clause.getCondition().getRequiredColumns()) {
        all.computeIfAbsent(column, k -> EnumSet.noneOf(ColumnUsage.class)).add(ColumnUsage.JOIN);
      }
    }
    // A predicate pushed onto the join's base table (leftFilter) also references base-table columns.
    analysis.getJoinBaseTableFilter().ifPresent(baseFilter -> {
      for (String column : baseFilter.getRequiredColumns()) {
        all.computeIfAbsent(column, k -> EnumSet.noneOf(ColumnUsage.class)).add(ColumnUsage.FILTER);
      }
    });

    Map<DataSource, Map<String, EnumSet<ColumnUsage>>> partitioned = new LinkedHashMap<>();
    for (Map.Entry<String, EnumSet<ColumnUsage>> entry : all.entrySet()) {
      String column = entry.getKey();
      DataSource target = base;
      String resolved = column;
      for (PreJoinableClause clause : clauses) {
        if (JoinPrefixUtils.isPrefixedBy(column, clause.getPrefix())) {
          target = clause.getDataSource();
          resolved = JoinPrefixUtils.unprefix(column, clause.getPrefix());
          break;
        }
      }
      partitioned.computeIfAbsent(target, k -> new TreeMap<>())
                 .computeIfAbsent(resolved, k -> EnumSet.noneOf(ColumnUsage.class))
                 .addAll(entry.getValue());
    }
    for (Map.Entry<DataSource, Map<String, EnumSet<ColumnUsage>>> entry : partitioned.entrySet()) {
      attribute(result, entry.getKey(), entry.getValue());
    }
  }

  private static void addRoles(
      Map<String, Map<String, EnumSet<ColumnUsage>>> result,
      String table,
      String column,
      EnumSet<ColumnUsage> roles
  )
  {
    result.computeIfAbsent(table, k -> new TreeMap<>())
          .computeIfAbsent(column, k -> EnumSet.noneOf(ColumnUsage.class))
          .addAll(roles);
  }

  /**
   * Deep-copies a column-to-roles map so a wrapper/join branch can add its own roles without mutating
   * the shared {@link EnumSet} values of the map it was handed (the same {@code roles} instance is
   * passed to every {@link UnionDataSource} member, so in-place mutation would leak across members).
   */
  private static Map<String, EnumSet<ColumnUsage>> copyRoles(Map<String, EnumSet<ColumnUsage>> roles)
  {
    Map<String, EnumSet<ColumnUsage>> copy = new TreeMap<>();
    for (Map.Entry<String, EnumSet<ColumnUsage>> entry : roles.entrySet()) {
      copy.put(entry.getKey(), EnumSet.copyOf(entry.getValue()));
    }
    return copy;
  }

  /**
   * Walks the query's column-bearing parts (projected columns, dimensions, aggregator inputs, filter
   * columns) and records each referenced base column with the role(s) it was used in. Virtual columns
   * are expanded transitively to their underlying base columns, carrying the consuming role. Only
   * explicitly-referenced columns are captured (notably {@code __time} is included only when it
   * actually appears in a part, never its implicit interval usage). Returns an empty map for
   * unsupported query types and for a bare {@code SELECT *} (a Scan with neither explicit columns nor
   * a filter); a {@code SELECT *} that carries a filter still contributes its filter columns.
   */
  private static Map<String, EnumSet<ColumnUsage>> collectColumnRoles(Query<?> query)
  {
    Map<String, EnumSet<ColumnUsage>> roles = new TreeMap<>();
    if (query instanceof ScanQuery) {
      ScanQuery scan = (ScanQuery) query;
      VirtualColumns vcs = scan.getVirtualColumns();
      if (scan.getColumns() != null) {
        for (String column : scan.getColumns()) {
          addColumn(roles, vcs, column, ColumnUsage.PROJECTION);
        }
      }
      addFilterColumns(roles, vcs, scan.getFilter());
    } else if (query instanceof GroupByQuery) {
      GroupByQuery groupBy = (GroupByQuery) query;
      VirtualColumns vcs = groupBy.getVirtualColumns();
      for (DimensionSpec dimension : groupBy.getDimensions()) {
        addColumn(roles, vcs, dimension.getDimension(), ColumnUsage.GROUP_BY);
      }
      addAggregatorColumns(roles, vcs, groupBy.getAggregatorSpecs());
      addFilterColumns(roles, vcs, groupBy.getDimFilter());
    } else if (query instanceof TopNQuery) {
      TopNQuery topN = (TopNQuery) query;
      VirtualColumns vcs = topN.getVirtualColumns();
      addColumn(roles, vcs, topN.getDimensionSpec().getDimension(), ColumnUsage.GROUP_BY);
      addAggregatorColumns(roles, vcs, topN.getAggregatorSpecs());
      addFilterColumns(roles, vcs, topN.getFilter());
    } else if (query instanceof TimeseriesQuery) {
      TimeseriesQuery timeseries = (TimeseriesQuery) query;
      VirtualColumns vcs = timeseries.getVirtualColumns();
      addAggregatorColumns(roles, vcs, timeseries.getAggregatorSpecs());
      addFilterColumns(roles, vcs, timeseries.getFilter());
    } else {
      // Unsupported query type: no column-level information.
      return Collections.emptyMap();
    }
    return roles;
  }

  private static void addAggregatorColumns(
      Map<String, EnumSet<ColumnUsage>> roles,
      VirtualColumns vcs,
      List<AggregatorFactory> aggregators
  )
  {
    for (AggregatorFactory aggregator : aggregators) {
      if (aggregator instanceof FilteredAggregatorFactory) {
        // e.g. SUM(x) FILTER (WHERE y = ...): requiredFields() mixes the aggregation input (x) with
        // the filter predicate (y). Tag the delegate's inputs AGGREGATION and the predicate FILTER.
        FilteredAggregatorFactory filtered = (FilteredAggregatorFactory) aggregator;
        addAggregatorColumns(roles, vcs, Collections.singletonList(filtered.getAggregator()));
        addFilterColumns(roles, vcs, filtered.getFilter());
      } else {
        for (String column : aggregator.requiredFields()) {
          addColumn(roles, vcs, column, ColumnUsage.AGGREGATION);
        }
      }
    }
  }

  private static void addFilterColumns(
      Map<String, EnumSet<ColumnUsage>> roles,
      @Nullable VirtualColumns vcs,
      @Nullable DimFilter filter
  )
  {
    if (filter != null) {
      for (String column : filter.getRequiredColumns()) {
        addColumn(roles, vcs, column, ColumnUsage.FILTER);
      }
    }
  }

  private static void addColumn(
      Map<String, EnumSet<ColumnUsage>> roles,
      @Nullable VirtualColumns vcs,
      String column,
      ColumnUsage role
  )
  {
    expandColumn(roles, vcs, column, role, new HashSet<>());
  }

  /**
   * Adds {@code column} with {@code role}, expanding virtual columns transitively to their underlying
   * base columns. The {@code visited} set guards against virtual columns that reference one another.
   */
  private static void expandColumn(
      Map<String, EnumSet<ColumnUsage>> roles,
      @Nullable VirtualColumns vcs,
      @Nullable String column,
      ColumnUsage role,
      Set<String> visited
  )
  {
    if (column == null || !visited.add(column)) {
      return;
    }
    if (vcs != null && vcs.exists(column)) {
      VirtualColumn virtualColumn = vcs.getVirtualColumn(column);
      if (virtualColumn != null) {
        for (String required : virtualColumn.requiredColumns()) {
          expandColumn(roles, vcs, required, role, visited);
        }
        return;
      }
    }
    roles.computeIfAbsent(column, k -> EnumSet.noneOf(ColumnUsage.class)).add(role);
  }
}
