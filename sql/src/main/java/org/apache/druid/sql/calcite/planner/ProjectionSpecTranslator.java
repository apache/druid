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

package org.apache.druid.sql.calcite.planner;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidSqlInput;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.RangeFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.security.AuthorizationResult;
import org.apache.druid.server.security.NoopEscalator;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.apache.druid.sql.calcite.rel.Grouping;
import org.apache.druid.sql.calcite.table.DatasourceTable;
import org.apache.druid.sql.calcite.table.DatasourceTable.PhysicalDatasourceMetadata;
import org.apache.druid.sql.calcite.table.DruidTable;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Turns the SQL body of a projection definition into the {@link AggregateProjectionSpec} the catalog stores.
 * <p>
 * The body is planned through the normal pipeline against the columns the enclosing statement declares, and the
 * specification is lifted out of the resulting native query. Going through the planner is the point: a projection is
 * only useful if it matches the queries the planner generates at query time, and that agreement is guaranteed when
 * the same machinery produces both. It also means aggregators contributed by extensions work without a second
 * registry.
 */
public class ProjectionSpecTranslator
{
  /**
   * Planning is deterministic in the shapes the lift understands: no timeseries or topN rewrite to hide the grouping
   * columns, and no approximation choices that depend on unrelated configuration.
   */
  private static final Map<String, Object> CONTEXT = ImmutableMap.of(
      PlannerContext.CTX_SQL_USE_GRANULARITY, false,
      QueryContexts.TIME_BOUNDARY_PLANNING_KEY, false,
      PlannerConfig.CTX_KEY_USE_APPROXIMATE_TOPN, false
  );

  private final PlannerFactory plannerFactory;

  public ProjectionSpecTranslator(PlannerFactory plannerFactory)
  {
    this.plannerFactory = plannerFactory;
  }

  /**
   * Translate one projection definition.
   *
   * @param tableName the table the projection belongs to
   * @param columns   the table's declared columns, which are the only ones the body may reference
   */
  public AggregateProjectionSpec translate(
      final String tableName,
      final List<ColumnSpec> columns,
      final String projectionName,
      final SqlSelect body
  )
  {
    rejectSubqueries(projectionName, body);

    final DruidQuery druidQuery = planBody(tableName, columns, projectionName, body);
    return lift(projectionName, tableName, druidQuery);
  }

  /**
   * Plan the body against a table built from the declared columns. The table is synthesized rather than looked up
   * because for {@code CREATE TABLE} it does not exist yet, and for {@code ALTER TABLE} the statement's own columns
   * are what the projection must agree with, not whatever a possibly stale cache holds.
   */
  private DruidQuery planBody(
      final String tableName,
      final List<ColumnSpec> columns,
      final String projectionName,
      final SqlSelect body
  )
  {
    final SqlSelect query = (SqlSelect) body.clone(body.getParserPosition());
    query.setFrom(new SqlIdentifier(tableName, SqlParserPos.ZERO));

    final ProjectionSqlEngine engine = new ProjectionSqlEngine();
    final String sql = query.toString();
    try (DruidPlanner planner = plannerFactory.createPlannerForTable(
        engine,
        sql,
        query,
        CONTEXT,
        tableName,
        tableFor(tableName, columns)
    )) {
      planner.getPlannerContext()
             .setAuthenticationResult(NoopEscalator.getInstance().createEscalatedAuthenticationResult());
      planner.validate();
      planner.authorize(ra -> AuthorizationResult.ALLOW_NO_RESTRICTION, Collections.emptySet());
      planner.plan().run();
    }
    catch (DruidException e) {
      throw contextualize(projectionName, e);
    }
    return engine.captured();
  }

  /**
   * Build the table the body is planned against. Mirrors how a catalog-only table is presented to the planner:
   * declared columns in declared order, with {@code __time} supplied if the statement did not declare it.
   */
  private static DruidTable tableFor(final String tableName, final List<ColumnSpec> columns)
  {
    RowSignature.Builder builder = RowSignature.builder();
    boolean hasTime = false;
    for (ColumnSpec column : columns) {
      ColumnType type = Columns.druidType(column);
      if (type == null) {
        type = ColumnType.STRING;
      }
      if (Columns.isTimeColumn(column.name())) {
        hasTime = true;
      }
      builder.add(column.name(), type);
    }
    if (!hasTime) {
      builder = RowSignature.builder()
                            .add(Columns.TIME_COLUMN, ColumnType.LONG)
                            .addAll(builder.build());
    }
    final RowSignature signature = builder.build();
    return new DatasourceTable(
        signature,
        new PhysicalDatasourceMetadata(new TableDataSource(tableName), signature, false, false),
        DatasourceTable.EffectiveMetadata.of(signature)
    );
  }

  /**
   * Lift the projection specification out of the planned query.
   */
  private static AggregateProjectionSpec lift(
      final String projectionName,
      final String tableName,
      final DruidQuery druidQuery
  )
  {
    final DataSource dataSource = druidQuery.getDataSource();
    if (!(dataSource instanceof TableDataSource) || !tableName.equals(((TableDataSource) dataSource).getName())) {
      throw invalid(
          projectionName,
          "its body requires more than one pass over the data. Rewrite it as a single aggregation, for example by"
          + " using APPROX_COUNT_DISTINCT instead of COUNT(DISTINCT ...)"
      );
    }

    final Grouping grouping = druidQuery.getGrouping();
    if (grouping == null) {
      throw invalid(projectionName, "its body does not aggregate. Add a GROUP BY clause, or use SELECT DISTINCT");
    }
    if (!grouping.getPostAggregators().isEmpty()) {
      throw invalid(
          projectionName,
          "its body computes an expression over aggregates, which a projection cannot store. Store the aggregates"
          + " themselves instead, for example SUM(x) and COUNT(x) rather than AVG(x)"
      );
    }
    if (grouping.getHavingFilter() != null) {
      throw invalid(projectionName, "its body has a HAVING clause, which a projection cannot store");
    }

    final Query<?> query = druidQuery.getQuery();
    final List<DimensionSpec> dimensions;
    final List<AggregatorFactory> aggregators;
    final VirtualColumnsAndFilter virtualColumnsAndFilter;
    if (query instanceof GroupByQuery) {
      final GroupByQuery groupBy = (GroupByQuery) query;
      dimensions = groupBy.getDimensions();
      aggregators = groupBy.getAggregatorSpecs();
      virtualColumnsAndFilter = new VirtualColumnsAndFilter(
          groupBy.getVirtualColumns(),
          groupBy.getDimFilter(),
          groupBy.getIntervals()
      );
    } else if (query instanceof TimeseriesQuery) {
      // GROUP BY () plans to a timeseries over all time; it has no grouping columns.
      final TimeseriesQuery timeseries = (TimeseriesQuery) query;
      dimensions = Collections.emptyList();
      aggregators = List.of(timeseries.getAggregatorSpecs().toArray(new AggregatorFactory[0]));
      virtualColumnsAndFilter = new VirtualColumnsAndFilter(
          timeseries.getVirtualColumns(),
          timeseries.getFilter(),
          timeseries.getIntervals()
      );
    } else {
      throw invalid(
          projectionName,
          "its body did not plan to an aggregation. Add a GROUP BY clause, or use SELECT DISTINCT"
      );
    }

    return AggregateProjectionSpec
        .builder(projectionName)
        .virtualColumns(virtualColumnsAndFilter.virtualColumns)
        .filter(virtualColumnsAndFilter.filter(projectionName))
        .groupingColumns(groupingColumns(projectionName, dimensions))
        .aggregators(renameToOutputNames(projectionName, druidQuery, aggregators))
        .build();
  }

  private static List<DimensionSchema> groupingColumns(
      final String projectionName,
      final List<DimensionSpec> dimensions
  )
  {
    final List<DimensionSchema> groupingColumns = new ArrayList<>(dimensions.size());
    for (DimensionSpec dimension : dimensions) {
      if (!(dimension instanceof DefaultDimensionSpec)) {
        throw invalid(
            projectionName,
            "its grouping column [" + dimension.getOutputName() + "] is not a plain column reference"
        );
      }
      final ColumnType type = dimension.getOutputType();
      if (type == null || (!type.isPrimitive() && !type.isArray())) {
        throw invalid(
            projectionName,
            "its grouping column [" + dimension.getDimension() + "] has type [" + type
            + "], which a projection cannot group on"
        );
      }
      // The stored name is the physical column or virtual column the planner grouped on, not the SELECT alias:
      // projections are matched structurally against a query's grouping columns, not by output name.
      groupingColumns.add(DimensionSchema.getDefaultSchemaForBuiltInType(dimension.getDimension(), type));
    }
    return groupingColumns;
  }

  /**
   * Aggregators are stored under the name the projection's own column will have, which is the SELECT alias. The
   * planner names them {@code a0}, {@code a1}, ... internally, so an explicit alias is required.
   */
  private static AggregatorFactory[] renameToOutputNames(
      final String projectionName,
      final DruidQuery druidQuery,
      final List<AggregatorFactory> aggregators
  )
  {
    final RowSignature internal = druidQuery.getOutputRowSignature();
    final List<String> outputNames = druidQuery.getOutputRowType().getFieldNames();
    final AggregatorFactory[] renamed = new AggregatorFactory[aggregators.size()];
    for (int i = 0; i < aggregators.size(); i++) {
      final AggregatorFactory aggregator = aggregators.get(i);
      final int position = internal.indexOf(aggregator.getName());
      final String outputName = position < 0 || position >= outputNames.size()
                                ? aggregator.getName()
                                : outputNames.get(position);
      if (isPlannerGeneratedName(outputName)) {
        throw invalid(
            projectionName,
            "one of its aggregate expressions has no name. Give every aggregate an alias, for example"
            + " SUM(x) AS sum_x"
        );
      }
      renamed[i] = aggregator.withName(outputName);
    }
    return renamed;
  }

  private static boolean isPlannerGeneratedName(String name)
  {
    return name.startsWith("EXPR$");
  }

  /**
   * The virtual columns, filter and intervals of the planned query. Kept together because a time filter written in
   * the body is moved out of the filter and into the query's intervals during planning, and has to be put back:
   * a projection has nowhere to store an interval.
   */
  private static class VirtualColumnsAndFilter
  {
    private final VirtualColumns virtualColumns;
    @Nullable
    private final DimFilter dimFilter;
    private final List<Interval> intervals;

    VirtualColumnsAndFilter(
        VirtualColumns virtualColumns,
        @Nullable DimFilter dimFilter,
        List<Interval> intervals
    )
    {
      this.virtualColumns = virtualColumns;
      this.dimFilter = dimFilter;
      this.intervals = intervals;
    }

    @Nullable
    DimFilter filter(String projectionName)
    {
      if (intervals.size() == 1 && Intervals.ETERNITY.equals(intervals.get(0))) {
        return dimFilter;
      }
      if (intervals.size() != 1) {
        throw invalid(
            projectionName,
            "its WHERE clause selects more than one time range, which a projection cannot store"
        );
      }
      final Interval interval = intervals.get(0);
      final DimFilter timeFilter = new RangeFilter(
          Columns.TIME_COLUMN,
          ColumnType.LONG,
          interval.getStartMillis(),
          interval.getEndMillis(),
          false,
          true,
          null
      );
      return dimFilter == null ? timeFilter : new AndDimFilter(timeFilter, dimFilter);
    }
  }

  /**
   * Subqueries would be planned as a second pass over the data, which a projection cannot represent. The grammar
   * cannot exclude them, because they appear inside expressions.
   */
  private static void rejectSubqueries(final String projectionName, final SqlSelect body)
  {
    body.accept(new SqlBasicVisitor<Void>()
    {
      @Override
      public Void visit(SqlCall call)
      {
        if (call instanceof SqlSelect && call != body) {
          throw invalid(projectionName, "its body contains a subquery, which a projection cannot store");
        }
        return super.visit(call);
      }
    });
  }

  private static DruidException contextualize(final String projectionName, final DruidException e)
  {
    if (e.getTargetPersona() == DruidException.Persona.USER) {
      return InvalidSqlInput.exception(e, "Cannot define projection [%s]: %s", projectionName, e.getMessage());
    }
    return e;
  }

  private static DruidException invalid(final String projectionName, final String reason)
  {
    return InvalidSqlInput.exception("Cannot define projection [%s] because %s", projectionName, reason);
  }

}
