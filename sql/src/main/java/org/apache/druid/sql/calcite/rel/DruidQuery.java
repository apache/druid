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

package org.apache.druid.sql.calcite.rel;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Ordering;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.math.expr.ExprType;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.ExpressionPostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.having.DimFilterHavingSpec;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.ordering.StringComparator;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.select.PagingSpec;
import org.apache.druid.query.select.SelectQuery;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.topn.DimensionTopNMetricSpec;
import org.apache.druid.query.topn.InvertedTopNMetricSpec;
import org.apache.druid.query.topn.NumericTopNMetricSpec;
import org.apache.druid.query.topn.TopNMetricSpec;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.DimensionExpression;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rule.GroupByRules;
import org.apache.druid.sql.calcite.table.RowSignature;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * A fully formed Druid query, built from a {@link PartialDruidQuery}. The work to develop this query is done
 * during construction, which may throw {@link CannotBuildQueryException}.
 */
public class DruidQuery
{
  private final DataSource dataSource;
  private final PlannerContext plannerContext;

  @Nullable
  private final DimFilter filter;

  @Nullable
  private final SelectProjection selectProjection;

  @Nullable
  private final Grouping grouping;

  @Nullable
  private final SortProject sortProject;

  @Nullable
  private final DefaultLimitSpec limitSpec;

  @Nullable
  private final RowSignature outputRowSignature;

  @Nullable
  private final RelDataType outputRowType;

  private final Query query;

  private final DruidQuerySignature sourceQuerySignature;

  public DruidQuery(
      final PartialDruidQuery partialQuery,
      final DataSource dataSource,
      final RowSignature sourceRowSignature,
      final PlannerContext plannerContext,
      final RexBuilder rexBuilder,
      final boolean finalizeAggregations
  )
  {
    this.dataSource = dataSource;
    this.outputRowType = partialQuery.leafRel().getRowType();
    this.sourceQuerySignature = new DruidQuerySignature(sourceRowSignature);
    this.plannerContext = plannerContext;

    // Now the fun begins.
    this.filter = computeWhereFilter(partialQuery, plannerContext, sourceQuerySignature);
    this.selectProjection = computeSelectProjection(partialQuery, plannerContext, sourceQuerySignature);
    this.grouping = computeGrouping(partialQuery, plannerContext, sourceQuerySignature, rexBuilder, finalizeAggregations);

    final RowSignature sortingInputRowSignature;

    if (this.selectProjection != null) {
      sortingInputRowSignature = this.selectProjection.getOutputRowSignature();
    } else if (this.grouping != null) {
      sortingInputRowSignature = this.grouping.getOutputRowSignature();
    } else {
      sortingInputRowSignature = sourceRowSignature;
    }

    this.sortProject = computeSortProject(partialQuery, plannerContext, sortingInputRowSignature);

    // outputRowSignature is used only for scan and select query, and thus sort and grouping must be null
    this.outputRowSignature = sortProject == null ? sortingInputRowSignature : sortProject.getOutputRowSignature();

    this.limitSpec = computeLimitSpec(partialQuery, sortingInputRowSignature);
    this.query = computeQuery();
  }

  @Nullable
  private static DimFilter computeWhereFilter(
      final PartialDruidQuery partialQuery,
      final PlannerContext plannerContext,
      final DruidQuerySignature querySignature
  )
  {
    final Filter whereFilter = partialQuery.getWhereFilter();

    if (whereFilter == null) {
      return null;
    }

    return getDimFilter(plannerContext, querySignature, whereFilter);
  }

  @Nullable
  private static DimFilter computeHavingFilter(
      final PartialDruidQuery partialQuery,
      final PlannerContext plannerContext,
      final DruidQuerySignature querySignature
  )
  {
    final Filter havingFilter = partialQuery.getHavingFilter();

    if (havingFilter == null) {
      return null;
    }

    return getDimFilter(plannerContext, querySignature, havingFilter);
  }

  @Nonnull
  private static DimFilter getDimFilter(
      final PlannerContext plannerContext,
      final DruidQuerySignature querySignature,
      Filter filter
  )
  {
    final RexNode condition = filter.getCondition();
    final DimFilter dimFilter = Expressions.toFilter(
        plannerContext,
        querySignature,
        condition
    );
    if (dimFilter == null) {
      throw new CannotBuildQueryException(filter, condition);
    } else {
      return dimFilter;
    }
  }

  @Nullable
  private static SelectProjection computeSelectProjection(
      final PartialDruidQuery partialQuery,
      final PlannerContext plannerContext,
      final DruidQuerySignature queryColumns
  )
  {
    final Project project = partialQuery.getSelectProject();

    if (project == null || partialQuery.getAggregate() != null) {
      return null;
    }

    final List<DruidExpression> expressions = new ArrayList<>();

    for (final RexNode rexNode : project.getChildExps()) {
      final DruidExpression expression = Expressions.toDruidExpression(
          plannerContext,
          queryColumns.getRowSignature(),
          rexNode
      );

      if (expression == null) {
        throw new CannotBuildQueryException(project, rexNode);
      } else {
        expressions.add(expression);
      }
    }

    final List<String> directColumns = new ArrayList<>();
    final Set<VirtualColumn> virtualColumns = new HashSet<>();
    final List<String> rowOrder = new ArrayList<>();

    for (int i = 0; i < expressions.size(); i++) {
      final DruidExpression expression = expressions.get(i);
      if (expression.isDirectColumnAccess()) {
        directColumns.add(expression.getDirectColumn());
        rowOrder.add(expression.getDirectColumn());
      } else {
        VirtualColumn virtualColumn = queryColumns.getOrCreateVirtualColumnForExpression(
            plannerContext,
            expression,
            project.getChildExps().get(i).getType().getSqlTypeName()
        );
        virtualColumns.add(virtualColumn);
        rowOrder.add(virtualColumn.getOutputName());
      }
    }

    return new SelectProjection(
        directColumns,
        ImmutableList.copyOf(virtualColumns),
        RowSignature.from(rowOrder, project.getRowType())
    );
  }

  @Nullable
  private static Grouping computeGrouping(
      final PartialDruidQuery partialQuery,
      final PlannerContext plannerContext,
      final DruidQuerySignature queryColumns,
      final RexBuilder rexBuilder,
      final boolean finalizeAggregations
  )
  {
    final Aggregate aggregate = partialQuery.getAggregate();
    final Project aggregateProject = partialQuery.getAggregateProject();

    if (aggregate == null) {
      return null;
    }

    final List<DimensionExpression> dimensions = computeDimensions(partialQuery, plannerContext, queryColumns);
    final List<Aggregation> aggregations = computeAggregations(
        partialQuery,
        plannerContext,
        queryColumns,
        rexBuilder,
        finalizeAggregations
    );

    final RowSignature aggregateRowSignature = RowSignature.from(
        ImmutableList.copyOf(
            Iterators.concat(
                dimensions.stream().map(DimensionExpression::getOutputName).iterator(),
                aggregations.stream().map(Aggregation::getOutputName).iterator()
            )
        ),
        aggregate.getRowType()
    );

    DruidQuerySignature aggregateContext = queryColumns.withRowSignature(aggregateRowSignature).asImmutable();
    final DimFilter havingFilter = computeHavingFilter(
        partialQuery,
        plannerContext,
        aggregateContext
    );

    if (aggregateProject == null) {
      return Grouping.create(dimensions, aggregations, havingFilter, aggregateRowSignature);
    } else {
      final ProjectRowOrderAndPostAggregations projectRowOrderAndPostAggregations = computePostAggregations(
          plannerContext,
          aggregateRowSignature,
          aggregateProject,
          "p"
      );
      projectRowOrderAndPostAggregations.postAggregations.forEach(
          postAggregator -> aggregations.add(Aggregation.create(postAggregator))
      );

      // Remove literal dimensions that did not appear in the projection. This is useful for queries
      // like "SELECT COUNT(*) FROM tbl GROUP BY 'dummy'" which some tools can generate, and for which we don't
      // actually want to include a dimension 'dummy'.
      final ImmutableBitSet aggregateProjectBits = RelOptUtil.InputFinder.bits(aggregateProject.getChildExps(), null);
      for (int i = dimensions.size() - 1; i >= 0; i--) {
        final DimensionExpression dimension = dimensions.get(i);
        if (Parser.parse(dimension.getDruidExpression().getExpression(), plannerContext.getExprMacroTable())
                  .isLiteral() && !aggregateProjectBits.get(i)) {
          dimensions.remove(i);
        }
      }

      return Grouping.create(
          dimensions,
          aggregations,
          havingFilter,
          RowSignature.from(projectRowOrderAndPostAggregations.rowOrder, aggregateProject.getRowType())
      );
    }
  }

  @Nullable
  private SortProject computeSortProject(
      PartialDruidQuery partialQuery,
      PlannerContext plannerContext,
      RowSignature sortingInputRowSignature
  )
  {
    final Project sortProject = partialQuery.getSortProject();
    if (sortProject == null) {
      return null;
    } else {
      final ProjectRowOrderAndPostAggregations projectRowOrderAndPostAggregations = computePostAggregations(
          plannerContext,
          sortingInputRowSignature,
          sortProject,
          "s"
      );

      return new SortProject(
          sortingInputRowSignature,
          projectRowOrderAndPostAggregations.postAggregations,
          RowSignature.from(projectRowOrderAndPostAggregations.rowOrder, sortProject.getRowType())
      );
    }
  }

  private static class ProjectRowOrderAndPostAggregations
  {
    private final List<String> rowOrder;
    private final List<PostAggregator> postAggregations;

    ProjectRowOrderAndPostAggregations(List<String> rowOrder, List<PostAggregator> postAggregations)
    {
      this.rowOrder = rowOrder;
      this.postAggregations = postAggregations;
    }
  }

  private static ProjectRowOrderAndPostAggregations computePostAggregations(
      PlannerContext plannerContext,
      RowSignature inputRowSignature,
      Project project,
      String basePrefix
  )
  {
    final List<String> rowOrder = new ArrayList<>();
    final List<PostAggregator> aggregations = new ArrayList<>();
    final String outputNamePrefix = Calcites.findUnusedPrefix(
        basePrefix,
        new TreeSet<>(inputRowSignature.getRowOrder())
    );

    int outputNameCounter = 0;
    for (final RexNode postAggregatorRexNode : project.getChildExps()) {
      // Attempt to convert to PostAggregator.
      final DruidExpression postAggregatorExpression = Expressions.toDruidExpression(
          plannerContext,
          inputRowSignature,
          postAggregatorRexNode
      );

      if (postAggregatorExpression == null) {
        throw new CannotBuildQueryException(project, postAggregatorRexNode);
      }

      if (postAggregatorDirectColumnIsOk(inputRowSignature, postAggregatorExpression, postAggregatorRexNode)) {
        // Direct column access, without any type cast as far as Druid's runtime is concerned.
        // (There might be a SQL-level type cast that we don't care about)
        rowOrder.add(postAggregatorExpression.getDirectColumn());
      } else {
        final String postAggregatorName = outputNamePrefix + outputNameCounter++;
        final PostAggregator postAggregator = new ExpressionPostAggregator(
            postAggregatorName,
            postAggregatorExpression.getExpression(),
            null,
            plannerContext.getExprMacroTable()
        );
        aggregations.add(postAggregator);
        rowOrder.add(postAggregator.getName());
      }
    }

    return new ProjectRowOrderAndPostAggregations(rowOrder, aggregations);
  }

  /**
   * Returns dimensions corresponding to {@code aggregate.getGroupSet()}, in the same order.
   *
   * @param partialQuery       partial query
   * @param plannerContext     planner context
   * @param querySignature     source row signature and re-usable virtual column references
   *
   * @return dimensions
   *
   * @throws CannotBuildQueryException if dimensions cannot be computed
   */
  private static List<DimensionExpression> computeDimensions(
      final PartialDruidQuery partialQuery,
      final PlannerContext plannerContext,
      final DruidQuerySignature querySignature
  )
  {
    final Aggregate aggregate = Preconditions.checkNotNull(partialQuery.getAggregate());
    final List<DimensionExpression> dimensions = new ArrayList<>();
    final String outputNamePrefix = Calcites.findUnusedPrefix("d", new TreeSet<>(querySignature.getRowSignature().getRowOrder()));
    int outputNameCounter = 0;

    for (int i : aggregate.getGroupSet()) {
      // Dimension might need to create virtual columns. Avoid giving it a name that would lead to colliding columns.
      final RexNode rexNode = Expressions.fromFieldAccess(querySignature.getRowSignature(), partialQuery.getSelectProject(), i);
      final DruidExpression druidExpression = Expressions.toDruidExpression(
          plannerContext,
          querySignature.getRowSignature(),
          rexNode
      );
      if (druidExpression == null) {
        throw new CannotBuildQueryException(aggregate, rexNode);
      }

      final SqlTypeName sqlTypeName = rexNode.getType().getSqlTypeName();
      final ValueType outputType = Calcites.getValueTypeForSqlTypeName(sqlTypeName);
      if (outputType == null || outputType == ValueType.COMPLEX) {
        // Can't group on unknown or COMPLEX types.
        throw new CannotBuildQueryException(aggregate, rexNode);
      }

      final VirtualColumn virtualColumn;

      final String dimOutputName;
      if (!druidExpression.isSimpleExtraction()) {
        virtualColumn = querySignature.getOrCreateVirtualColumnForExpression(plannerContext, druidExpression, sqlTypeName);
        dimOutputName = virtualColumn.getOutputName();
      } else {
        dimOutputName = outputNamePrefix + outputNameCounter++;
      }

      dimensions.add(new DimensionExpression(dimOutputName, druidExpression, outputType));
    }

    return dimensions;
  }

  /**
   * Returns aggregations corresponding to {@code aggregate.getAggCallList()}, in the same order.
   *
   * @param partialQuery         partial query
   * @param plannerContext       planner context
   * @param querySignature       source row signature and re-usable virtual column references
   * @param rexBuilder           calcite RexBuilder
   * @param finalizeAggregations true if this query should include explicit finalization for all of its
   *                             aggregators, where required. Useful for subqueries where Druid's native query layer
   *                             does not do this automatically.
   *
   * @return aggregations
   *
   * @throws CannotBuildQueryException if dimensions cannot be computed
   */
  private static List<Aggregation> computeAggregations(
      final PartialDruidQuery partialQuery,
      final PlannerContext plannerContext,
      final DruidQuerySignature querySignature,
      final RexBuilder rexBuilder,
      final boolean finalizeAggregations
  )
  {
    final Aggregate aggregate = Preconditions.checkNotNull(partialQuery.getAggregate());
    final List<Aggregation> aggregations = new ArrayList<>();
    final String outputNamePrefix = Calcites.findUnusedPrefix("a", new TreeSet<>(querySignature.getRowSignature().getRowOrder()));

    for (int i = 0; i < aggregate.getAggCallList().size(); i++) {
      final String aggName = outputNamePrefix + i;
      final AggregateCall aggCall = aggregate.getAggCallList().get(i);
      final Aggregation aggregation = GroupByRules.translateAggregateCall(
          plannerContext,
          querySignature,
          rexBuilder,
          partialQuery.getSelectProject(),
          aggregations,
          aggName,
          aggCall,
          finalizeAggregations
      );

      if (aggregation == null) {
        throw new CannotBuildQueryException(aggregate, aggCall);
      }

      aggregations.add(aggregation);
    }

    return aggregations;
  }

  @Nullable
  private static DefaultLimitSpec computeLimitSpec(
      final PartialDruidQuery partialQuery,
      final RowSignature outputRowSignature
  )
  {
    final Sort sort;

    if (partialQuery.getAggregate() == null) {
      sort = partialQuery.getSelectSort();
    } else {
      sort = partialQuery.getSort();
    }

    if (sort == null) {
      return null;
    }

    final Integer limit = sort.fetch != null ? RexLiteral.intValue(sort.fetch) : null;
    final List<OrderByColumnSpec> orderBys = new ArrayList<>(sort.getChildExps().size());

    if (sort.offset != null) {
      // LimitSpecs don't accept offsets.
      throw new CannotBuildQueryException(sort);
    }

    // Extract orderBy column specs.
    for (int sortKey = 0; sortKey < sort.getChildExps().size(); sortKey++) {
      final RexNode sortExpression = sort.getChildExps().get(sortKey);
      final RelFieldCollation collation = sort.getCollation().getFieldCollations().get(sortKey);
      final OrderByColumnSpec.Direction direction;
      final StringComparator comparator;

      if (collation.getDirection() == RelFieldCollation.Direction.ASCENDING) {
        direction = OrderByColumnSpec.Direction.ASCENDING;
      } else if (collation.getDirection() == RelFieldCollation.Direction.DESCENDING) {
        direction = OrderByColumnSpec.Direction.DESCENDING;
      } else {
        throw new ISE("WTF?! Don't know what to do with direction[%s]", collation.getDirection());
      }

      final SqlTypeName sortExpressionType = sortExpression.getType().getSqlTypeName();
      if (SqlTypeName.NUMERIC_TYPES.contains(sortExpressionType)
          || SqlTypeName.TIMESTAMP == sortExpressionType
          || SqlTypeName.DATE == sortExpressionType) {
        comparator = StringComparators.NUMERIC;
      } else {
        comparator = StringComparators.LEXICOGRAPHIC;
      }

      if (sortExpression.isA(SqlKind.INPUT_REF)) {
        final RexInputRef ref = (RexInputRef) sortExpression;
        final String fieldName = outputRowSignature.getRowOrder().get(ref.getIndex());
        orderBys.add(new OrderByColumnSpec(fieldName, direction, comparator));
      } else {
        // We don't support sorting by anything other than refs which actually appear in the query result.
        throw new CannotBuildQueryException(sort, sortExpression);
      }
    }

    return new DefaultLimitSpec(orderBys, limit);
  }

  /**
   * Returns true if a post-aggregation "expression" can be realized as a direct field access. This is true if it's
   * a direct column access that doesn't require an implicit cast.
   *
   * @param aggregateRowSignature signature of the aggregation
   * @param expression            post-aggregation expression
   * @param rexNode               RexNode for the post-aggregation expression
   *
   * @return yes or no
   */
  private static boolean postAggregatorDirectColumnIsOk(
      final RowSignature aggregateRowSignature,
      final DruidExpression expression,
      final RexNode rexNode
  )
  {
    if (!expression.isDirectColumnAccess()) {
      return false;
    }

    // Check if a cast is necessary.
    final ExprType toExprType = Expressions.exprTypeForValueType(
        aggregateRowSignature.getColumnType(expression.getDirectColumn())
    );

    final ExprType fromExprType = Expressions.exprTypeForValueType(
        Calcites.getValueTypeForSqlTypeName(rexNode.getType().getSqlTypeName())
    );

    return toExprType.equals(fromExprType);
  }

  public VirtualColumns getVirtualColumns(final boolean includeDimensions)
  {
    // 'sourceRowSignature' could provide a list of all defined virtual columns while constructing a query, but we
    // still want to collect the set of VirtualColumns this way to ensure we only add what is still being used after
    // the various transforms and optimizations
    Set<VirtualColumn> virtualColumns = new HashSet<>();

    // we always want to add any virtual columns used by the query level DimFilter
    if (filter != null) {
      for (String columnName : filter.getRequiredColumns()) {
        if (sourceQuerySignature.isVirtualColumnDefined(columnName)) {
          virtualColumns.add(sourceQuerySignature.getVirtualColumn(columnName));
        }
      }
    }
    if (selectProjection != null) {
      virtualColumns.addAll(selectProjection.getVirtualColumns());
    } else {
      if (grouping != null) {
        if (includeDimensions) {
          for (DimensionExpression expression : grouping.getDimensions()) {
            if (sourceQuerySignature.isVirtualColumnDefined(expression.getOutputName())) {
              virtualColumns.add(sourceQuerySignature.getVirtualColumn(expression.getOutputName()));
            }
          }
        }

        for (Aggregation aggregation : grouping.getAggregations()) {
          virtualColumns.addAll(aggregation.getVirtualColumns());
        }
      }
    }

    // sort for predictable output
    List<VirtualColumn> columns = new ArrayList<>(virtualColumns);
    columns.sort(Comparator.comparing(VirtualColumn::getOutputName));
    return VirtualColumns.create(columns);
  }

  public Grouping getGrouping()
  {
    return grouping;
  }

  public DefaultLimitSpec getLimitSpec()
  {
    return limitSpec;
  }

  public SortProject getSortProject()
  {
    return sortProject;
  }

  public RelDataType getOutputRowType()
  {
    return outputRowType;
  }

  public RowSignature getOutputRowSignature()
  {
    return outputRowSignature;
  }

  public Query getQuery()
  {
    return query;
  }

  /**
   * Return this query as some kind of Druid query. The returned query will either be {@link TopNQuery},
   * {@link TimeseriesQuery}, {@link GroupByQuery}, {@link ScanQuery}, or {@link SelectQuery}.
   *
   * @return Druid query
   */
  private Query computeQuery()
  {
    if (dataSource instanceof QueryDataSource) {
      // If there is a subquery then the outer query must be a groupBy.
      final GroupByQuery outerQuery = toGroupByQuery();

      if (outerQuery == null) {
        // Bug in the planner rules. They shouldn't allow this to happen.
        throw new IllegalStateException("Can't use QueryDataSource without an outer groupBy query!");
      }

      return outerQuery;
    }

    final TimeseriesQuery tsQuery = toTimeseriesQuery();
    if (tsQuery != null) {
      return tsQuery;
    }

    final TopNQuery topNQuery = toTopNQuery();
    if (topNQuery != null) {
      return topNQuery;
    }

    final GroupByQuery groupByQuery = toGroupByQuery();
    if (groupByQuery != null) {
      return groupByQuery;
    }

    final ScanQuery scanQuery = toScanQuery();
    if (scanQuery != null) {
      return scanQuery;
    }

    final SelectQuery selectQuery = toSelectQuery();
    if (selectQuery != null) {
      return selectQuery;
    }

    throw new CannotBuildQueryException("Cannot convert query parts into an actual query");
  }

  /**
   * Return this query as a Timeseries query, or null if this query is not compatible with Timeseries.
   *
   * @return query
   */
  @Nullable
  public TimeseriesQuery toTimeseriesQuery()
  {
    if (grouping == null || grouping.getHavingFilter() != null) {
      return null;
    }

    final Granularity queryGranularity;
    final boolean descending;
    int timeseriesLimit = 0;
    if (grouping.getDimensions().isEmpty()) {
      queryGranularity = Granularities.ALL;
      descending = false;
    } else if (grouping.getDimensions().size() == 1) {
      final DimensionExpression dimensionExpression = Iterables.getOnlyElement(grouping.getDimensions());
      queryGranularity = Expressions.toQueryGranularity(
          dimensionExpression.getDruidExpression(),
          plannerContext.getExprMacroTable()
      );

      if (queryGranularity == null) {
        // Timeseries only applies if the single dimension is granular __time.
        return null;
      }
      if (limitSpec != null) {
        // If there is a limit spec, set timeseriesLimit to given value if less than Integer.Max_VALUE
        if (limitSpec.isLimited()) {
          timeseriesLimit = limitSpec.getLimit();
        }

        if (limitSpec.getColumns().isEmpty()) {
          descending = false;
        } else {
          // We're ok if the first order by is time (since every time value is distinct, the rest of the columns
          // wouldn't matter anyway).
          final OrderByColumnSpec firstOrderBy = limitSpec.getColumns().get(0);

          if (firstOrderBy.getDimension().equals(dimensionExpression.getOutputName())) {
            // Order by time.
            descending = firstOrderBy.getDirection() == OrderByColumnSpec.Direction.DESCENDING;
          } else {
            // Order by something else.
            return null;
          }
        }
      } else {
        // No limitSpec.
        descending = false;
      }
    } else {
      // More than one dimension, timeseries cannot handle.
      return null;
    }

    final Filtration filtration = Filtration.create(filter).optimize(sourceQuerySignature);

    final List<PostAggregator> postAggregators = new ArrayList<>(grouping.getPostAggregators());
    if (sortProject != null) {
      postAggregators.addAll(sortProject.getPostAggregators());
    }
    final Map<String, Object> theContext = new HashMap<>();
    theContext.put("skipEmptyBuckets", true);
    theContext.putAll(plannerContext.getQueryContext());

    return new TimeseriesQuery(
        dataSource,
        filtration.getQuerySegmentSpec(),
        descending,
        getVirtualColumns(false),
        filtration.getDimFilter(),
        queryGranularity,
        grouping.getAggregatorFactories(),
        postAggregators,
        timeseriesLimit,
        ImmutableSortedMap.copyOf(theContext)
    );
  }

  /**
   * Return this query as a TopN query, or null if this query is not compatible with TopN.
   *
   * @return query or null
   */
  @Nullable
  public TopNQuery toTopNQuery()
  {
    // Must have GROUP BY one column, ORDER BY zero or one column, limit less than maxTopNLimit, and no HAVING.
    final boolean topNOk = grouping != null
                           && grouping.getDimensions().size() == 1
                           && limitSpec != null
                           && (limitSpec.getColumns().size() <= 1
                               && limitSpec.getLimit() <= plannerContext.getPlannerConfig().getMaxTopNLimit())
                           && grouping.getHavingFilter() == null;

    if (!topNOk) {
      return null;
    }

    final DimensionSpec dimensionSpec = Iterables.getOnlyElement(grouping.getDimensions()).toDimensionSpec();
    final OrderByColumnSpec limitColumn;
    if (limitSpec.getColumns().isEmpty()) {
      limitColumn = new OrderByColumnSpec(
          dimensionSpec.getOutputName(),
          OrderByColumnSpec.Direction.ASCENDING,
          Calcites.getStringComparatorForValueType(dimensionSpec.getOutputType())
      );
    } else {
      limitColumn = Iterables.getOnlyElement(limitSpec.getColumns());
    }
    final TopNMetricSpec topNMetricSpec;

    if (limitColumn.getDimension().equals(dimensionSpec.getOutputName())) {
      // DimensionTopNMetricSpec is exact; always return it even if allowApproximate is false.
      final DimensionTopNMetricSpec baseMetricSpec = new DimensionTopNMetricSpec(
          null,
          limitColumn.getDimensionComparator()
      );
      topNMetricSpec = limitColumn.getDirection() == OrderByColumnSpec.Direction.ASCENDING
                       ? baseMetricSpec
                       : new InvertedTopNMetricSpec(baseMetricSpec);
    } else if (plannerContext.getPlannerConfig().isUseApproximateTopN()) {
      // ORDER BY metric
      final NumericTopNMetricSpec baseMetricSpec = new NumericTopNMetricSpec(limitColumn.getDimension());
      topNMetricSpec = limitColumn.getDirection() == OrderByColumnSpec.Direction.ASCENDING
                       ? new InvertedTopNMetricSpec(baseMetricSpec)
                       : baseMetricSpec;
    } else {
      return null;
    }

    final Filtration filtration = Filtration.create(filter).optimize(sourceQuerySignature);

    final List<PostAggregator> postAggregators = new ArrayList<>(grouping.getPostAggregators());
    if (sortProject != null) {
      postAggregators.addAll(sortProject.getPostAggregators());
    }

    return new TopNQuery(
        dataSource,
        getVirtualColumns(true),
        dimensionSpec,
        topNMetricSpec,
        limitSpec.getLimit(),
        filtration.getQuerySegmentSpec(),
        filtration.getDimFilter(),
        Granularities.ALL,
        grouping.getAggregatorFactories(),
        postAggregators,
        ImmutableSortedMap.copyOf(plannerContext.getQueryContext())
    );
  }

  /**
   * Return this query as a GroupBy query, or null if this query is not compatible with GroupBy.
   *
   * @return query or null
   */
  @Nullable
  public GroupByQuery toGroupByQuery()
  {
    if (grouping == null) {
      return null;
    }

    final Filtration filtration = Filtration.create(filter).optimize(sourceQuerySignature);

    final DimFilterHavingSpec havingSpec;
    if (grouping.getHavingFilter() != null) {
      havingSpec = new DimFilterHavingSpec(
          Filtration.create(grouping.getHavingFilter()).optimizeFilterOnly(sourceQuerySignature).getDimFilter(),
          true
      );
    } else {
      havingSpec = null;
    }
    final List<PostAggregator> postAggregators = new ArrayList<>(grouping.getPostAggregators());
    if (sortProject != null) {
      postAggregators.addAll(sortProject.getPostAggregators());
    }

    return new GroupByQuery(
        dataSource,
        filtration.getQuerySegmentSpec(),
        getVirtualColumns(true),
        filtration.getDimFilter(),
        Granularities.ALL,
        grouping.getDimensionSpecs(),
        grouping.getAggregatorFactories(),
        postAggregators,
        havingSpec,
        limitSpec,
        null,
        ImmutableSortedMap.copyOf(plannerContext.getQueryContext())
    );
  }

  /**
   * Return this query as a Scan query, or null if this query is not compatible with Scan.
   *
   * @return query or null
   */
  @Nullable
  public ScanQuery toScanQuery()
  {
    if (grouping != null) {
      // Scan cannot GROUP BY.
      return null;
    }

    if (limitSpec != null && limitSpec.getColumns().size() > 0) {
      // Scan cannot ORDER BY.
      return null;
    }

    if (outputRowSignature.getRowOrder().isEmpty()) {
      // Should never do a scan query without any columns that we're interested in. This is probably a planner bug.
      throw new ISE("WTF?! Attempting to convert to Scan query without any columns?");
    }

    final Filtration filtration = Filtration.create(filter).optimize(sourceQuerySignature);

    // DefaultLimitSpec (which we use to "remember" limits) is int typed, and Integer.MAX_VALUE means "no limit".
    final long scanLimit = limitSpec == null || limitSpec.getLimit() == Integer.MAX_VALUE
                           ? 0L
                           : (long) limitSpec.getLimit();

    return new ScanQuery(
        dataSource,
        filtration.getQuerySegmentSpec(),
        selectProjection != null ? VirtualColumns.create(selectProjection.getVirtualColumns()) : VirtualColumns.EMPTY,
        ScanQuery.RESULT_FORMAT_COMPACTED_LIST,
        0,
        scanLimit,
        filtration.getDimFilter(),
        Ordering.natural().sortedCopy(ImmutableSet.copyOf(outputRowSignature.getRowOrder())),
        false,
        ImmutableSortedMap.copyOf(plannerContext.getQueryContext())
    );
  }

  /**
   * Return this query as a Select query, or null if this query is not compatible with Select.
   *
   * @return query or null
   */
  @Nullable
  public SelectQuery toSelectQuery()
  {
    if (grouping != null) {
      return null;
    }

    final Filtration filtration = Filtration.create(filter).optimize(sourceQuerySignature);
    final boolean descending;
    final int threshold;

    if (limitSpec != null) {
      // Safe to assume limitSpec has zero or one entry; DruidSelectSortRule wouldn't push in anything else.
      if (limitSpec.getColumns().size() == 0) {
        descending = false;
      } else if (limitSpec.getColumns().size() == 1) {
        final OrderByColumnSpec orderBy = Iterables.getOnlyElement(limitSpec.getColumns());
        if (!orderBy.getDimension().equals(ColumnHolder.TIME_COLUMN_NAME)) {
          // Select cannot handle sorting on anything other than __time.
          return null;
        }
        descending = orderBy.getDirection() == OrderByColumnSpec.Direction.DESCENDING;
      } else {
        // Select cannot handle sorting on more than one column.
        return null;
      }

      threshold = limitSpec.getLimit();
    } else {
      descending = false;
      threshold = 0;
    }

    // We need to ask for dummy columns to prevent Select from returning all of them.
    String dummyColumn = "dummy";
    while (sourceQuerySignature.getRowSignature().getColumnType(dummyColumn) != null
           || outputRowSignature.getRowOrder().contains(dummyColumn)) {
      dummyColumn = dummyColumn + "_";
    }

    final List<String> metrics = new ArrayList<>();

    if (selectProjection != null) {
      metrics.addAll(selectProjection.getDirectColumns());
      metrics.addAll(selectProjection.getVirtualColumns()
                                     .stream()
                                     .map(VirtualColumn::getOutputName)
                                     .collect(Collectors.toList()));
    } else {
      // No projection, rowOrder should reference direct columns.
      metrics.addAll(outputRowSignature.getRowOrder());
    }

    if (metrics.isEmpty()) {
      metrics.add(dummyColumn);
    }

    // Not used for actual queries (will be replaced by QueryMaker) but the threshold is important for the planner.
    final PagingSpec pagingSpec = new PagingSpec(null, threshold);

    return new SelectQuery(
        dataSource,
        filtration.getQuerySegmentSpec(),
        descending,
        filtration.getDimFilter(),
        Granularities.ALL,
        ImmutableList.of(new DefaultDimensionSpec(dummyColumn, dummyColumn)),
        metrics.stream().sorted().distinct().collect(Collectors.toList()),
        getVirtualColumns(true),
        pagingSpec,
        ImmutableSortedMap.copyOf(plannerContext.getQueryContext())
    );
  }
}
