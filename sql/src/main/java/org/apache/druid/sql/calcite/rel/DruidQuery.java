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
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
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
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.having.DimFilterHavingSpec;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.NoopLimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.ordering.StringComparator;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.topn.DimensionTopNMetricSpec;
import org.apache.druid.query.topn.InvertedTopNMetricSpec;
import org.apache.druid.query.topn.NumericTopNMetricSpec;
import org.apache.druid.query.topn.TopNMetricSpec;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.DimensionExpression;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rule.GroupByRules;
import org.apache.druid.sql.calcite.table.RowSignatures;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
  private final Projection selectProjection;

  @Nullable
  private final Grouping grouping;

  @Nullable
  private final Sorting sorting;

  private final Query query;
  private final RowSignature outputRowSignature;
  private final RelDataType outputRowType;
  private final VirtualColumnRegistry virtualColumnRegistry;

  private DruidQuery(
      final DataSource dataSource,
      final PlannerContext plannerContext,
      @Nullable final DimFilter filter,
      @Nullable final Projection selectProjection,
      @Nullable final Grouping grouping,
      @Nullable final Sorting sorting,
      final RowSignature sourceRowSignature,
      final RelDataType outputRowType,
      final VirtualColumnRegistry virtualColumnRegistry
  )
  {
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.plannerContext = Preconditions.checkNotNull(plannerContext, "plannerContext");
    this.filter = filter;
    this.selectProjection = selectProjection;
    this.grouping = grouping;
    this.sorting = sorting;
    this.outputRowSignature = computeOutputRowSignature(sourceRowSignature, selectProjection, grouping, sorting);
    this.outputRowType = Preconditions.checkNotNull(outputRowType, "outputRowType");
    this.virtualColumnRegistry = Preconditions.checkNotNull(virtualColumnRegistry, "virtualColumnRegistry");
    this.query = computeQuery();
  }

  public static DruidQuery fromPartialQuery(
      final PartialDruidQuery partialQuery,
      final DataSource dataSource,
      final RowSignature sourceRowSignature,
      final PlannerContext plannerContext,
      final RexBuilder rexBuilder,
      final boolean finalizeAggregations
  )
  {
    final RelDataType outputRowType = partialQuery.leafRel().getRowType();
    final VirtualColumnRegistry virtualColumnRegistry = VirtualColumnRegistry.create(sourceRowSignature);

    // Now the fun begins.
    final DimFilter filter;
    final Projection selectProjection;
    final Grouping grouping;
    final Sorting sorting;

    if (partialQuery.getWhereFilter() != null) {
      filter = Preconditions.checkNotNull(
          computeWhereFilter(
              partialQuery,
              plannerContext,
              sourceRowSignature,
              virtualColumnRegistry
          )
      );
    } else {
      filter = null;
    }

    // Only compute "selectProjection" if this is a non-aggregating query. (For aggregating queries, "grouping" will
    // reflect select-project from partialQuery on its own.)
    if (partialQuery.getSelectProject() != null && partialQuery.getAggregate() == null) {
      selectProjection = Preconditions.checkNotNull(
          computeSelectProjection(
              partialQuery,
              plannerContext,
              computeOutputRowSignature(sourceRowSignature, null, null, null),
              virtualColumnRegistry
          )
      );
    } else {
      selectProjection = null;
    }

    if (partialQuery.getAggregate() != null) {
      grouping = Preconditions.checkNotNull(
          computeGrouping(
              partialQuery,
              plannerContext,
              computeOutputRowSignature(sourceRowSignature, null, null, null),
              virtualColumnRegistry,
              rexBuilder,
              finalizeAggregations
          )
      );
    } else {
      grouping = null;
    }

    if (partialQuery.getSort() != null) {
      sorting = Preconditions.checkNotNull(
          computeSorting(
              partialQuery,
              plannerContext,
              computeOutputRowSignature(sourceRowSignature, selectProjection, grouping, null),
              // When sorting follows grouping, virtual columns cannot be used
              partialQuery.getAggregate() != null ? null : virtualColumnRegistry
          )
      );
    } else {
      sorting = null;
    }

    return new DruidQuery(
        dataSource,
        plannerContext,
        filter,
        selectProjection,
        grouping,
        sorting,
        sourceRowSignature,
        outputRowType,
        virtualColumnRegistry
    );
  }

  @Nonnull
  private static DimFilter computeWhereFilter(
      final PartialDruidQuery partialQuery,
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final VirtualColumnRegistry virtualColumnRegistry
  )
  {
    return getDimFilter(plannerContext, rowSignature, virtualColumnRegistry, partialQuery.getWhereFilter());
  }

  @Nullable
  private static DimFilter computeHavingFilter(
      final PartialDruidQuery partialQuery,
      final PlannerContext plannerContext,
      final RowSignature aggregateSignature
  )
  {
    final Filter havingFilter = partialQuery.getHavingFilter();

    if (havingFilter == null) {
      return null;
    }

    // null virtualColumnRegistry, since virtual columns cannot be referenced by "having" filters.
    return getDimFilter(plannerContext, aggregateSignature, null, havingFilter);
  }

  @Nonnull
  private static DimFilter getDimFilter(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      @Nullable final VirtualColumnRegistry virtualColumnRegistry,
      final Filter filter
  )
  {
    final RexNode condition = filter.getCondition();
    final DimFilter dimFilter = Expressions.toFilter(
        plannerContext,
        rowSignature,
        virtualColumnRegistry,
        condition
    );
    if (dimFilter == null) {
      throw new CannotBuildQueryException(filter, condition);
    } else {
      return dimFilter;
    }
  }

  @Nonnull
  private static Projection computeSelectProjection(
      final PartialDruidQuery partialQuery,
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final VirtualColumnRegistry virtualColumnRegistry
  )
  {
    final Project project = Preconditions.checkNotNull(partialQuery.getSelectProject(), "selectProject");

    if (partialQuery.getAggregate() != null) {
      throw new ISE("Cannot have both 'selectProject' and 'aggregate', how can this be?");
    } else {
      return Projection.preAggregation(project, plannerContext, rowSignature, virtualColumnRegistry);
    }
  }

  @Nonnull
  private static Grouping computeGrouping(
      final PartialDruidQuery partialQuery,
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final VirtualColumnRegistry virtualColumnRegistry,
      final RexBuilder rexBuilder,
      final boolean finalizeAggregations
  )
  {
    final Aggregate aggregate = Preconditions.checkNotNull(partialQuery.getAggregate(), "aggregate");
    final Project aggregateProject = partialQuery.getAggregateProject();

    final List<DimensionExpression> dimensions = computeDimensions(
        partialQuery,
        plannerContext,
        rowSignature,
        virtualColumnRegistry
    );

    final Subtotals subtotals = computeSubtotals(
        partialQuery,
        rowSignature
    );

    final List<Aggregation> aggregations = computeAggregations(
        partialQuery,
        plannerContext,
        rowSignature,
        virtualColumnRegistry,
        rexBuilder,
        finalizeAggregations
    );

    final RowSignature aggregateRowSignature = RowSignatures.fromRelDataType(
        ImmutableList.copyOf(
            Iterators.concat(
                dimensions.stream().map(DimensionExpression::getOutputName).iterator(),
                aggregations.stream().map(Aggregation::getOutputName).iterator()
            )
        ),
        aggregate.getRowType()
    );

    final DimFilter havingFilter = computeHavingFilter(
        partialQuery,
        plannerContext,
        aggregateRowSignature
    );

    final Grouping grouping = Grouping.create(dimensions, subtotals, aggregations, havingFilter, aggregateRowSignature);

    if (aggregateProject == null) {
      return grouping;
    } else {
      return grouping.applyProject(plannerContext, aggregateProject);
    }
  }

  /**
   * Returns dimensions corresponding to {@code aggregate.getGroupSet()}, in the same order.
   *
   * @param partialQuery          partial query
   * @param plannerContext        planner context
   * @param rowSignature          source row signature
   * @param virtualColumnRegistry re-usable virtual column references
   *
   * @return dimensions
   *
   * @throws CannotBuildQueryException if dimensions cannot be computed
   */
  private static List<DimensionExpression> computeDimensions(
      final PartialDruidQuery partialQuery,
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final VirtualColumnRegistry virtualColumnRegistry
  )
  {
    final Aggregate aggregate = Preconditions.checkNotNull(partialQuery.getAggregate());
    final List<DimensionExpression> dimensions = new ArrayList<>();
    final String outputNamePrefix = Calcites.findUnusedPrefixForDigits("d", rowSignature.getColumnNames());

    int outputNameCounter = 0;

    for (int i : aggregate.getGroupSet()) {
      // Dimension might need to create virtual columns. Avoid giving it a name that would lead to colliding columns.
      final RexNode rexNode = Expressions.fromFieldAccess(
          rowSignature,
          partialQuery.getSelectProject(),
          i
      );
      final DruidExpression druidExpression = Expressions.toDruidExpression(plannerContext, rowSignature, rexNode);
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


      final String dimOutputName = outputNamePrefix + outputNameCounter++;
      if (!druidExpression.isSimpleExtraction()) {
        virtualColumn = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(
            plannerContext,
            druidExpression,
            sqlTypeName
        );
        dimensions.add(DimensionExpression.ofVirtualColumn(
            virtualColumn.getOutputName(),
            dimOutputName,
            druidExpression,
            outputType
        ));
      } else {
        dimensions.add(DimensionExpression.ofSimpleColumn(dimOutputName, druidExpression, outputType));
      }
    }

    return dimensions;
  }

  /**
   * Builds a {@link Subtotals} object based on {@link Aggregate#getGroupSets()}.
   */
  private static Subtotals computeSubtotals(
      final PartialDruidQuery partialQuery,
      final RowSignature rowSignature
  )
  {
    final Aggregate aggregate = partialQuery.getAggregate();

    // dimBitMapping maps from input field position to group set position (dimension number).
    final int[] dimBitMapping;
    if (partialQuery.getSelectProject() != null) {
      dimBitMapping = new int[partialQuery.getSelectProject().getRowType().getFieldCount()];
    } else {
      dimBitMapping = new int[rowSignature.size()];
    }

    int i = 0;
    for (int dimBit : aggregate.getGroupSet()) {
      dimBitMapping[dimBit] = i++;
    }

    // Use dimBitMapping to remap groupSets (which is input-field-position based) into subtotals (which is
    // dimension-list-position based).
    final List<IntList> subtotals = new ArrayList<>();
    for (ImmutableBitSet groupSet : aggregate.getGroupSets()) {
      final IntList subtotal = new IntArrayList();
      for (int dimBit : groupSet) {
        subtotal.add(dimBitMapping[dimBit]);
      }

      subtotals.add(subtotal);
    }

    return new Subtotals(subtotals);
  }

  /**
   * Returns aggregations corresponding to {@code aggregate.getAggCallList()}, in the same order.
   *
   * @param partialQuery          partial query
   * @param plannerContext        planner context
   * @param rowSignature          source row signature
   * @param virtualColumnRegistry re-usable virtual column references
   * @param rexBuilder            calcite RexBuilder
   * @param finalizeAggregations  true if this query should include explicit finalization for all of its
   *                              aggregators, where required. Useful for subqueries where Druid's native query layer
   *                              does not do this automatically.
   *
   * @return aggregations
   *
   * @throws CannotBuildQueryException if dimensions cannot be computed
   */
  private static List<Aggregation> computeAggregations(
      final PartialDruidQuery partialQuery,
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final VirtualColumnRegistry virtualColumnRegistry,
      final RexBuilder rexBuilder,
      final boolean finalizeAggregations
  )
  {
    final Aggregate aggregate = Preconditions.checkNotNull(partialQuery.getAggregate());
    final List<Aggregation> aggregations = new ArrayList<>();
    final String outputNamePrefix = Calcites.findUnusedPrefixForDigits("a", rowSignature.getColumnNames());

    for (int i = 0; i < aggregate.getAggCallList().size(); i++) {
      final String aggName = outputNamePrefix + i;
      final AggregateCall aggCall = aggregate.getAggCallList().get(i);
      final Aggregation aggregation = GroupByRules.translateAggregateCall(
          plannerContext,
          rowSignature,
          virtualColumnRegistry,
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

  @Nonnull
  private static Sorting computeSorting(
      final PartialDruidQuery partialQuery,
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      @Nullable final VirtualColumnRegistry virtualColumnRegistry
  )
  {
    final Sort sort = Preconditions.checkNotNull(partialQuery.getSort(), "sort");
    final Project sortProject = partialQuery.getSortProject();

    // Extract limit.
    final Long limit = sort.fetch != null ? ((Number) RexLiteral.value(sort.fetch)).longValue() : null;
    final List<OrderByColumnSpec> orderBys = new ArrayList<>(sort.getChildExps().size());

    if (sort.offset != null) {
      // Druid cannot currently handle LIMIT with OFFSET.
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
        throw new ISE("Don't know what to do with direction[%s]", collation.getDirection());
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
        final String fieldName = rowSignature.getColumnName(ref.getIndex());
        orderBys.add(new OrderByColumnSpec(fieldName, direction, comparator));
      } else {
        // We don't support sorting by anything other than refs which actually appear in the query result.
        throw new CannotBuildQueryException(sort, sortExpression);
      }
    }

    // Extract any post-sort Projection.
    final Projection projection;

    if (sortProject == null) {
      projection = null;
    } else if (partialQuery.getAggregate() == null) {
      if (virtualColumnRegistry == null) {
        throw new ISE("Must provide 'virtualColumnRegistry' for pre-aggregation Projection!");
      }

      projection = Projection.preAggregation(sortProject, plannerContext, rowSignature, virtualColumnRegistry);
    } else {
      projection = Projection.postAggregation(sortProject, plannerContext, rowSignature, "s");
    }

    return Sorting.create(orderBys, limit, projection);
  }

  /**
   * Return the {@link RowSignature} corresponding to the output of a query with the given parameters.
   */
  private static RowSignature computeOutputRowSignature(
      final RowSignature sourceRowSignature,
      @Nullable final Projection selectProjection,
      @Nullable final Grouping grouping,
      @Nullable final Sorting sorting
  )
  {
    if (sorting != null && sorting.getProjection() != null) {
      return sorting.getProjection().getOutputRowSignature();
    } else if (grouping != null) {
      // Sanity check: cannot have both "grouping" and "selectProjection".
      Preconditions.checkState(selectProjection == null, "Cannot have both 'grouping' and 'selectProjection'");
      return grouping.getOutputRowSignature();
    } else if (selectProjection != null) {
      return selectProjection.getOutputRowSignature();
    } else {
      return sourceRowSignature;
    }
  }

  private VirtualColumns getVirtualColumns(final boolean includeDimensions)
  {
    // 'sourceRowSignature' could provide a list of all defined virtual columns while constructing a query, but we
    // still want to collect the set of VirtualColumns this way to ensure we only add what is still being used after
    // the various transforms and optimizations
    Set<VirtualColumn> virtualColumns = new HashSet<>();

    // we always want to add any virtual columns used by the query level DimFilter
    if (filter != null) {
      for (String columnName : filter.getRequiredColumns()) {
        if (virtualColumnRegistry.isVirtualColumnDefined(columnName)) {
          virtualColumns.add(virtualColumnRegistry.getVirtualColumn(columnName));
        }
      }
    }

    if (selectProjection != null) {
      virtualColumns.addAll(selectProjection.getVirtualColumns());
    }

    if (grouping != null) {
      if (includeDimensions) {
        for (DimensionExpression expression : grouping.getDimensions()) {
          if (virtualColumnRegistry.isVirtualColumnDefined(expression.getVirtualColumn())) {
            virtualColumns.add(virtualColumnRegistry.getVirtualColumn(expression.getVirtualColumn()));
          }
        }
      }

      for (Aggregation aggregation : grouping.getAggregations()) {
        virtualColumns.addAll(aggregation.getVirtualColumns());
      }
    }

    if (sorting != null && sorting.getProjection() != null && grouping == null) {
      // Sorting without grouping means we might have some post-sort Projection virtual columns.
      virtualColumns.addAll(sorting.getProjection().getVirtualColumns());
    }

    // sort for predictable output
    List<VirtualColumn> columns = new ArrayList<>(virtualColumns);
    columns.sort(Comparator.comparing(VirtualColumn::getOutputName));
    return VirtualColumns.create(columns);
  }

  public DataSource getDataSource()
  {
    return dataSource;
  }

  @Nullable
  public Grouping getGrouping()
  {
    return grouping;
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
   * {@link TimeseriesQuery}, {@link GroupByQuery}, {@link ScanQuery}
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
    if (grouping == null
        || grouping.getSubtotals().hasEffect(grouping.getDimensionSpecs())
        || grouping.getHavingFilter() != null) {
      return null;
    }

    final Granularity queryGranularity;
    final boolean descending;
    int timeseriesLimit = 0;
    final Map<String, Object> theContext = new HashMap<>();
    theContext.put("skipEmptyBuckets", true);
    theContext.putAll(plannerContext.getQueryContext());
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
      theContext.put(
          TimeseriesQuery.CTX_TIMESTAMP_RESULT_FIELD,
          Iterables.getOnlyElement(grouping.getDimensions()).toDimensionSpec().getOutputName()
      );
      if (sorting != null) {
        // If there is sorting, set timeseriesLimit to given value if less than Integer.Max_VALUE
        if (sorting.isLimited()) {
          timeseriesLimit = Ints.checkedCast(sorting.getLimit());
        }

        switch (sorting.getSortKind(dimensionExpression.getOutputName())) {
          case UNORDERED:
          case TIME_ASCENDING:
            descending = false;
            break;
          case TIME_DESCENDING:
            descending = true;
            break;
          default:
            // Sorting on a metric, maybe. Timeseries cannot handle.
            return null;
        }
      } else {
        // No limitSpec.
        descending = false;
      }
    } else {
      // More than one dimension, timeseries cannot handle.
      return null;
    }

    final Filtration filtration = Filtration.create(filter).optimize(virtualColumnRegistry.getFullRowSignature());

    final List<PostAggregator> postAggregators = new ArrayList<>(grouping.getPostAggregators());
    if (sorting != null && sorting.getProjection() != null) {
      postAggregators.addAll(sorting.getProjection().getPostAggregators());
    }

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
    // Must have GROUP BY one column, no GROUPING SETS, ORDER BY ≤ 1 column, limit less than maxTopNLimit, no HAVING.
    final boolean topNOk = grouping != null
                           && grouping.getDimensions().size() == 1
                           && !grouping.getSubtotals().hasEffect(grouping.getDimensionSpecs())
                           && sorting != null
                           && (sorting.getOrderBys().size() <= 1
                               && sorting.isLimited() && sorting.getLimit() <= plannerContext.getPlannerConfig()
                                                                                             .getMaxTopNLimit())
                           && grouping.getHavingFilter() == null;

    if (!topNOk) {
      return null;
    }

    final DimensionSpec dimensionSpec = Iterables.getOnlyElement(grouping.getDimensions()).toDimensionSpec();
    final OrderByColumnSpec limitColumn;
    if (sorting.getOrderBys().isEmpty()) {
      limitColumn = new OrderByColumnSpec(
          dimensionSpec.getOutputName(),
          OrderByColumnSpec.Direction.ASCENDING,
          Calcites.getStringComparatorForValueType(dimensionSpec.getOutputType())
      );
    } else {
      limitColumn = Iterables.getOnlyElement(sorting.getOrderBys());
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

    final Filtration filtration = Filtration.create(filter).optimize(virtualColumnRegistry.getFullRowSignature());

    final List<PostAggregator> postAggregators = new ArrayList<>(grouping.getPostAggregators());
    if (sorting.getProjection() != null) {
      postAggregators.addAll(sorting.getProjection().getPostAggregators());
    }

    return new TopNQuery(
        dataSource,
        getVirtualColumns(true),
        dimensionSpec,
        topNMetricSpec,
        Ints.checkedCast(sorting.getLimit()),
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

    final Filtration filtration = Filtration.create(filter).optimize(virtualColumnRegistry.getFullRowSignature());

    final DimFilterHavingSpec havingSpec;
    if (grouping.getHavingFilter() != null) {
      havingSpec = new DimFilterHavingSpec(
          Filtration.create(grouping.getHavingFilter())
                    .optimizeFilterOnly(grouping.getOutputRowSignature())
                    .getDimFilter(),
          true
      );
    } else {
      havingSpec = null;
    }
    final List<PostAggregator> postAggregators = new ArrayList<>(grouping.getPostAggregators());
    if (sorting != null && sorting.getProjection() != null) {
      postAggregators.addAll(sorting.getProjection().getPostAggregators());
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
        sorting != null
        ? new DefaultLimitSpec(
            sorting.getOrderBys(),
            sorting.isLimited() ? Ints.checkedCast(sorting.getLimit()) : null
        )
        : NoopLimitSpec.instance(),
        grouping.getSubtotals().toSubtotalsSpec(grouping.getDimensionSpecs()),
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

    if (outputRowSignature.size() == 0) {
      // Should never do a scan query without any columns that we're interested in. This is probably a planner bug.
      throw new ISE("Cannot convert to Scan query without any columns.");
    }

    final Filtration filtration = Filtration.create(filter).optimize(virtualColumnRegistry.getFullRowSignature());
    final ScanQuery.Order order;
    long scanLimit = 0L;

    if (sorting != null) {
      if (sorting.isLimited()) {
        scanLimit = sorting.getLimit();
      }

      final Sorting.SortKind sortKind = sorting.getSortKind(ColumnHolder.TIME_COLUMN_NAME);

      if (sortKind == Sorting.SortKind.UNORDERED) {
        order = ScanQuery.Order.NONE;
      } else if (sortKind == Sorting.SortKind.TIME_ASCENDING) {
        order = ScanQuery.Order.ASCENDING;
      } else if (sortKind == Sorting.SortKind.TIME_DESCENDING) {
        order = ScanQuery.Order.DESCENDING;
      } else {
        assert sortKind == Sorting.SortKind.NON_TIME;

        // Scan cannot ORDER BY non-time columns.
        return null;
      }
    } else {
      order = ScanQuery.Order.NONE;
    }

    // Compute the list of columns to select.
    final Set<String> columns = new HashSet<>(outputRowSignature.getColumnNames());

    if (order != ScanQuery.Order.NONE) {
      columns.add(ColumnHolder.TIME_COLUMN_NAME);
    }

    return new ScanQuery(
        dataSource,
        filtration.getQuerySegmentSpec(),
        getVirtualColumns(true),
        ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST,
        0,
        scanLimit,
        order,
        filtration.getDimFilter(),
        Ordering.natural().sortedCopy(columns),
        false,
        ImmutableSortedMap.copyOf(plannerContext.getQueryContext())
    );
  }
}
