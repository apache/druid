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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.FilteredDataSource;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnnestDataSource;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.LongMaxAggregatorFactory;
import org.apache.druid.query.aggregation.LongMinAggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.SimpleLongAggregatorFactory;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.having.DimFilterHavingSpec;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.operator.ColumnWithDirection;
import org.apache.druid.query.operator.ColumnWithDirection.Direction;
import org.apache.druid.query.operator.NaiveSortOperatorFactory;
import org.apache.druid.query.operator.OperatorFactory;
import org.apache.druid.query.operator.ScanOperatorFactory;
import org.apache.druid.query.operator.WindowOperatorQuery;
import org.apache.druid.query.ordering.StringComparator;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.LegacySegmentSpec;
import org.apache.druid.query.timeboundary.TimeBoundaryQuery;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.topn.DimensionTopNMetricSpec;
import org.apache.druid.query.topn.InvertedTopNMetricSpec;
import org.apache.druid.query.topn.NumericTopNMetricSpec;
import org.apache.druid.query.topn.TopNMetricSpec;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.segment.RowBasedStorageAdapter;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.Types;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.DimensionExpression;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.OffsetLimit;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rule.GroupByRules;
import org.apache.druid.sql.calcite.run.EngineFeature;
import org.apache.druid.sql.calcite.table.RowSignatures;
import org.joda.time.Interval;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * A fully formed Druid query, built from a {@link PartialDruidQuery}. The work to develop this query is done
 * during construction, which may throw {@link CannotBuildQueryException}.
 */
public class DruidQuery
{
  /**
   * Native query context key that is set when {@link EngineFeature#SCAN_NEEDS_SIGNATURE}.
   *
   * {@link Deprecated} Instead of the context value {@link ScanQuery#getRowSignature()} can be used.
   */
  @Deprecated
  public static final String CTX_SCAN_SIGNATURE = "scanSignature";

  /**
   * Maximum number of time-granular buckets that we allow for non-Druid tables.
   * <p>
   * Used by {@link #canUseQueryGranularity}.
   */
  private static final int MAX_TIME_GRAINS_NON_DRUID_TABLE = 100000;

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

  @Nullable
  private final Windowing windowing;

  private final Query<?> query;
  private final RowSignature outputRowSignature;
  private final RelDataType outputRowType;
  private final VirtualColumnRegistry virtualColumnRegistry;
  private final RowSignature sourceRowSignature;

  private DruidQuery(
      final DataSource dataSource,
      final PlannerContext plannerContext,
      @Nullable final DimFilter filter,
      @Nullable final Projection selectProjection,
      @Nullable final Grouping grouping,
      @Nullable final Sorting sorting,
      @Nullable final Windowing windowing,
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
    this.windowing = windowing;
    this.sourceRowSignature = sourceRowSignature;

    this.outputRowSignature = computeOutputRowSignature(
        sourceRowSignature,
        selectProjection,
        grouping,
        sorting,
        windowing
    );
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
      final boolean finalizeAggregations,
      @Nullable VirtualColumnRegistry virtualColumnRegistry
  )
  {
    final RelDataType outputRowType = partialQuery.leafRel().getRowType();
    if (virtualColumnRegistry == null) {
      virtualColumnRegistry = VirtualColumnRegistry.create(
          sourceRowSignature,
          plannerContext.getExpressionParser(),
          plannerContext.getPlannerConfig().isForceExpressionVirtualColumns()
      );
    }

    // Now the fun begins.
    final DimFilter filter;
    final Projection selectProjection;
    final Grouping grouping;
    final Sorting sorting;
    final Windowing windowing;

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
              computeOutputRowSignature(sourceRowSignature, null, null, null, null),
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
              computeOutputRowSignature(sourceRowSignature, null, null, null, null),
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
              computeOutputRowSignature(sourceRowSignature, selectProjection, grouping, null, null),
              // When sorting follows grouping, virtual columns cannot be used
              partialQuery.getAggregate() != null ? null : virtualColumnRegistry
          )
      );
    } else {
      sorting = null;
    }

    if (partialQuery.getWindow() != null) {
      if (plannerContext.featureAvailable(EngineFeature.WINDOW_FUNCTIONS)) {
        windowing = Preconditions.checkNotNull(
            Windowing.fromCalciteStuff(
                partialQuery,
                plannerContext,
                sourceRowSignature, // Plans immediately after Scan, so safe to use the row signature from scan
                rexBuilder,
                virtualColumnRegistry
            )
        );
      } else {
        plannerContext.setPlanningError("Windowing not supported");
        throw new CannotBuildQueryException("Windowing not supported");
      }
    } else {
      windowing = null;
    }

    return new DruidQuery(
        dataSource,
        plannerContext,
        filter,
        selectProjection,
        grouping,
        sorting,
        windowing,
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
  public static DimFilter getDimFilter(
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
        virtualColumnRegistry,
        rexBuilder.getTypeFactory()
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
   * @param typeFactory           factory for SQL types
   *
   * @return dimensions
   *
   * @throws CannotBuildQueryException if dimensions cannot be computed
   */
  private static List<DimensionExpression> computeDimensions(
      final PartialDruidQuery partialQuery,
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final VirtualColumnRegistry virtualColumnRegistry,
      final RelDataTypeFactory typeFactory
  )
  {
    final Aggregate aggregate = Preconditions.checkNotNull(partialQuery.getAggregate());
    final List<DimensionExpression> dimensions = new ArrayList<>();
    final String outputNamePrefix = Calcites.findUnusedPrefixForDigits("d", rowSignature.getColumnNames());

    int outputNameCounter = 0;

    for (int i : aggregate.getGroupSet()) {
      // Dimension might need to create virtual columns. Avoid giving it a name that would lead to colliding columns.
      final RexNode rexNode = Expressions.fromFieldAccess(
          typeFactory,
          rowSignature,
          partialQuery.getSelectProject(),
          i
      );
      final DruidExpression druidExpression = Expressions.toDruidExpression(plannerContext, rowSignature, rexNode);
      if (druidExpression == null) {
        throw new CannotBuildQueryException(aggregate, rexNode);
      }

      final RelDataType dataType = rexNode.getType();
      final ColumnType outputType = Calcites.getColumnTypeForRelDataType(dataType);
      if (Types.isNullOr(outputType, ValueType.COMPLEX)) {
        // Can't group on unknown or COMPLEX types.
        plannerContext.setPlanningError(
            "SQL requires a group-by on a column of type %s that is unsupported.",
            outputType
        );
        throw new CannotBuildQueryException(aggregate, rexNode);
      }

      final String dimOutputName = outputNamePrefix + outputNameCounter++;
      if (!druidExpression.isSimpleExtraction()) {
        final String virtualColumn = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(
            druidExpression,
            dataType
        );
        dimensions.add(DimensionExpression.ofVirtualColumn(
            virtualColumn,
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
          InputAccessor.buildFor(
              rexBuilder,
              rowSignature,
              partialQuery.getSelectProject(),
              null),
          aggregations,
          aggName,
          aggCall,
          finalizeAggregations
      );

      if (aggregation == null) {
        if (null == plannerContext.getPlanningError()) {
          plannerContext.setPlanningError("Aggregation [%s] is not supported", aggCall);
        }
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

    // Extract limit and offset.
    final OffsetLimit offsetLimit = OffsetLimit.fromSort(sort);

    // Extract orderBy column specs.
    final List<OrderByColumnSpec> orderBys = new ArrayList<>(sort.getSortExps().size());
    for (int sortKey = 0; sortKey < sort.getSortExps().size(); sortKey++) {
      final RexNode sortExpression = sort.getSortExps().get(sortKey);
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

      comparator = Calcites.getStringComparatorForRelDataType(sortExpression.getType());

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

    return Sorting.create(orderBys, offsetLimit, projection);
  }

  /**
   * Return the {@link RowSignature} corresponding to the output of a query with the given parameters.
   */
  private static RowSignature computeOutputRowSignature(
      final RowSignature sourceRowSignature,
      @Nullable final Projection selectProjection,
      @Nullable final Grouping grouping,
      @Nullable final Sorting sorting,
      @Nullable final Windowing windowing
  )
  {
    if (windowing != null) {
      return windowing.getSignature();
    } else if (sorting != null && sorting.getProjection() != null) {
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

  VirtualColumns getVirtualColumns(final boolean includeDimensions)
  {
    // 'sourceRowSignature' could provide a list of all defined virtual columns while constructing a query, but we
    // still want to collect the set of VirtualColumns this way to ensure we only add what is still being used after
    // the various transforms and optimizations
    Set<VirtualColumn> virtualColumns = new HashSet<>();


    // rewrite any "specialized" virtual column expressions as top level virtual columns so that their native
    // implementation can be used instead of being composed as part of some expression tree in an expresson virtual
    // column
    Set<String> specialized = new HashSet<>();
    final boolean forceExpressionVirtualColumns =
        plannerContext.getPlannerConfig().isForceExpressionVirtualColumns();
    virtualColumnRegistry.visitAllSubExpressions((expression) -> {
      if (!forceExpressionVirtualColumns && expression.getType() == DruidExpression.NodeType.SPECIALIZED) {
        // add the expression to the top level of the registry as a standalone virtual column
        final String name = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(
            expression,
            expression.getDruidType()
        );
        specialized.add(name);
        // replace with an identifier expression of the new virtual column name
        return DruidExpression.ofColumn(expression.getDruidType(), name);
      } else {
        // do nothing
        return expression;
      }
    });

    // we always want to add any virtual columns used by the query level DimFilter
    if (filter != null) {
      for (String columnName : filter.getRequiredColumns()) {
        if (virtualColumnRegistry.isVirtualColumnDefined(columnName)) {
          virtualColumns.add(virtualColumnRegistry.getVirtualColumn(columnName));
        }
      }
    }

    if (selectProjection != null) {
      for (String columnName : selectProjection.getVirtualColumns()) {
        if (virtualColumnRegistry.isVirtualColumnDefined(columnName)) {
          virtualColumns.add(virtualColumnRegistry.getVirtualColumn(columnName));
        }
      }
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
        virtualColumns.addAll(virtualColumnRegistry.getAllVirtualColumns(aggregation.getRequiredColumns()));
      }
    }

    if (sorting != null && sorting.getProjection() != null && grouping == null) {
      // Sorting without grouping means we might have some post-sort Projection virtual columns.

      for (String columnName : sorting.getProjection().getVirtualColumns()) {
        if (virtualColumnRegistry.isVirtualColumnDefined(columnName)) {
          virtualColumns.add(virtualColumnRegistry.getVirtualColumn(columnName));
        }
      }
    }

    if (dataSource instanceof JoinDataSource) {
      for (String expression : ((JoinDataSource) dataSource).getVirtualColumnCandidates()) {
        if (virtualColumnRegistry.isVirtualColumnDefined(expression)) {
          virtualColumns.add(virtualColumnRegistry.getVirtualColumn(expression));
        }
      }
    }

    for (String columnName : specialized) {
      if (virtualColumnRegistry.isVirtualColumnDefined(columnName)) {
        virtualColumns.add(virtualColumnRegistry.getVirtualColumn(columnName));
      }
    }

    // sort for predictable output
    List<VirtualColumn> columns = new ArrayList<>(virtualColumns);
    columns.sort(Comparator.comparing(VirtualColumn::getOutputName));
    return VirtualColumns.create(columns);
  }

  public static List<DimFilter> getAllFiltersUnderDataSource(DataSource d, List<DimFilter> dimFilterList)
  {
    if (d instanceof FilteredDataSource) {
      dimFilterList.add(((FilteredDataSource) d).getFilter());
    }
    for (DataSource ds : d.getChildren()) {
      dimFilterList.addAll(getAllFiltersUnderDataSource(ds, dimFilterList));
    }
    return dimFilterList;
  }

  /**
   * Returns a pair of DataSource and Filtration object created on the query filter. In case the, data source is
   * a join datasource, the datasource may be altered and left filter of join datasource may
   * be rid of time filters.
   */
  @VisibleForTesting
  static Pair<DataSource, Filtration> getFiltration(
      DataSource dataSource,
      DimFilter filter,
      VirtualColumnRegistry virtualColumnRegistry,
      JoinableFactoryWrapper joinableFactoryWrapper
  )
  {
    if (dataSource instanceof UnnestDataSource) {
      // UnnestDataSource can have another unnest data source
      // join datasource, filtered data source, etc as base
      Pair<DataSource, Filtration> pair = getFiltration(
          ((UnnestDataSource) dataSource).getBase(),
          filter,
          virtualColumnRegistry,
          joinableFactoryWrapper
      );
      return Pair.of(dataSource, pair.rhs);
    } else if (!canUseIntervalFiltering(dataSource)) {
      return Pair.of(dataSource, toFiltration(filter, virtualColumnRegistry.getFullRowSignature(), false));
    } else if (dataSource instanceof FilteredDataSource) {
      // A filteredDS is created only inside the rel for Unnest, ensuring it only grabs the outermost filter
      // and, if possible, pushes it down inside the data source.
      // So a chain of Filter->Unnest->Filter is typically impossible when the query is done through SQL.
      // Also, Calcite has filter reduction rules that push filters deep into base data sources for better query planning.
      // A base table with a chain of filters is synonymous with a filteredDS.
      // We recursively find all filters under a filteredDS and then
      // 1) creating a filtration from the filteredDS's filters and
      // 2) Updating the interval of the outer filter with the intervals in step 1, and you'll see these 2 calls in the code
      List<DimFilter> dimFilterList = getAllFiltersUnderDataSource(dataSource, new ArrayList<>());
      final FilteredDataSource filteredDataSource = (FilteredDataSource) dataSource;
      // Defensive check as in the base of a filter cannot be another filter
      final DataSource baseOfFilterDataSource = filteredDataSource.getBase();
      if (baseOfFilterDataSource instanceof FilteredDataSource) {
        throw DruidException.defensive("Cannot create a filteredDataSource using another filteredDataSource as a base");
      }
      final boolean useIntervalFiltering = canUseIntervalFiltering(filteredDataSource);
      final Filtration baseFiltration = toFiltration(
          new AndDimFilter(dimFilterList),
          virtualColumnRegistry.getFullRowSignature(),
          useIntervalFiltering
      );
      // Adds the intervals from the filter of filtered data source to query filtration
      final Filtration queryFiltration = Filtration.create(filter, baseFiltration.getIntervals())
                                                   .optimize(virtualColumnRegistry.getFullRowSignature());
      return Pair.of(filteredDataSource, queryFiltration);
    } else if (dataSource instanceof JoinDataSource && ((JoinDataSource) dataSource).getLeftFilter() != null) {
      final JoinDataSource joinDataSource = (JoinDataSource) dataSource;

      // If the join is left or inner, we can pull the intervals up to the query. This is done
      // so that broker can prune the segments to query.
      final Filtration leftFiltration = Filtration.create(joinDataSource.getLeftFilter())
                                                  .optimize(virtualColumnRegistry.getFullRowSignature());

      // Adds the intervals from the join left filter to query filtration
      final Filtration queryFiltration = Filtration.create(filter, leftFiltration.getIntervals())
                                                   .optimize(virtualColumnRegistry.getFullRowSignature());

      final JoinDataSource newDataSource = JoinDataSource.create(
          joinDataSource.getLeft(),
          joinDataSource.getRight(),
          joinDataSource.getRightPrefix(),
          joinDataSource.getConditionAnalysis(),
          joinDataSource.getJoinType(),
          leftFiltration.getDimFilter(),
          joinableFactoryWrapper
      );
      return Pair.of(newDataSource, queryFiltration);
    } else {
      return Pair.of(dataSource, toFiltration(filter, virtualColumnRegistry.getFullRowSignature(), true));
    }
  }

  /**
   * Whether the given datasource can make use of "intervals" based filtering. The is true for anything based on
   * regular tables ({@link TableDataSource}).
   */
  private static boolean canUseIntervalFiltering(final DataSource dataSource)
  {
    return dataSource.getAnalysis().isTableBased();
  }

  private static Filtration toFiltration(
      final DimFilter filter,
      final RowSignature rowSignature,
      final boolean useIntervals
  )
  {
    final Filtration filtration = Filtration.create(filter);

    if (useIntervals) {
      return filtration.optimize(rowSignature);
    } else {
      return filtration.optimizeFilterOnly(rowSignature);
    }
  }

  /**
   * Whether the provided combination of dataSource, filtration, and queryGranularity is safe to use in queries.
   * <p>
   * Necessary because some combinations are unsafe, mainly because they would lead to the creation of too many
   * time-granular buckets during query processing.
   */
  private static boolean canUseQueryGranularity(
      final DataSource dataSource,
      final Filtration filtration,
      final Granularity queryGranularity
  )
  {
    if (Granularities.ALL.equals(queryGranularity)) {
      // Always OK: no storage adapter has problem with ALL.
      return true;
    }

    if (dataSource.getAnalysis().isConcreteAndTableBased()) {
      // Always OK: queries on concrete tables (regular Druid datasources) use segment-based storage adapters
      // (IncrementalIndex or QueryableIndex). These clip query interval to data interval, making wide query
      // intervals safer. They do not have special checks for granularity and interval safety.
      return true;
    }

    // Query is against something other than a regular Druid table. Apply additional checks, because we can't
    // count on interval-clipping to save us.

    for (final Interval filtrationInterval : filtration.getIntervals()) {
      // Query may be using RowBasedStorageAdapter. We don't know for sure, so check
      // RowBasedStorageAdapter#isQueryGranularityAllowed to be safe.
      if (!RowBasedStorageAdapter.isQueryGranularityAllowed(filtrationInterval, queryGranularity)) {
        return false;
      }

      // Validate the interval against MAX_TIME_GRAINS_NON_DRUID_TABLE.
      // Estimate based on the size of the first bucket, to avoid computing them all. (That's what we're
      // trying to avoid!)
      final Interval firstBucket = queryGranularity.bucket(filtrationInterval.getStart());
      final long estimatedNumBuckets = filtrationInterval.toDurationMillis() / firstBucket.toDurationMillis();
      if (estimatedNumBuckets > MAX_TIME_GRAINS_NON_DRUID_TABLE) {
        return false;
      }
    }

    return true;
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

  public Query<?> getQuery()
  {
    return query;
  }

  /**
   * Return this query as some kind of Druid query. The returned query will either be {@link TopNQuery},
   * {@link TimeseriesQuery}, {@link GroupByQuery}, {@link ScanQuery}
   *
   * @return Druid query
   */
  private Query<?> computeQuery()
  {
    if (dataSource instanceof QueryDataSource) {
      // If there is a subquery, then we prefer the outer query to be a groupBy if possible, since this potentially
      // enables more efficient execution. (The groupBy query toolchest can handle some subqueries by itself, without
      // requiring the Broker to inline results.)
      final GroupByQuery outerQuery = toGroupByQuery();

      if (outerQuery != null) {
        return outerQuery;
      }
    }

    final WindowOperatorQuery operatorQuery = toWindowQuery();
    if (operatorQuery != null) {
      return operatorQuery;
    }

    final TimeBoundaryQuery timeBoundaryQuery = toTimeBoundaryQuery();
    if (timeBoundaryQuery != null) {
      return timeBoundaryQuery;
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

    final ScanQuery scanQuery = toScanQuery(true);
    if (scanQuery != null) {
      return scanQuery;
    }

    final WindowOperatorQuery scanAndSortQuery = toScanAndSortQuery();
    if (scanAndSortQuery != null) {
      return scanAndSortQuery;
    }

    throw new CannotBuildQueryException("Cannot convert query parts into an actual query");
  }

  /**
   * Return this query as a TimeBoundary query, or null if this query is not compatible with Timeseries.
   *
   * @return a TimeBoundaryQuery if possible. null if it is not possible to construct one.
   */
  @Nullable
  private TimeBoundaryQuery toTimeBoundaryQuery()
  {
    if (!plannerContext.featureAvailable(EngineFeature.TIME_BOUNDARY_QUERY)
        || grouping == null
        || grouping.getSubtotals().hasEffect(grouping.getDimensionSpecs())
        || grouping.getHavingFilter() != null
        || selectProjection != null
        || windowing != null) {
      return null;
    }

    if (sorting != null && sorting.getOffsetLimit().hasOffset()) {
      // Timeboundary cannot handle offsets.
      return null;
    }

    if (grouping.getDimensions().isEmpty() &&
        grouping.getPostAggregators().isEmpty() &&
        grouping.getAggregatorFactories().size() == 1) { // currently only handles max(__time) or min(__time) not both
      boolean minTime;
      AggregatorFactory aggregatorFactory = Iterables.getOnlyElement(grouping.getAggregatorFactories());
      if (aggregatorFactory instanceof LongMaxAggregatorFactory ||
          aggregatorFactory instanceof LongMinAggregatorFactory) {
        SimpleLongAggregatorFactory minMaxFactory = (SimpleLongAggregatorFactory) aggregatorFactory;
        String fieldName = minMaxFactory.getFieldName();
        if (fieldName == null ||
            !fieldName.equals(ColumnHolder.TIME_COLUMN_NAME) ||
            (minMaxFactory.getExpression() != null && !minMaxFactory.getExpression().isEmpty())) {
          return null;
        }
        minTime = aggregatorFactory instanceof LongMinAggregatorFactory;
      } else {
        return null;
      }
      final Pair<DataSource, Filtration> dataSourceFiltrationPair = getFiltration(
          dataSource,
          filter,
          virtualColumnRegistry,
          plannerContext.getJoinableFactoryWrapper()
      );
      final DataSource newDataSource = dataSourceFiltrationPair.lhs;
      final Filtration filtration = dataSourceFiltrationPair.rhs;
      String bound = minTime ? TimeBoundaryQuery.MIN_TIME : TimeBoundaryQuery.MAX_TIME;
      Map<String, Object> context = new HashMap<>(plannerContext.queryContextMap());
      if (minTime) {
        context.put(TimeBoundaryQuery.MIN_TIME_ARRAY_OUTPUT_NAME, aggregatorFactory.getName());
      } else {
        context.put(TimeBoundaryQuery.MAX_TIME_ARRAY_OUTPUT_NAME, aggregatorFactory.getName());
      }
      return new TimeBoundaryQuery(
          newDataSource,
          filtration.getQuerySegmentSpec(),
          bound,
          filtration.getDimFilter(),
          context
      );
    }
    return null;
  }

  /**
   * Return this query as a Timeseries query, or null if this query is not compatible with Timeseries.
   *
   * @return query
   */
  @Nullable
  private TimeseriesQuery toTimeseriesQuery()
  {
    if (!plannerContext.featureAvailable(EngineFeature.TIMESERIES_QUERY)
        || grouping == null
        || grouping.getSubtotals().hasEffect(grouping.getDimensionSpecs())
        || grouping.getHavingFilter() != null
        || windowing != null) {
      return null;
    }

    if (sorting != null && sorting.getOffsetLimit().hasOffset()) {
      // Timeseries cannot handle offsets.
      return null;
    }

    final Granularity queryGranularity;
    final boolean descending;
    int timeseriesLimit = 0;
    final Map<String, Object> theContext = new HashMap<>();
    if (grouping.getDimensions().isEmpty()) {
      queryGranularity = Granularities.ALL;
      descending = false;
    } else if (grouping.getDimensions().size() == 1) {
      final DimensionExpression dimensionExpression = Iterables.getOnlyElement(grouping.getDimensions());
      queryGranularity = Expressions.toQueryGranularity(
          dimensionExpression.getDruidExpression(),
          plannerContext.getExpressionParser()
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
        if (sorting.getOffsetLimit().hasLimit()) {
          final long limit = sorting.getOffsetLimit().getLimit();

          if (limit == 0) {
            // Can't handle zero limit (the Timeseries query engine would treat it as unlimited).
            return null;
          }

          timeseriesLimit = Ints.checkedCast(limit);
        }

        switch (sorting.getTimeSortKind(dimensionExpression.getOutputName())) {
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

    // An aggregation query should return one row per group, with no grouping (e.g. ALL granularity), the entire table
    // is the group, so we should not skip empty buckets. When there are no results, this means we return the
    // initialized state for given aggregators instead of nothing.
    // Alternatively, the timeseries query should return empty buckets, even with ALL granularity when timeseries query
    // was originally a groupBy query, but with the grouping dimensions removed away in Grouping#applyProject
    if (!Granularities.ALL.equals(queryGranularity) || grouping.hasGroupingDimensionsDropped()) {
      theContext.put(TimeseriesQuery.SKIP_EMPTY_BUCKETS, true);
    }
    theContext.putAll(plannerContext.queryContextMap());

    final Pair<DataSource, Filtration> dataSourceFiltrationPair = getFiltration(
        dataSource,
        filter,
        virtualColumnRegistry,
        plannerContext.getJoinableFactoryWrapper()
    );
    final DataSource newDataSource = dataSourceFiltrationPair.lhs;
    final Filtration filtration = dataSourceFiltrationPair.rhs;

    if (!canUseQueryGranularity(dataSource, filtration, queryGranularity)) {
      return null;
    }

    final List<PostAggregator> postAggregators = new ArrayList<>(grouping.getPostAggregators());
    if (sorting != null && sorting.getProjection() != null) {
      postAggregators.addAll(sorting.getProjection().getPostAggregators());
    }

    return new TimeseriesQuery(
        newDataSource,
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
  private TopNQuery toTopNQuery()
  {
    // Must be allowed by the QueryMaker.
    if (!plannerContext.featureAvailable(EngineFeature.TOPN_QUERY)) {
      return null;
    }

    // Must have GROUP BY one column, no GROUPING SETS, ORDER BY ≤ 1 column, LIMIT > 0 and ≤ maxTopNLimit,
    // no OFFSET, no HAVING, no windowing.
    final boolean topNOk = grouping != null
                           && grouping.getDimensions().size() == 1
                           && !grouping.getSubtotals().hasEffect(grouping.getDimensionSpecs())
                           && sorting != null
                           && (sorting.getOrderBys().size() <= 1
                               && sorting.getOffsetLimit().hasLimit()
                               && sorting.getOffsetLimit().getLimit() > 0
                               && sorting.getOffsetLimit().getLimit() <= plannerContext.getPlannerConfig()
                                                                                       .getMaxTopNLimit()
                               && !sorting.getOffsetLimit().hasOffset())
                           && grouping.getHavingFilter() == null
                           && windowing == null;

    if (!topNOk) {
      return null;
    }

    final DimensionSpec dimensionSpec = Iterables.getOnlyElement(grouping.getDimensions()).toDimensionSpec();
    // grouping col cannot be type array
    if (dimensionSpec.getOutputType().isArray()) {
      return null;
    }
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

    final Pair<DataSource, Filtration> dataSourceFiltrationPair = getFiltration(
        dataSource,
        filter,
        virtualColumnRegistry,
        plannerContext.getJoinableFactoryWrapper()
    );
    final DataSource newDataSource = dataSourceFiltrationPair.lhs;
    final Filtration filtration = dataSourceFiltrationPair.rhs;

    final List<PostAggregator> postAggregators = new ArrayList<>(grouping.getPostAggregators());
    if (sorting.getProjection() != null) {
      postAggregators.addAll(sorting.getProjection().getPostAggregators());
    }

    return new TopNQuery(
        newDataSource,
        getVirtualColumns(true),
        dimensionSpec,
        topNMetricSpec,
        Ints.checkedCast(sorting.getOffsetLimit().getLimit()),
        filtration.getQuerySegmentSpec(),
        filtration.getDimFilter(),
        Granularities.ALL,
        grouping.getAggregatorFactories(),
        postAggregators,
        ImmutableSortedMap.copyOf(plannerContext.queryContextMap())
    );
  }

  /**
   * Return this query as a GroupBy query, or null if this query is not compatible with GroupBy.
   *
   * @return query or null
   */
  @Nullable
  private GroupByQuery toGroupByQuery()
  {
    if (grouping == null || windowing != null) {
      return null;
    }

    if (sorting != null && sorting.getOffsetLimit().hasLimit() && sorting.getOffsetLimit().getLimit() <= 0) {
      // Cannot handle zero or negative limits.
      return null;
    }

    final Pair<DataSource, Filtration> dataSourceFiltrationPair = getFiltration(
        dataSource,
        filter,
        virtualColumnRegistry,
        plannerContext.getJoinableFactoryWrapper()
    );
    final DataSource newDataSource = dataSourceFiltrationPair.lhs;
    final Filtration filtration = dataSourceFiltrationPair.rhs;

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

    GroupByQuery query = new GroupByQuery(
        newDataSource,
        filtration.getQuerySegmentSpec(),
        getVirtualColumns(true),
        filtration.getDimFilter(),
        Granularities.ALL,
        grouping.getDimensionSpecs(),
        grouping.getAggregatorFactories(),
        postAggregators,
        havingSpec,
        Optional.ofNullable(sorting).orElse(Sorting.none()).limitSpec(),
        grouping.getSubtotals().toSubtotalsSpec(grouping.getDimensionSpecs()),
        ImmutableSortedMap.copyOf(plannerContext.queryContextMap())
    );
    // We don't apply timestamp computation optimization yet when limit is pushed down. Maybe someday.
    if (query.getLimitSpec() instanceof DefaultLimitSpec && query.isApplyLimitPushDown()) {
      return query;
    }
    Map<String, Object> theContext = new HashMap<>();

    Granularity queryGranularity = null;

    // sql like "group by city_id,time_floor(__time to day)",
    // the original translated query is granularity=all and dimensions:[d0, d1]
    // the better plan is granularity=day and dimensions:[d0]
    // but the ResultRow structure is changed from [d0, d1] to [__time, d0]
    // this structure should be fixed as [d0, d1] (actually it is [d0, __time]) before postAggs are called.
    //
    // the above is the general idea of this optimization.
    // but from coding perspective, the granularity=all and "d0" dimension are referenced by many places,
    // eg: subtotals, having, grouping set, post agg,
    // there would be many many places need to be fixed if "d0" dimension is removed from query.dimensions
    // and the same to the granularity change.
    // so from easier coding perspective, this optimization is coded as groupby engine-level inner process change.
    // the most part of codes are in GroupByStrategyV2 about the process change between broker and compute node.
    // the basic logic like nested queries and subtotals are kept unchanged,
    // they will still see the granularity=all and the "d0" dimension.
    //
    // the tradeoff is that GroupByStrategyV2 behaviors differently according to the below query contexts.
    // in another word,
    // the query generated by "explain plan for select ..." doesn't match to the native query ACTUALLY being executed,
    // the granularity and dimensions are slightly different.
    // now, part of the query plan logic is handled in GroupByStrategyV2.
    if (!grouping.getDimensions().isEmpty()) {
      for (DimensionExpression dimensionExpression : grouping.getDimensions()) {
        Granularity granularity = Expressions.toQueryGranularity(
            dimensionExpression.getDruidExpression(),
            plannerContext.getExpressionParser()
        );
        if (granularity == null || !canUseQueryGranularity(dataSource, filtration, granularity)) {
          // Can't, or won't, convert this dimension to a query granularity.
          continue;
        }
        if (queryGranularity != null) {
          // group by more than one timestamp_floor
          // eg: group by timestamp_floor(__time to DAY),timestamp_floor(__time, to HOUR)
          queryGranularity = null;
          break;
        }
        queryGranularity = granularity;
        int timestampDimensionIndexInDimensions = grouping.getDimensions().indexOf(dimensionExpression);

        // these settings will only affect the most inner query sent to the down streaming compute nodes
        theContext.put(GroupByQuery.CTX_TIMESTAMP_RESULT_FIELD, dimensionExpression.getOutputName());
        theContext.put(GroupByQuery.CTX_TIMESTAMP_RESULT_FIELD_INDEX, timestampDimensionIndexInDimensions);

        try {
          theContext.put(
              GroupByQuery.CTX_TIMESTAMP_RESULT_FIELD_GRANULARITY,
              plannerContext.getJsonMapper().writeValueAsString(queryGranularity)
          );
        }
        catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
    if (queryGranularity == null) {
      return query;
    }
    return query.withOverriddenContext(theContext);
  }

  /**
   * Return this query as a {@link WindowOperatorQuery}, or null if this query cannot be run that way.
   *
   * @return query or null
   */
  @Nullable
  private WindowOperatorQuery toWindowQuery()
  {
    if (windowing == null) {
      return null;
    }

    // This is not yet supported
    if (dataSource.isConcrete()) {
      return null;
    }
    if (dataSource instanceof TableDataSource) {
      // We need a scan query to pull the results up for us before applying the window
      // Returning null here to ensure that the planner generates that alternative
      return null;
    }

    // all virtual cols are needed - these columns are only referenced from the aggregates
    VirtualColumns virtualColumns = virtualColumnRegistry.build(Collections.emptySet());
    final List<OperatorFactory> operators;

    if (virtualColumns.isEmpty()) {
      operators = windowing.getOperators();
    } else {
      operators = ImmutableList.<OperatorFactory>builder()
          .add(new ScanOperatorFactory(
              null,
              null,
              null,
              null,
              virtualColumns,
              null))
          .addAll(windowing.getOperators())
          .build();
    }
    return new WindowOperatorQuery(
        dataSource,
        new LegacySegmentSpec(Intervals.ETERNITY),
        plannerContext.queryContextMap(),
        windowing.getSignature(),
        operators,
        null
    );
  }

  /**
   * Create an OperatorQuery which runs an order on top of a scan.
   */
  @Nullable
  private WindowOperatorQuery toScanAndSortQuery()
  {
    if (sorting == null
        || sorting.getOrderBys().isEmpty()
        || (sorting.getProjection() != null && !sorting.getProjection().getVirtualColumns().isEmpty())) {
      return null;
    }

    ScanQuery scan = toScanQuery(false);
    if (scan == null) {
      return null;
    }

    if (dataSource.isConcrete()) {
      // Currently only non-time orderings of subqueries are allowed.
      setPlanningErrorOrderByNonTimeIsUnsupported();
      return null;
    }

    QueryDataSource newDataSource = new QueryDataSource(scan);
    List<ColumnWithDirection> sortColumns = getColumnWithDirectionsFromOrderBys(sorting.getOrderBys());
    RowSignature signature = getOutputRowSignature();
    List<OperatorFactory> operators = new ArrayList<>();

    operators.add(new NaiveSortOperatorFactory(sortColumns));


    final Projection projection = sorting.getProjection();

    final org.apache.druid.query.operator.OffsetLimit offsetLimit = sorting.getOffsetLimit().isNone()
        ? null
        : sorting.getOffsetLimit().toOperatorOffsetLimit();

    final List<String> projectedColumns = projection == null
        ? null
        : projection.getOutputRowSignature().getColumnNames();

    if (offsetLimit != null || projectedColumns != null) {
      operators.add(
          new ScanOperatorFactory(
              null,
              null,
              offsetLimit,
              projectedColumns,
              null,
              null
          )
      );
    }

    return new WindowOperatorQuery(
        newDataSource,
        new LegacySegmentSpec(Intervals.ETERNITY),
        plannerContext.queryContextMap(),
        signature,
        operators,
        null
    );
  }

  private void setPlanningErrorOrderByNonTimeIsUnsupported()
  {
    List<String> orderByColumnNames = sorting.getOrderBys()
        .stream().map(OrderByColumnSpec::getDimension)
        .collect(Collectors.toList());
    plannerContext.setPlanningError(
        "SQL query requires ordering a table by non-time column [%s], which is not supported.",
        orderByColumnNames
    );
  }

  private ArrayList<ColumnWithDirection> getColumnWithDirectionsFromOrderBys(List<OrderByColumnSpec> orderBys)
  {
    ArrayList<ColumnWithDirection> ordering = new ArrayList<>();
    for (OrderByColumnSpec orderBySpec : orderBys) {
      Direction direction = orderBySpec.getDirection() == OrderByColumnSpec.Direction.ASCENDING
          ? ColumnWithDirection.Direction.ASC
          : ColumnWithDirection.Direction.DESC;
      ordering.add(new ColumnWithDirection(orderBySpec.getDimension(), direction));
    }
    return ordering;
  }

  /**
   * Return this query as a Scan query, or null if this query is not compatible with Scan.
   * @param considerSorting can be used to ignore the current sorting requirements {@link #toScanAndSortQuery()} uses it to produce the non-sorted part
   * @return query or null
   */
  @Nullable
  private ScanQuery toScanQuery(final boolean considerSorting)
  {
    if (grouping != null || windowing != null) {
      // Scan cannot GROUP BY or do windows.
      return null;
    }

    if (outputRowSignature.size() == 0) {
      // Should never do a scan query without any columns that we're interested in. This is probably a planner bug.
      throw new ISE("Cannot convert to Scan query without any columns.");
    }

    final Pair<DataSource, Filtration> dataSourceFiltrationPair = getFiltration(
        dataSource,
        filter,
        virtualColumnRegistry,
        plannerContext.getJoinableFactoryWrapper()
    );
    final DataSource newDataSource = dataSourceFiltrationPair.lhs;
    final Filtration filtration = dataSourceFiltrationPair.rhs;

    final List<ScanQuery.OrderBy> orderByColumns;
    long scanOffset = 0L;
    long scanLimit = 0L;

    if (considerSorting && sorting != null) {
      scanOffset = sorting.getOffsetLimit().getOffset();

      if (sorting.getOffsetLimit().hasLimit()) {
        final long limit = sorting.getOffsetLimit().getLimit();

        if (limit == 0) {
          // Can't handle zero limit (the Scan query engine would treat it as unlimited).
          return null;
        }

        scanLimit = limit;
      }

      orderByColumns = sorting.getOrderBys().stream().map(
          orderBy ->
              new ScanQuery.OrderBy(
                  orderBy.getDimension(),
                  orderBy.getDirection() == OrderByColumnSpec.Direction.DESCENDING
                  ? ScanQuery.Order.DESCENDING
                  : ScanQuery.Order.ASCENDING
              )
      ).collect(Collectors.toList());
    } else {
      orderByColumns = Collections.emptyList();
    }

    if (!plannerContext.featureAvailable(EngineFeature.SCAN_ORDER_BY_NON_TIME) && !orderByColumns.isEmpty()) {
      if (orderByColumns.size() > 1
          || orderByColumns.stream()
                           .anyMatch(orderBy -> !orderBy.getColumnName().equals(ColumnHolder.TIME_COLUMN_NAME))) {
        // We cannot handle this ordering, but we encounter this ordering as part of the exploration of the volcano
        // planner, which means that the query that we are looking right now might only be doing this as one of the
        // potential branches of exploration rather than being a semantic requirement of the query itself.  So, it is
        // not safe to send an error message telling the end-user exactly what is happening, instead we need to set the
        // planning error and hope.
        setPlanningErrorOrderByNonTimeIsUnsupported();
        return null;
      }
    }

    // Compute the list of columns to select, sorted and deduped.
    final SortedSet<String> scanColumns = new TreeSet<>(outputRowSignature.getColumnNames());
    orderByColumns.forEach(column -> scanColumns.add(column.getColumnName()));

    final VirtualColumns virtualColumns = getVirtualColumns(true);
    final ImmutableList<String> scanColumnsList = ImmutableList.copyOf(scanColumns);

    return new ScanQuery(
        newDataSource,
        filtration.getQuerySegmentSpec(),
        virtualColumns,
        ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST,
        0,
        scanOffset,
        scanLimit,
        null,
        orderByColumns,
        filtration.getDimFilter(),
        scanColumnsList,
        false,
        withScanSignatureIfNeeded(
            virtualColumns,
            scanColumnsList,
            plannerContext.queryContextMap()
        ),
        outputRowSignature.buildSafeSignature(scanColumnsList).getColumnTypes()
    );
  }

  /**
   * Returns a copy of "queryContext" with {@link #CTX_SCAN_SIGNATURE} added if the execution context has the
   * {@link EngineFeature#SCAN_NEEDS_SIGNATURE} feature.
   *
   * {@link Deprecated} Instead of the context value {@link ScanQuery#getRowSignature()} can be used.
   */
  @Deprecated
  private Map<String, Object> withScanSignatureIfNeeded(
      final VirtualColumns virtualColumns,
      final List<String> scanColumns,
      final Map<String, Object> queryContext
  )
  {
    if (!plannerContext.featureAvailable(EngineFeature.SCAN_NEEDS_SIGNATURE)) {
      return queryContext;
    }
    final RowSignature signature = buildRowSignature(virtualColumns, scanColumns);

    try {
      Map<String, Object> revised = new HashMap<>(queryContext);
      revised.put(
          CTX_SCAN_SIGNATURE,
          plannerContext.getJsonMapper().writeValueAsString(signature)
      );
      return revised;
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private RowSignature buildRowSignature(final VirtualColumns virtualColumns, final List<String> columns)
  {
    // Compute the signature of the columns that we are selecting.
    final RowSignature.Builder builder = RowSignature.builder();

    for (final String columnName : columns) {
      final ColumnCapabilities capabilities =
          virtualColumns.getColumnCapabilitiesWithFallback(sourceRowSignature, columnName);

      if (capabilities == null) {
        // No type for this column. This is a planner bug.
        throw new ISE("No type for column [%s]", columnName);
      }
      builder.add(columnName, capabilities.toColumnType());
    }
    return builder.build();
  }
}
