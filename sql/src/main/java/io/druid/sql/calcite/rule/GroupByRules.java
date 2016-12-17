/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.sql.calcite.rule;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.java.util.common.ISE;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.DoubleMaxAggregatorFactory;
import io.druid.query.aggregation.DoubleMinAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.LongMaxAggregatorFactory;
import io.druid.query.aggregation.LongMinAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniqueFinalizingPostAggregator;
import io.druid.query.aggregation.post.ArithmeticPostAggregator;
import io.druid.query.aggregation.post.FieldAccessPostAggregator;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.ExtractionDimensionSpec;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.NotDimFilter;
import io.druid.query.groupby.orderby.DefaultLimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.ordering.StringComparator;
import io.druid.query.ordering.StringComparators;
import io.druid.segment.column.Column;
import io.druid.segment.column.ValueType;
import io.druid.sql.calcite.aggregation.Aggregation;
import io.druid.sql.calcite.aggregation.PostAggregatorFactory;
import io.druid.sql.calcite.expression.Expressions;
import io.druid.sql.calcite.expression.RowExtraction;
import io.druid.sql.calcite.filtration.Filtration;
import io.druid.sql.calcite.planner.PlannerConfig;
import io.druid.sql.calcite.rel.DruidRel;
import io.druid.sql.calcite.rel.Grouping;
import io.druid.sql.calcite.table.DruidTable;
import io.druid.sql.calcite.table.DruidTables;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;
import java.util.Map;

public class GroupByRules
{
  private GroupByRules()
  {
    // No instantiation.
  }

  public static List<RelOptRule> rules(final PlannerConfig plannerConfig)
  {
    return ImmutableList.of(
        new DruidAggregateRule(plannerConfig.isUseApproximateCountDistinct()),
        new DruidAggregateProjectRule(plannerConfig.isUseApproximateCountDistinct()),
        new DruidProjectAfterAggregationRule(),
        new DruidFilterAfterAggregationRule(),
        new DruidGroupBySortRule()
    );
  }

  /**
   * Used to represent inputs to aggregators. Ideally this should be folded into {@link RowExtraction}, but we
   * can't do that until RowExtractions are a bit more versatile.
   */
  private static class FieldOrExpression
  {
    private final String fieldName;
    private final String expression;

    public FieldOrExpression(String fieldName, String expression)
    {
      this.fieldName = fieldName;
      this.expression = expression;
      Preconditions.checkArgument(fieldName == null ^ expression == null, "must have either fieldName or expression");
    }

    public static FieldOrExpression fromRexNode(final List<String> rowOrder, final RexNode rexNode)
    {
      final RowExtraction rex = Expressions.toRowExtraction(rowOrder, rexNode);
      if (rex != null && rex.getExtractionFn() == null) {
        // This was a simple field access.
        return fieldName(rex.getColumn());
      }

      // Try as a math expression.
      final String mathExpression = Expressions.toMathExpression(rowOrder, rexNode);
      if (mathExpression != null) {
        return expression(mathExpression);
      }

      return null;
    }

    public static FieldOrExpression fieldName(final String fieldName)
    {
      return new FieldOrExpression(fieldName, null);
    }

    public static FieldOrExpression expression(final String expression)
    {
      return new FieldOrExpression(null, expression);
    }

    public String getFieldName()
    {
      return fieldName;
    }

    public String getExpression()
    {
      return expression;
    }
  }

  public static class DruidAggregateRule extends RelOptRule
  {
    final boolean approximateCountDistinct;

    private DruidAggregateRule(final boolean approximateCountDistinct)
    {
      super(operand(Aggregate.class, operand(DruidRel.class, none())));
      this.approximateCountDistinct = approximateCountDistinct;
    }

    @Override
    public void onMatch(RelOptRuleCall call)
    {
      final Aggregate aggregate = call.rel(0);
      final DruidRel druidRel = call.rel(1);
      final DruidRel newDruidRel = GroupByRules.applyAggregate(
          druidRel,
          null,
          aggregate,
          approximateCountDistinct
      );
      if (newDruidRel != null) {
        call.transformTo(newDruidRel);
      }
    }
  }

  public static class DruidAggregateProjectRule extends RelOptRule
  {
    final boolean approximateCountDistinct;

    private DruidAggregateProjectRule(final boolean approximateCountDistinct)
    {
      super(operand(Aggregate.class, operand(Project.class, operand(DruidRel.class, none()))));
      this.approximateCountDistinct = approximateCountDistinct;
    }

    @Override
    public void onMatch(RelOptRuleCall call)
    {
      final Aggregate aggregate = call.rel(0);
      final Project project = call.rel(1);
      final DruidRel druidRel = call.rel(2);
      final DruidRel newDruidRel = GroupByRules.applyAggregate(
          druidRel,
          project,
          aggregate,
          approximateCountDistinct
      );
      if (newDruidRel != null) {
        call.transformTo(newDruidRel);
      }
    }
  }

  public static class DruidProjectAfterAggregationRule extends RelOptRule
  {
    private DruidProjectAfterAggregationRule()
    {
      super(operand(Project.class, operand(DruidRel.class, none())));
    }

    @Override
    public void onMatch(RelOptRuleCall call)
    {
      final Project postProject = call.rel(0);
      final DruidRel druidRel = call.rel(1);
      final DruidRel newDruidRel = GroupByRules.applyProjectAfterAggregate(druidRel, postProject);
      if (newDruidRel != null) {
        call.transformTo(newDruidRel);
      }
    }
  }

  public static class DruidFilterAfterAggregationRule extends RelOptRule
  {
    private DruidFilterAfterAggregationRule()
    {
      super(operand(Filter.class, operand(DruidRel.class, none())));
    }

    @Override
    public void onMatch(RelOptRuleCall call)
    {
      final Filter postFilter = call.rel(0);
      final DruidRel druidRel = call.rel(1);
      final DruidRel newDruidRel = GroupByRules.applyFilterAfterAggregate(druidRel, postFilter);
      if (newDruidRel != null) {
        call.transformTo(newDruidRel);
      }
    }
  }

  public static class DruidGroupBySortRule extends RelOptRule
  {
    private DruidGroupBySortRule()
    {
      super(operand(Sort.class, operand(DruidRel.class, none())));
    }

    @Override
    public void onMatch(RelOptRuleCall call)
    {
      final Sort sort = call.rel(0);
      final DruidRel druidRel = call.rel(1);
      final DruidRel newDruidRel = GroupByRules.applySort(druidRel, sort);
      if (newDruidRel != null) {
        call.transformTo(newDruidRel);
      }
    }
  }

  private static DruidRel applyAggregate(
      final DruidRel druidRel,
      final Project project0,
      final Aggregate aggregate,
      final boolean approximateCountDistinct
  )
  {
    if ((project0 != null && druidRel.getQueryBuilder().getSelectProjection() != null /* can't project twice */)
        || druidRel.getQueryBuilder().getGrouping() != null
        || aggregate.indicator
        || aggregate.getGroupSets().size() != 1) {
      return null;
    }

    final Project project;
    if (project0 != null) {
      project = project0;
    } else if (druidRel.getQueryBuilder().getSelectProjection() != null) {
      project = druidRel.getQueryBuilder().getSelectProjection().getProject();
    } else {
      project = null;
    }

    final List<DimensionSpec> dimensions = Lists.newArrayList();
    final List<Aggregation> aggregations = Lists.newArrayList();
    final List<String> rowOrder = Lists.newArrayList();

    // Translate groupSet.
    final ImmutableBitSet groupSet = aggregate.getGroupSet();

    int dimOutputNameCounter = 0;
    for (int i : groupSet) {
      if (project != null && project.getChildExps().get(i) instanceof RexLiteral) {
        // Ignore literals in GROUP BY, so a user can write e.g. "GROUP BY 'dummy'" to group everything into a single
        // row. Add dummy rowOrder entry so NULLs come out. This is not strictly correct but it works as long as
        // nobody actually expects to see the literal.
        rowOrder.add(dimOutputName(dimOutputNameCounter++));
      } else {
        final DimensionSpec dimensionSpec = toDimensionSpec(
            druidRel.getDruidTable(),
            Expressions.toRowExtraction(
                DruidTables.rowOrder(druidRel.getDruidTable()),
                Expressions.fromFieldAccess(druidRel.getDruidTable(), project, i)
            ),
            dimOutputName(dimOutputNameCounter++)
        );
        if (dimensionSpec == null) {
          return null;
        }
        dimensions.add(dimensionSpec);
        rowOrder.add(dimensionSpec.getOutputName());
      }
    }

    // Translate aggregates.
    for (int i = 0; i < aggregate.getAggCallList().size(); i++) {
      final AggregateCall aggCall = aggregate.getAggCallList().get(i);
      final Aggregation aggregation = translateAggregateCall(
          druidRel,
          project,
          aggCall,
          i,
          approximateCountDistinct
      );

      if (aggregation == null) {
        return null;
      }

      aggregations.add(aggregation);
      rowOrder.add(aggregation.getOutputName());
    }

    return druidRel.withQueryBuilder(
        druidRel.getQueryBuilder()
                .withGrouping(
                    Grouping.create(dimensions, aggregations),
                    aggregate.getRowType(),
                    rowOrder
                )
    );
  }

  private static DruidRel applyProjectAfterAggregate(
      final DruidRel druidRel,
      final Project postProject
  )
  {
    if (druidRel.getQueryBuilder().getGrouping() == null || druidRel.getQueryBuilder().getLimitSpec() != null) {
      return null;
    }

    final List<String> rowOrder = druidRel.getQueryBuilder().getRowOrder();
    final Grouping grouping = druidRel.getQueryBuilder().getGrouping();
    final List<Aggregation> newAggregations = Lists.newArrayList(grouping.getAggregations());
    final List<PostAggregatorFactory> finalizingPostAggregatorFactories = Lists.newArrayList();
    final List<String> newRowOrder = Lists.newArrayList();

    // Build list of finalizingPostAggregatorFactories.
    final Map<String, Aggregation> aggregationMap = Maps.newHashMap();
    for (final Aggregation aggregation : grouping.getAggregations()) {
      aggregationMap.put(aggregation.getOutputName(), aggregation);
    }
    for (final String field : rowOrder) {
      final Aggregation aggregation = aggregationMap.get(field);
      finalizingPostAggregatorFactories.add(
          aggregation == null
          ? null
          : aggregation.getFinalizingPostAggregatorFactory()
      );
    }

    // Walk through the postProject expressions.
    for (final RexNode projectExpression : postProject.getChildExps()) {
      if (projectExpression.isA(SqlKind.INPUT_REF)) {
        final RexInputRef ref = (RexInputRef) projectExpression;
        final String fieldName = rowOrder.get(ref.getIndex());
        newRowOrder.add(fieldName);
        finalizingPostAggregatorFactories.add(null);
      } else {
        // Attempt to convert to PostAggregator.
        final String postAggregatorName = aggOutputName(newAggregations.size());
        final PostAggregator postAggregator = Expressions.toPostAggregator(
            postAggregatorName,
            rowOrder,
            finalizingPostAggregatorFactories,
            projectExpression
        );
        if (postAggregator != null) {
          newAggregations.add(Aggregation.create(postAggregator));
          newRowOrder.add(postAggregator.getName());
          finalizingPostAggregatorFactories.add(null);
        } else {
          return null;
        }
      }
    }

    return druidRel.withQueryBuilder(
        druidRel.getQueryBuilder()
                .withAdjustedGrouping(
                    Grouping.create(grouping.getDimensions(), newAggregations),
                    postProject.getRowType(),
                    newRowOrder
                )
    );
  }

  private static DruidRel applyFilterAfterAggregate(
      final DruidRel druidRel,
      final Filter postFilter
  )
  {
    if (druidRel.getQueryBuilder().getGrouping() == null
        || druidRel.getQueryBuilder().getHaving() != null
        || druidRel.getQueryBuilder().getLimitSpec() != null) {
      return null;
    }

    final DimFilter dimFilter = Expressions.toFilter(
        null, // null table; this filter is being applied as a HAVING on result rows
        druidRel.getQueryBuilder().getRowOrder(),
        postFilter.getCondition()
    );

    if (dimFilter != null) {
      return druidRel.withQueryBuilder(
          druidRel.getQueryBuilder()
                  .withHaving(dimFilter)
      );
    } else {
      return null;
    }
  }

  private static DruidRel applySort(
      final DruidRel druidRel,
      final Sort sort
  )
  {
    if (druidRel.getQueryBuilder().getGrouping() == null || druidRel.getQueryBuilder().getLimitSpec() != null) {
      // Can only sort when grouping and not already sorting.
      return null;
    }

    final Grouping grouping = druidRel.getQueryBuilder().getGrouping();
    final DefaultLimitSpec limitSpec = toLimitSpec(druidRel.getQueryBuilder().getRowOrder(), sort);
    if (limitSpec == null) {
      return null;
    }

    final List<OrderByColumnSpec> orderBys = limitSpec.getColumns();
    final List<DimensionSpec> newDimensions = Lists.newArrayList(grouping.getDimensions());

    // Reorder dimensions, maybe, to allow groupBy to consider pushing down sorting (see DefaultLimitSpec).
    if (!orderBys.isEmpty()) {
      final Map<String, Integer> dimensionOrderByOutputName = Maps.newHashMap();
      for (int i = 0; i < newDimensions.size(); i++) {
        dimensionOrderByOutputName.put(newDimensions.get(i).getOutputName(), i);
      }
      for (int i = 0; i < orderBys.size(); i++) {
        final OrderByColumnSpec orderBy = orderBys.get(i);
        final Integer dimensionOrder = dimensionOrderByOutputName.get(orderBy.getDimension());
        if (dimensionOrder != null
            && dimensionOrder != i
            && orderBy.getDirection() == OrderByColumnSpec.Direction.ASCENDING
            && orderBy.getDimensionComparator().equals(StringComparators.LEXICOGRAPHIC)) {
          final DimensionSpec tmp = newDimensions.get(i);
          newDimensions.set(i, newDimensions.get(dimensionOrder));
          newDimensions.set(dimensionOrder, tmp);
          dimensionOrderByOutputName.put(newDimensions.get(i).getOutputName(), i);
          dimensionOrderByOutputName.put(newDimensions.get(dimensionOrder).getOutputName(), dimensionOrder);
        }
      }
    }

    if (!orderBys.isEmpty() || limitSpec.getLimit() < Integer.MAX_VALUE) {
      return druidRel.withQueryBuilder(
          druidRel.getQueryBuilder()
                  .withAdjustedGrouping(
                      Grouping.create(newDimensions, grouping.getAggregations()),
                      druidRel.getQueryBuilder().getRowType(),
                      druidRel.getQueryBuilder().getRowOrder()
                  )
                  .withLimitSpec(limitSpec)
      );
    } else {
      return druidRel;
    }
  }

  public static DefaultLimitSpec toLimitSpec(
      final List<String> rowOrder,
      final Sort sort
  )
  {
    final Integer limit = sort.fetch != null ? RexLiteral.intValue(sort.fetch) : null;
    final List<OrderByColumnSpec> orderBys = Lists.newArrayListWithCapacity(sort.getChildExps().size());

    if (sort.offset != null) {
      // LimitSpecs don't accept offsets.
      return null;
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

      if (SqlTypeName.NUMERIC_TYPES.contains(sortExpression.getType().getSqlTypeName())
          || SqlTypeName.DATETIME_TYPES.contains(sortExpression.getType().getSqlTypeName())) {
        comparator = StringComparators.NUMERIC;
      } else {
        comparator = StringComparators.LEXICOGRAPHIC;
      }

      if (sortExpression.isA(SqlKind.INPUT_REF)) {
        final RexInputRef ref = (RexInputRef) sortExpression;
        final String fieldName = rowOrder.get(ref.getIndex());
        orderBys.add(new OrderByColumnSpec(fieldName, direction, comparator));
      } else {
        // We don't support sorting by anything other than refs which actually appear in the query result.
        return null;
      }
    }

    return new DefaultLimitSpec(orderBys, limit);
  }

  private static DimensionSpec toDimensionSpec(
      final DruidTable druidTable,
      final RowExtraction rex,
      final String name
  )
  {
    if (rex == null) {
      return null;
    }

    final int columnNumber = druidTable.getColumnNumber(rex.getColumn());
    if (columnNumber < 0) {
      return null;
    }

    final ValueType columnType = druidTable.getColumnType(columnNumber);

    if (columnType == ValueType.STRING ||
        (rex.getColumn().equals(Column.TIME_COLUMN_NAME) && rex.getExtractionFn() != null)) {
      return rex.getExtractionFn() == null
             ? new DefaultDimensionSpec(rex.getColumn(), name)
             : new ExtractionDimensionSpec(rex.getColumn(), name, rex.getExtractionFn());
    } else {
      // Can't create dimensionSpecs for non-string, non-time.
      return null;
    }
  }

  /**
   * Translate an AggregateCall to Druid equivalents.
   *
   * @return translated aggregation, or null if translation failed.
   */
  private static Aggregation translateAggregateCall(
      final DruidRel druidRel,
      final Project project,
      final AggregateCall call,
      final int aggNumber,
      final boolean approximateCountDistinct
  )
  {
    final List<DimFilter> filters = Lists.newArrayList();
    final List<String> rowOrder = DruidTables.rowOrder(druidRel.getDruidTable());
    final String name = aggOutputName(aggNumber);
    final SqlKind kind = call.getAggregation().getKind();
    final SqlTypeName outputType = call.getType().getSqlTypeName();
    final Aggregation retVal;

    if (call.filterArg >= 0) {
      // AGG(xxx) FILTER(WHERE yyy)
      if (project == null) {
        // We need some kind of projection to support filtered aggregations.
        return null;
      }

      final RexNode expression = project.getChildExps().get(call.filterArg);
      final DimFilter filter = Expressions.toFilter(druidRel.getDruidTable(), rowOrder, expression);
      if (filter == null) {
        return null;
      }

      filters.add(filter);
    }

    if (call.getAggregation().getKind() == SqlKind.COUNT && call.getArgList().isEmpty()) {
      // COUNT(*)
      retVal = Aggregation.create(new CountAggregatorFactory(name));
    } else if (call.getAggregation().getKind() == SqlKind.COUNT && call.isDistinct() && approximateCountDistinct) {
      // COUNT(DISTINCT x)
      final DimensionSpec dimensionSpec = toDimensionSpec(
          druidRel.getDruidTable(),
          Expressions.toRowExtraction(
              rowOrder,
              Expressions.fromFieldAccess(
                  druidRel.getDruidTable(),
                  project,
                  Iterables.getOnlyElement(call.getArgList())
              )
          ),
          aggInternalName(aggNumber, "dimSpec")
      );

      if (dimensionSpec == null) {
        return null;
      }

      retVal = Aggregation.createFinalizable(
          ImmutableList.<AggregatorFactory>of(
              new CardinalityAggregatorFactory(name, ImmutableList.of(dimensionSpec), false)
          ),
          null,
          new PostAggregatorFactory()
          {
            @Override
            public PostAggregator factorize(String outputName)
            {
              return new HyperUniqueFinalizingPostAggregator(outputName, name);
            }
          }
      );
    } else if (!call.isDistinct() && call.getArgList().size() == 1) {
      // AGG(xxx), not distinct, not COUNT(*)
      boolean forceCount = false;
      final FieldOrExpression input;

      final int inputField = Iterables.getOnlyElement(call.getArgList());
      final RexNode rexNode = Expressions.fromFieldAccess(druidRel.getDruidTable(), project, inputField);
      final FieldOrExpression foe = FieldOrExpression.fromRexNode(rowOrder, rexNode);

      if (foe != null) {
        input = foe;
      } else if (rexNode.getKind() == SqlKind.CASE && ((RexCall) rexNode).getOperands().size() == 3) {
        // Possibly a CASE-style filtered aggregation. Styles supported:
        // A: SUM(CASE WHEN x = 'foo' THEN cnt END) => operands (x = 'foo', cnt, null)
        // B: SUM(CASE WHEN x = 'foo' THEN 1 ELSE 0 END) => operands (x = 'foo', 1, 0)
        // C: COUNT(CASE WHEN x = 'foo' THEN 'dummy' END) => operands (x = 'foo', 'dummy', null)
        // If the null and non-null args are switched, "flip" is set, which negates the filter.

        final RexCall caseCall = (RexCall) rexNode;
        final boolean flip = RexLiteral.isNullLiteral(caseCall.getOperands().get(1))
                             && !RexLiteral.isNullLiteral(caseCall.getOperands().get(2));
        final RexNode arg1 = caseCall.getOperands().get(flip ? 2 : 1);
        final RexNode arg2 = caseCall.getOperands().get(flip ? 1 : 2);

        // Operand 1: Filter
        final DimFilter filter = Expressions.toFilter(
            druidRel.getDruidTable(),
            rowOrder,
            caseCall.getOperands().get(0)
        );
        if (filter == null) {
          return null;
        } else {
          filters.add(flip ? new NotDimFilter(filter) : filter);
        }

        if (call.getAggregation().getKind() == SqlKind.COUNT
            && arg1 instanceof RexLiteral
            && !RexLiteral.isNullLiteral(arg1)
            && RexLiteral.isNullLiteral(arg2)) {
          // Case C
          forceCount = true;
          input = null;
        } else if (call.getAggregation().getKind() == SqlKind.SUM
                   && arg1 instanceof RexLiteral
                   && ((Number) RexLiteral.value(arg1)).intValue() == 1
                   && arg2 instanceof RexLiteral
                   && ((Number) RexLiteral.value(arg2)).intValue() == 0) {
          // Case B
          forceCount = true;
          input = null;
        } else if (RexLiteral.isNullLiteral(arg2)) {
          // Maybe case A
          input = FieldOrExpression.fromRexNode(rowOrder, arg1);
          if (input == null) {
            return null;
          }
        } else {
          // Can't translate CASE into a filter.
          return null;
        }
      } else {
        // Can't translate aggregator expression.
        return null;
      }

      if (!forceCount) {
        Preconditions.checkNotNull(input, "WTF?! input was null for non-COUNT aggregation");
      }

      if (forceCount || kind == SqlKind.COUNT) {
        // COUNT(x)
        retVal = Aggregation.create(new CountAggregatorFactory(name));
      } else {
        // All aggregators other than COUNT expect a single argument with no extractionFn.
        final String fieldName = input.getFieldName();
        final String expression = input.getExpression();

        final boolean isLong = SqlTypeName.INT_TYPES.contains(outputType)
                               || SqlTypeName.DATETIME_TYPES.contains(outputType);

        if (kind == SqlKind.SUM || kind == SqlKind.SUM0) {
          retVal = isLong
                   ? Aggregation.create(new LongSumAggregatorFactory(name, fieldName, expression))
                   : Aggregation.create(new DoubleSumAggregatorFactory(name, fieldName, expression));
        } else if (kind == SqlKind.MIN) {
          retVal = isLong
                   ? Aggregation.create(new LongMinAggregatorFactory(name, fieldName, expression))
                   : Aggregation.create(new DoubleMinAggregatorFactory(name, fieldName, expression));
        } else if (kind == SqlKind.MAX) {
          retVal = isLong
                   ? Aggregation.create(new LongMaxAggregatorFactory(name, fieldName, expression))
                   : Aggregation.create(new DoubleMaxAggregatorFactory(name, fieldName, expression));
        } else if (kind == SqlKind.AVG) {
          final String sumName = aggInternalName(aggNumber, "sum");
          final String countName = aggInternalName(aggNumber, "count");
          final AggregatorFactory sum = isLong
                                        ? new LongSumAggregatorFactory(sumName, fieldName, expression)
                                        : new DoubleSumAggregatorFactory(sumName, fieldName, expression);
          final AggregatorFactory count = new CountAggregatorFactory(countName);
          retVal = Aggregation.create(
              ImmutableList.of(sum, count),
              new ArithmeticPostAggregator(
                  name,
                  "quotient",
                  ImmutableList.<PostAggregator>of(
                      new FieldAccessPostAggregator(null, sumName),
                      new FieldAccessPostAggregator(null, countName)
                  )
              )
          );
        } else {
          retVal = null;
        }
      }
    } else {
      retVal = null;
    }

    final DimFilter filter = filters.isEmpty()
                             ? null
                             : Filtration.create(new AndDimFilter(filters))
                                         .optimizeFilterOnly(druidRel.getDruidTable())
                                         .getDimFilter();

    return retVal != null ? retVal.filter(filter) : null;
  }

  public static String dimOutputName(final int dimNumber)
  {
    return "d" + dimNumber;
  }

  private static String aggOutputName(final int aggNumber)
  {
    return "a" + aggNumber;
  }

  private static String aggInternalName(final int aggNumber, final String key)
  {
    return "A" + aggNumber + ":" + key;
  }
}
