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
import io.druid.query.aggregation.post.ArithmeticPostAggregator;
import io.druid.query.aggregation.post.FieldAccessPostAggregator;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.NotDimFilter;
import io.druid.query.groupby.orderby.DefaultLimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.ordering.StringComparator;
import io.druid.query.ordering.StringComparators;
import io.druid.segment.column.ValueType;
import io.druid.sql.calcite.aggregation.Aggregation;
import io.druid.sql.calcite.aggregation.ApproxCountDistinctSqlAggregator;
import io.druid.sql.calcite.aggregation.PostAggregatorFactory;
import io.druid.sql.calcite.aggregation.SqlAggregator;
import io.druid.sql.calcite.expression.Expressions;
import io.druid.sql.calcite.expression.RowExtraction;
import io.druid.sql.calcite.filtration.Filtration;
import io.druid.sql.calcite.planner.Calcites;
import io.druid.sql.calcite.planner.DruidOperatorTable;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.rel.DruidNestedGroupBy;
import io.druid.sql.calcite.rel.DruidRel;
import io.druid.sql.calcite.rel.Grouping;
import io.druid.sql.calcite.table.RowSignature;
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
  private static final ApproxCountDistinctSqlAggregator APPROX_COUNT_DISTINCT = new ApproxCountDistinctSqlAggregator();

  private GroupByRules()
  {
    // No instantiation.
  }

  public static List<RelOptRule> rules(final DruidOperatorTable operatorTable)
  {
    return ImmutableList.of(
        new DruidAggregateRule(operatorTable),
        new DruidAggregateProjectRule(operatorTable),
        new DruidAggregateProjectFilterRule(operatorTable),
        new DruidGroupByPostAggregationRule(),
        new DruidGroupByHavingRule(operatorTable),
        new DruidGroupByLimitRule()
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

    public static FieldOrExpression fromRexNode(
        final DruidOperatorTable operatorTable,
        final PlannerContext plannerContext,
        final List<String> rowOrder,
        final RexNode rexNode
    )
    {
      final RowExtraction rex = Expressions.toRowExtraction(operatorTable, plannerContext, rowOrder, rexNode);
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
    private final DruidOperatorTable operatorTable;

    private DruidAggregateRule(final DruidOperatorTable operatorTable)
    {
      super(operand(Aggregate.class, operand(DruidRel.class, none())));
      this.operatorTable = operatorTable;
    }

    @Override
    public boolean matches(RelOptRuleCall call)
    {
      final Aggregate aggregate = call.rel(0);
      final DruidRel druidRel = call.rel(1);
      return canApplyAggregate(druidRel, null, null, aggregate);
    }

    @Override
    public void onMatch(RelOptRuleCall call)
    {
      final Aggregate aggregate = call.rel(0);
      final DruidRel druidRel = call.rel(1);
      final DruidRel newDruidRel = GroupByRules.applyAggregate(
          druidRel,
          null,
          null,
          aggregate,
          operatorTable,
          druidRel.getPlannerContext().getPlannerConfig().isUseApproximateCountDistinct()
      );
      if (newDruidRel != null) {
        call.transformTo(newDruidRel);
      }
    }
  }

  public static class DruidAggregateProjectRule extends RelOptRule
  {
    private final DruidOperatorTable operatorTable;

    private DruidAggregateProjectRule(final DruidOperatorTable operatorTable)
    {
      super(operand(Aggregate.class, operand(Project.class, operand(DruidRel.class, none()))));
      this.operatorTable = operatorTable;
    }

    @Override
    public boolean matches(RelOptRuleCall call)
    {
      final Aggregate aggregate = call.rel(0);
      final Project project = call.rel(1);
      final DruidRel druidRel = call.rel(2);
      return canApplyAggregate(druidRel, null, project, aggregate);
    }

    @Override
    public void onMatch(RelOptRuleCall call)
    {
      final Aggregate aggregate = call.rel(0);
      final Project project = call.rel(1);
      final DruidRel druidRel = call.rel(2);
      final DruidRel newDruidRel = GroupByRules.applyAggregate(
          druidRel,
          null,
          project,
          aggregate,
          operatorTable,
          druidRel.getPlannerContext().getPlannerConfig().isUseApproximateCountDistinct()
      );
      if (newDruidRel != null) {
        call.transformTo(newDruidRel);
      }
    }
  }

  public static class DruidAggregateProjectFilterRule extends RelOptRule
  {
    private final DruidOperatorTable operatorTable;

    private DruidAggregateProjectFilterRule(final DruidOperatorTable operatorTable)
    {
      super(operand(Aggregate.class, operand(Project.class, operand(Filter.class, operand(DruidRel.class, none())))));
      this.operatorTable = operatorTable;
    }

    @Override
    public boolean matches(RelOptRuleCall call)
    {
      final Aggregate aggregate = call.rel(0);
      final Project project = call.rel(1);
      final Filter filter = call.rel(2);
      final DruidRel druidRel = call.rel(3);
      return canApplyAggregate(druidRel, filter, project, aggregate);
    }

    @Override
    public void onMatch(RelOptRuleCall call)
    {
      final Aggregate aggregate = call.rel(0);
      final Project project = call.rel(1);
      final Filter filter = call.rel(2);
      final DruidRel druidRel = call.rel(3);
      final DruidRel newDruidRel = GroupByRules.applyAggregate(
          druidRel,
          filter,
          project,
          aggregate,
          operatorTable,
          druidRel.getPlannerContext().getPlannerConfig().isUseApproximateCountDistinct()
      );
      if (newDruidRel != null) {
        call.transformTo(newDruidRel);
      }
    }
  }

  public static class DruidGroupByPostAggregationRule extends RelOptRule
  {
    private DruidGroupByPostAggregationRule()
    {
      super(operand(Project.class, operand(DruidRel.class, none())));
    }

    @Override
    public boolean matches(RelOptRuleCall call)
    {
      final DruidRel druidRel = call.rel(1);
      return canApplyPostAggregation(druidRel);
    }

    @Override
    public void onMatch(RelOptRuleCall call)
    {
      final Project postProject = call.rel(0);
      final DruidRel druidRel = call.rel(1);
      final DruidRel newDruidRel = GroupByRules.applyPostAggregation(druidRel, postProject);
      if (newDruidRel != null) {
        call.transformTo(newDruidRel);
      }
    }
  }

  public static class DruidGroupByHavingRule extends RelOptRule
  {
    private final DruidOperatorTable operatorTable;

    private DruidGroupByHavingRule(final DruidOperatorTable operatorTable)
    {
      super(operand(Filter.class, operand(DruidRel.class, none())));
      this.operatorTable = operatorTable;
    }

    @Override
    public boolean matches(RelOptRuleCall call)
    {
      final DruidRel druidRel = call.rel(1);
      return canApplyHaving(druidRel);
    }

    @Override
    public void onMatch(RelOptRuleCall call)
    {
      final Filter postFilter = call.rel(0);
      final DruidRel druidRel = call.rel(1);
      final DruidRel newDruidRel = GroupByRules.applyHaving(operatorTable, druidRel, postFilter);
      if (newDruidRel != null) {
        call.transformTo(newDruidRel);
      }
    }
  }

  public static class DruidGroupByLimitRule extends RelOptRule
  {
    private DruidGroupByLimitRule()
    {
      super(operand(Sort.class, operand(DruidRel.class, none())));
    }

    @Override
    public boolean matches(RelOptRuleCall call)
    {
      final DruidRel druidRel = call.rel(1);
      return canApplyLimit(druidRel);
    }

    @Override
    public void onMatch(RelOptRuleCall call)
    {
      final Sort sort = call.rel(0);
      final DruidRel druidRel = call.rel(1);
      final DruidRel newDruidRel = GroupByRules.applyLimit(druidRel, sort);
      if (newDruidRel != null) {
        call.transformTo(newDruidRel);
      }
    }
  }

  private static boolean canApplyAggregate(
      final DruidRel druidRel,
      final Filter filter,
      final Project project,
      final Aggregate aggregate
  )
  {
    return (filter == null || druidRel.getQueryBuilder().getFilter() == null /* can't filter twice */)
           && (project == null || druidRel.getQueryBuilder().getSelectProjection() == null /* can't project twice */)
           && !aggregate.indicator
           && aggregate.getGroupSets().size() == 1;
  }

  /**
   * Applies a filter -> project -> aggregate chain to a druidRel. Do not call this method unless
   * {@link #canApplyAggregate(DruidRel, Filter, Project, Aggregate)} returns true.
   *
   * @return new rel, or null if the chain cannot be applied
   */
  private static DruidRel applyAggregate(
      final DruidRel druidRel,
      final Filter filter0,
      final Project project0,
      final Aggregate aggregate,
      final DruidOperatorTable operatorTable,
      final boolean approximateCountDistinct
  )
  {
    Preconditions.checkState(canApplyAggregate(druidRel, filter0, project0, aggregate), "Cannot applyAggregate.");

    final RowSignature sourceRowSignature;
    final boolean isNestedQuery = druidRel.getQueryBuilder().getGrouping() != null;

    if (isNestedQuery) {
      // Nested groupBy; source row signature is the output signature of druidRel.
      sourceRowSignature = druidRel.getOutputRowSignature();
    } else {
      sourceRowSignature = druidRel.getSourceRowSignature();
    }

    // Filter that should be applied before aggregating.
    final DimFilter filter;
    if (filter0 != null) {
      filter = Expressions.toFilter(
          operatorTable,
          druidRel.getPlannerContext(),
          sourceRowSignature,
          filter0.getCondition()
      );
      if (filter == null) {
        // Can't plan this filter.
        return null;
      }
    } else if (druidRel.getQueryBuilder().getFilter() != null && !isNestedQuery) {
      // We're going to replace the existing druidRel, so inherit its filter.
      filter = druidRel.getQueryBuilder().getFilter();
    } else {
      filter = null;
    }

    // Projection that should be applied before aggregating.
    final Project project;
    if (project0 != null) {
      project = project0;
    } else if (druidRel.getQueryBuilder().getSelectProjection() != null && !isNestedQuery) {
      // We're going to replace the existing druidRel, so inherit its projection.
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
        final RexNode rexNode = Expressions.fromFieldAccess(sourceRowSignature, project, i);
        final RowExtraction rex = Expressions.toRowExtraction(
            operatorTable,
            druidRel.getPlannerContext(),
            sourceRowSignature.getRowOrder(),
            rexNode
        );
        if (rex == null) {
          return null;
        }

        final SqlTypeName sqlTypeName = rexNode.getType().getSqlTypeName();
        final ValueType outputType = Calcites.getValueTypeForSqlTypeName(sqlTypeName);
        if (outputType == null) {
          throw new ISE("Cannot translate sqlTypeName[%s] to Druid type for field[%s]", sqlTypeName, rowOrder.get(i));
        }

        final DimensionSpec dimensionSpec = rex.toDimensionSpec(
            sourceRowSignature,
            dimOutputName(dimOutputNameCounter++),
            outputType
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
          druidRel.getPlannerContext(),
          sourceRowSignature,
          project,
          aggCall,
          operatorTable,
          aggregations,
          i,
          approximateCountDistinct
      );

      if (aggregation == null) {
        return null;
      }

      aggregations.add(aggregation);
      rowOrder.add(aggregation.getOutputName());
    }

    if (isNestedQuery) {
      // Nested groupBy.
      return DruidNestedGroupBy.from(
          druidRel,
          filter,
          Grouping.create(dimensions, aggregations),
          aggregate.getRowType(),
          rowOrder
      );
    } else {
      // groupBy on a base dataSource.
      return druidRel.withQueryBuilder(
          druidRel.getQueryBuilder()
                  .withFilter(filter)
                  .withGrouping(
                      Grouping.create(dimensions, aggregations),
                      aggregate.getRowType(),
                      rowOrder
                  )
      );
    }
  }

  private static boolean canApplyPostAggregation(final DruidRel druidRel)
  {
    return druidRel.getQueryBuilder().getGrouping() != null && druidRel.getQueryBuilder().getLimitSpec() == null;
  }

  /**
   * Applies a projection to the aggregations of a druidRel, by potentially adding post-aggregators.
   *
   * @return new rel, or null if the projection cannot be applied
   */
  private static DruidRel applyPostAggregation(final DruidRel druidRel, final Project postProject)
  {
    Preconditions.checkState(canApplyPostAggregation(druidRel), "Cannot applyPostAggregation");

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

  private static boolean canApplyHaving(final DruidRel druidRel)
  {
    return druidRel.getQueryBuilder().getGrouping() != null
           && druidRel.getQueryBuilder().getHaving() == null
           && druidRel.getQueryBuilder().getLimitSpec() == null;
  }

  /**
   * Applies a filter to an aggregating druidRel, as a HavingSpec. Do not call this method unless
   * {@link #canApplyHaving(DruidRel)} returns true.
   *
   * @return new rel, or null if the filter cannot be applied
   */
  private static DruidRel applyHaving(
      final DruidOperatorTable operatorTable,
      final DruidRel druidRel,
      final Filter postFilter
  )
  {
    Preconditions.checkState(canApplyHaving(druidRel), "Cannot applyHaving.");

    final DimFilter dimFilter = Expressions.toFilter(
        operatorTable,
        druidRel.getPlannerContext(),
        druidRel.getOutputRowSignature(),
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

  private static boolean canApplyLimit(final DruidRel druidRel)
  {
    return druidRel.getQueryBuilder().getGrouping() != null && druidRel.getQueryBuilder().getLimitSpec() == null;
  }

  /**
   * Applies a sort to an aggregating druidRel, as a LimitSpec. Do not call this method unless
   * {@link #canApplyLimit(DruidRel)} returns true.
   *
   * @return new rel, or null if the sort cannot be applied
   */
  private static DruidRel applyLimit(final DruidRel druidRel, final Sort sort)
  {
    Preconditions.checkState(canApplyLimit(druidRel), "Cannot applyLimit.");

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
        final String fieldName = rowOrder.get(ref.getIndex());
        orderBys.add(new OrderByColumnSpec(fieldName, direction, comparator));
      } else {
        // We don't support sorting by anything other than refs which actually appear in the query result.
        return null;
      }
    }

    return new DefaultLimitSpec(orderBys, limit);
  }

  /**
   * Translate an AggregateCall to Druid equivalents.
   *
   * @return translated aggregation, or null if translation failed.
   */
  private static Aggregation translateAggregateCall(
      final PlannerContext plannerContext,
      final RowSignature sourceRowSignature,
      final Project project,
      final AggregateCall call,
      final DruidOperatorTable operatorTable,
      final List<Aggregation> existingAggregations,
      final int aggNumber,
      final boolean approximateCountDistinct
  )
  {
    final List<DimFilter> filters = Lists.newArrayList();
    final List<String> rowOrder = sourceRowSignature.getRowOrder();
    final String name = aggOutputName(aggNumber);
    final SqlKind kind = call.getAggregation().getKind();
    final SqlTypeName outputType = call.getType().getSqlTypeName();

    if (call.filterArg >= 0) {
      // AGG(xxx) FILTER(WHERE yyy)
      if (project == null) {
        // We need some kind of projection to support filtered aggregations.
        return null;
      }

      final RexNode expression = project.getChildExps().get(call.filterArg);
      final DimFilter filter = Expressions.toFilter(operatorTable, plannerContext, sourceRowSignature, expression);
      if (filter == null) {
        return null;
      }

      filters.add(filter);
    }

    if (kind == SqlKind.COUNT && call.getArgList().isEmpty()) {
      // COUNT(*)
      return Aggregation.create(new CountAggregatorFactory(name)).filter(makeFilter(filters, sourceRowSignature));
    } else if (kind == SqlKind.COUNT && call.isDistinct()) {
      // COUNT(DISTINCT x)
      return approximateCountDistinct ? APPROX_COUNT_DISTINCT.toDruidAggregation(
          name,
          sourceRowSignature,
          operatorTable,
          plannerContext,
          existingAggregations,
          project,
          call,
          makeFilter(filters, sourceRowSignature)
      ) : null;
    } else if (kind == SqlKind.COUNT
               || kind == SqlKind.SUM
               || kind == SqlKind.SUM0
               || kind == SqlKind.MIN
               || kind == SqlKind.MAX
               || kind == SqlKind.AVG) {
      // Built-in agg, not distinct, not COUNT(*)
      boolean forceCount = false;
      final FieldOrExpression input;

      final int inputField = Iterables.getOnlyElement(call.getArgList());
      final RexNode rexNode = Expressions.fromFieldAccess(sourceRowSignature, project, inputField);
      final FieldOrExpression foe = FieldOrExpression.fromRexNode(operatorTable, plannerContext, rowOrder, rexNode);

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
            operatorTable,
            plannerContext,
            sourceRowSignature,
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
          input = FieldOrExpression.fromRexNode(operatorTable, plannerContext, rowOrder, arg1);
          if (input == null) {
            return null;
          }
        } else {
          // Can't translate CASE into a filter.
          return null;
        }
      } else {
        // Can't translate operand.
        return null;
      }

      if (!forceCount) {
        Preconditions.checkNotNull(input, "WTF?! input was null for non-COUNT aggregation");
      }

      if (forceCount || kind == SqlKind.COUNT) {
        // COUNT(x)
        return Aggregation.create(new CountAggregatorFactory(name)).filter(makeFilter(filters, sourceRowSignature));
      } else {
        // Built-in aggregator that is not COUNT.
        final Aggregation retVal;
        final String fieldName = input.getFieldName();
        final String expression = input.getExpression();

        final boolean isLong = SqlTypeName.INT_TYPES.contains(outputType)
                               || SqlTypeName.TIMESTAMP == outputType
                               || SqlTypeName.DATE == outputType;

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
          // Not reached.
          throw new ISE("WTF?! Kind[%s] got into the built-in aggregator path somehow?!", kind);
        }

        return retVal.filter(makeFilter(filters, sourceRowSignature));
      }
    } else {
      // Not a built-in aggregator, check operator table.
      final SqlAggregator sqlAggregator = operatorTable.lookupAggregator(call.getAggregation().getName());
      return sqlAggregator != null ? sqlAggregator.toDruidAggregation(
          name,
          sourceRowSignature,
          operatorTable,
          plannerContext,
          existingAggregations,
          project,
          call,
          makeFilter(filters, sourceRowSignature)
      ) : null;
    }
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

  private static DimFilter makeFilter(final List<DimFilter> filters, final RowSignature sourceRowSignature)
  {
    return filters.isEmpty()
           ? null
           : Filtration.create(new AndDimFilter(filters))
                       .optimizeFilterOnly(sourceRowSignature)
                       .getDimFilter();
  }
}
