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
import io.druid.math.expr.ExprMacroTable;
import io.druid.math.expr.ExprType;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.DoubleMaxAggregatorFactory;
import io.druid.query.aggregation.DoubleMinAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.FloatMaxAggregatorFactory;
import io.druid.query.aggregation.FloatMinAggregatorFactory;
import io.druid.query.aggregation.FloatSumAggregatorFactory;
import io.druid.query.aggregation.LongMaxAggregatorFactory;
import io.druid.query.aggregation.LongMinAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.post.ArithmeticPostAggregator;
import io.druid.query.aggregation.post.ExpressionPostAggregator;
import io.druid.query.aggregation.post.FieldAccessPostAggregator;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.groupby.orderby.DefaultLimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.ordering.StringComparator;
import io.druid.query.ordering.StringComparators;
import io.druid.segment.VirtualColumn;
import io.druid.segment.column.ValueType;
import io.druid.sql.calcite.aggregation.Aggregation;
import io.druid.sql.calcite.aggregation.ApproxCountDistinctSqlAggregator;
import io.druid.sql.calcite.aggregation.DimensionExpression;
import io.druid.sql.calcite.aggregation.SqlAggregator;
import io.druid.sql.calcite.expression.DruidExpression;
import io.druid.sql.calcite.expression.Expressions;
import io.druid.sql.calcite.expression.SimpleExtraction;
import io.druid.sql.calcite.filtration.Filtration;
import io.druid.sql.calcite.planner.Calcites;
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
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

public class GroupByRules
{
  private static final ApproxCountDistinctSqlAggregator APPROX_COUNT_DISTINCT = new ApproxCountDistinctSqlAggregator();

  private GroupByRules()
  {
    // No instantiation.
  }

  public static List<RelOptRule> rules()
  {
    return ImmutableList.of(
        new DruidAggregateRule(),
        new DruidAggregateProjectRule(),
        new DruidAggregateProjectFilterRule(),
        new DruidGroupByPostAggregationRule(),
        new DruidGroupByHavingRule(),
        new DruidGroupByLimitRule()
    );
  }

  public static class DruidAggregateRule extends RelOptRule
  {
    private DruidAggregateRule()
    {
      super(operand(Aggregate.class, operand(DruidRel.class, none())));
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
      final DruidRel newDruidRel = GroupByRules.applyAggregate(druidRel, null, null, aggregate);
      if (newDruidRel != null) {
        call.transformTo(newDruidRel);
      }
    }
  }

  public static class DruidAggregateProjectRule extends RelOptRule
  {
    private DruidAggregateProjectRule()
    {
      super(operand(Aggregate.class, operand(Project.class, operand(DruidRel.class, none()))));
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
      final DruidRel newDruidRel = GroupByRules.applyAggregate(druidRel, null, project, aggregate);
      if (newDruidRel != null) {
        call.transformTo(newDruidRel);
      }
    }
  }

  public static class DruidAggregateProjectFilterRule extends RelOptRule
  {
    private DruidAggregateProjectFilterRule()
    {
      super(operand(Aggregate.class, operand(Project.class, operand(Filter.class, operand(DruidRel.class, none())))));
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
          aggregate
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
    private DruidGroupByHavingRule()
    {
      super(operand(Filter.class, operand(DruidRel.class, none())));
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
      final DruidRel newDruidRel = GroupByRules.applyHaving(druidRel, postFilter);
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
   * @param druidRel  base rel to apply aggregation on top of
   * @param filter0   filter that should be applied before aggregating
   * @param project0  projection that should be applied before aggregating
   * @param aggregate aggregation to apply
   *
   * @return new rel, or null if the chain cannot be applied
   */
  private static DruidRel applyAggregate(
      final DruidRel druidRel,
      final Filter filter0,
      final Project project0,
      final Aggregate aggregate
  )
  {
    Preconditions.checkState(canApplyAggregate(druidRel, filter0, project0, aggregate), "Cannot applyAggregate.");

    final RowSignature sourceRowSignature;
    final TreeSet<String> reservedSourceRowNames = new TreeSet<>();
    final boolean isNestedQuery = druidRel.getQueryBuilder().getGrouping() != null;

    if (isNestedQuery) {
      // Nested groupBy; source row signature is the output signature of druidRel.
      sourceRowSignature = druidRel.getOutputRowSignature();
      reservedSourceRowNames.addAll(sourceRowSignature.getRowOrder());
    } else {
      sourceRowSignature = druidRel.getSourceRowSignature();
      reservedSourceRowNames.addAll(sourceRowSignature.getRowOrder());
      reservedSourceRowNames.addAll(
          Arrays.stream(druidRel.getQueryBuilder()
                                .getVirtualColumns(druidRel.getPlannerContext().getExprMacroTable())
                                .getVirtualColumns()).map(VirtualColumn::getOutputName).collect(Collectors.toList())
      );
    }

    // Filter that should be applied before aggregating.
    final DimFilter filter;
    if (filter0 != null) {
      filter = Expressions.toFilter(
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
      project = druidRel.getQueryBuilder().getSelectProjection().getCalciteProject();
    } else {
      project = null;
    }

    final List<DimensionExpression> dimensions = Lists.newArrayList();
    final List<Aggregation> aggregations = Lists.newArrayList();
    final List<String> rowOrder = Lists.newArrayList();

    // Translate groupSet.
    final ImmutableBitSet groupSet = aggregate.getGroupSet();

    int dimOutputNameCounter = 0;
    for (int i : groupSet) {
      // Dimension might need to create virtual columns. Avoid giving it a name that would lead to colliding columns.
      String dimOutputNameCurrent = "d" + dimOutputNameCounter++;
      while (Calcites.anyStartsWith(reservedSourceRowNames, dimOutputNameCurrent + ":")) {
        dimOutputNameCurrent = "d" + dimOutputNameCounter;
      }

      reservedSourceRowNames.add(dimOutputNameCurrent + ":");

      if (project != null && project.getChildExps().get(i) instanceof RexLiteral) {
        // Ignore literals in GROUP BY, so a user can write e.g. "GROUP BY 'dummy'" to group everything into a single
        // row. Add dummy rowOrder entry so NULLs come out. This is not strictly correct but it works as long as
        // nobody actually expects to see the literal.
        rowOrder.add(dimOutputNameCurrent);
      } else {
        final RexNode rexNode = Expressions.fromFieldAccess(sourceRowSignature, project, i);
        final DruidExpression druidExpression = Expressions.toDruidExpression(
            druidRel.getPlannerContext(),
            sourceRowSignature,
            rexNode
        );
        if (druidExpression == null) {
          return null;
        }

        final SqlTypeName sqlTypeName = rexNode.getType().getSqlTypeName();
        final ValueType outputType = Calcites.getValueTypeForSqlTypeName(sqlTypeName);
        if (outputType == null) {
          throw new ISE("Cannot translate sqlTypeName[%s] to Druid type for field[%s]", sqlTypeName, rowOrder.get(i));
        } else if (outputType == ValueType.COMPLEX) {
          // Can't group on complex columns.
          return null;
        }

        dimensions.add(new DimensionExpression(dimOutputNameCurrent, druidExpression, outputType));
        rowOrder.add(dimOutputNameCurrent);
      }
    }

    // Translate aggregates.
    int aggNameCounter = 0;
    for (int i = 0; i < aggregate.getAggCallList().size(); i++) {
      // Aggregation might need to create virtual columns. Avoid giving it a name that would lead to colliding columns.
      String aggNameCurrent = "a" + aggNameCounter++;
      while (Calcites.anyStartsWith(reservedSourceRowNames, aggNameCurrent + ":")) {
        aggNameCurrent = "a" + aggNameCounter++;
      }

      reservedSourceRowNames.add(aggNameCurrent + ":");

      final AggregateCall aggCall = aggregate.getAggCallList().get(i);
      final Aggregation aggregation = translateAggregateCall(
          druidRel.getPlannerContext(),
          sourceRowSignature,
          druidRel.getCluster().getRexBuilder(),
          project,
          aggCall,
          aggregations,
          aggNameCurrent
      );

      if (aggregation == null) {
        return null;
      }

      aggregations.add(aggregation);
      rowOrder.add(aggregation.getOutputName());
    }

    final Grouping grouping = Grouping.create(dimensions, aggregations);

    if (isNestedQuery) {
      // Nested groupBy.
      return DruidNestedGroupBy.from(druidRel, filter, grouping, aggregate.getRowType(), rowOrder);
    } else {
      // groupBy on a base dataSource or semiJoin.
      return druidRel.withQueryBuilder(
          druidRel.getQueryBuilder()
                  .withFilter(filter)
                  .withGrouping(grouping, aggregate.getRowType(), rowOrder)
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
  private static DruidRel applyPostAggregation(
      final DruidRel druidRel,
      final Project postProject
  )
  {
    Preconditions.checkState(canApplyPostAggregation(druidRel), "Cannot applyPostAggregation");

    final Grouping grouping = druidRel.getQueryBuilder().getGrouping();
    final List<Aggregation> newAggregations = Lists.newArrayList(grouping.getAggregations());
    final List<String> newRowOrder = Lists.newArrayList();
    final Set<String> allPostAggregatorNames = grouping.getPostAggregators()
                                                       .stream()
                                                       .map(PostAggregator::getName)
                                                       .collect(Collectors.toSet());

    // Walk through the postProject expressions.
    int projectPostAggregatorCount = 0;
    for (final RexNode postAggregatorRexNode : postProject.getChildExps()) {
      // Attempt to convert to PostAggregator.
      final DruidExpression postAggregatorExpression = Expressions.toDruidExpression(
          druidRel.getPlannerContext(),
          druidRel.getOutputRowSignature(),
          postAggregatorRexNode
      );

      if (postAggregatorExpression == null) {
        return null;
      }

      if (postAggregatorDirectColumnIsOk(druidRel, postAggregatorExpression, postAggregatorRexNode)) {
        // Direct column access, without any type cast as far as Druid's runtime is concerned.
        // (There might be a SQL-level type cast that we don't care about)
        newRowOrder.add(postAggregatorExpression.getDirectColumn());
      } else {
        String postAggregatorNameCurrent = "p" + projectPostAggregatorCount++;
        while (allPostAggregatorNames.contains(postAggregatorNameCurrent)) {
          postAggregatorNameCurrent = "p" + postAggregatorNameCurrent;
        }
        final PostAggregator postAggregator = new ExpressionPostAggregator(
            postAggregatorNameCurrent,
            postAggregatorExpression.getExpression(),
            null,
            druidRel.getPlannerContext().getExprMacroTable()
        );
        newAggregations.add(Aggregation.create(postAggregator));
        newRowOrder.add(postAggregator.getName());
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

  /**
   * Returns true if a post-aggregation "expression" can be realized as a direct field access. This is true if it's
   * a direct column access that doesn't require an implicit cast.
   *
   * @param druidRel   druid aggregation rel
   * @param expression post-aggregation expression
   * @param rexNode    RexNode for the post-aggregation expression
   *
   * @return
   */
  private static boolean postAggregatorDirectColumnIsOk(
      final DruidRel druidRel,
      final DruidExpression expression,
      final RexNode rexNode
  )
  {
    if (!expression.isDirectColumnAccess()) {
      return false;
    }

    // Check if a cast is necessary.
    final ExprType toExprType = Expressions.exprTypeForValueType(
        druidRel.getQueryBuilder()
                .getOutputRowSignature()
                .getColumnType(expression.getDirectColumn())
    );

    final ExprType fromExprType = Expressions.exprTypeForValueType(
        Calcites.getValueTypeForSqlTypeName(rexNode.getType().getSqlTypeName())
    );

    return toExprType.equals(fromExprType);
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
      final DruidRel druidRel,
      final Filter postFilter
  )
  {
    Preconditions.checkState(canApplyHaving(druidRel), "Cannot applyHaving.");

    final DimFilter dimFilter = Expressions.toFilter(
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
    final RowSignature outputRowSignature = druidRel.getOutputRowSignature();
    final DefaultLimitSpec limitSpec = toLimitSpec(druidRel.getQueryBuilder().getRowOrder(), sort);
    if (limitSpec == null) {
      return null;
    }

    final List<OrderByColumnSpec> orderBys = limitSpec.getColumns();
    final List<DimensionExpression> newDimensions = Lists.newArrayList(grouping.getDimensions());

    // Reorder dimensions, maybe, to allow groupBy to consider pushing down sorting (see DefaultLimitSpec).
    if (!orderBys.isEmpty()) {
      final Map<String, Integer> dimensionOrderByOutputName = Maps.newHashMap();
      for (int i = 0; i < newDimensions.size(); i++) {
        dimensionOrderByOutputName.put(newDimensions.get(i).getOutputName(), i);
      }
      for (int i = 0; i < orderBys.size(); i++) {
        final OrderByColumnSpec orderBy = orderBys.get(i);
        final Integer dimensionOrder = dimensionOrderByOutputName.get(orderBy.getDimension());
        final StringComparator comparator = outputRowSignature.naturalStringComparator(
            SimpleExtraction.of(orderBy.getDimension(), null)
        );

        if (dimensionOrder != null
            && orderBy.getDirection() == OrderByColumnSpec.Direction.ASCENDING
            && orderBy.getDimensionComparator().equals(comparator)) {
          if (dimensionOrder != i) {
            final DimensionExpression tmp = newDimensions.get(i);
            newDimensions.set(i, newDimensions.get(dimensionOrder));
            newDimensions.set(dimensionOrder, tmp);
            dimensionOrderByOutputName.put(newDimensions.get(i).getOutputName(), i);
            dimensionOrderByOutputName.put(newDimensions.get(dimensionOrder).getOutputName(), dimensionOrder);
          }
        } else {
          // Ordering by something that we can't shift into the grouping key. Bail out.
          break;
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
      final RexBuilder rexBuilder,
      final Project project,
      final AggregateCall call,
      final List<Aggregation> existingAggregations,
      final String name
  )
  {
    final DimFilter filter;
    final SqlKind kind = call.getAggregation().getKind();
    final SqlTypeName outputType = call.getType().getSqlTypeName();

    if (call.filterArg >= 0) {
      // AGG(xxx) FILTER(WHERE yyy)
      if (project == null) {
        // We need some kind of projection to support filtered aggregations.
        return null;
      }

      final RexNode expression = project.getChildExps().get(call.filterArg);
      filter = Expressions.toFilter(plannerContext, sourceRowSignature, expression);
      if (filter == null) {
        return null;
      }
    } else {
      filter = null;
    }

    if (kind == SqlKind.COUNT && call.getArgList().isEmpty()) {
      // COUNT(*)
      return Aggregation.create(new CountAggregatorFactory(name)).filter(makeFilter(filter, sourceRowSignature));
    } else if (call.isDistinct()) {
      // AGG(DISTINCT x)
      if (kind == SqlKind.COUNT && plannerContext.getPlannerConfig().isUseApproximateCountDistinct()) {
        // Approximate COUNT(DISTINCT x)
        return APPROX_COUNT_DISTINCT.toDruidAggregation(
            name,
            sourceRowSignature,
            plannerContext,
            existingAggregations,
            project,
            call,
            makeFilter(filter, sourceRowSignature)
        );
      } else {
        // Exact COUNT(DISTINCT x), or some non-COUNT aggregator.
        return null;
      }
    } else if (kind == SqlKind.COUNT
               || kind == SqlKind.SUM
               || kind == SqlKind.SUM0
               || kind == SqlKind.MIN
               || kind == SqlKind.MAX
               || kind == SqlKind.AVG) {
      // Built-in agg, not distinct, not COUNT(*)
      final RexNode rexNode = Expressions.fromFieldAccess(
          sourceRowSignature,
          project,
          Iterables.getOnlyElement(call.getArgList())
      );

      final DruidExpression input = toDruidExpressionForAggregator(plannerContext, sourceRowSignature, rexNode);
      if (input == null) {
        return null;
      }

      if (kind == SqlKind.COUNT) {
        // COUNT(x) should count all non-null values of x.
        if (rexNode.getType().isNullable()) {
          final DimFilter nonNullFilter = Expressions.toFilter(
              plannerContext,
              sourceRowSignature,
              rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, ImmutableList.of(rexNode))
          );

          if (nonNullFilter == null) {
            // Don't expect this to happen.
            throw new ISE("Could not create not-null filter for rexNode[%s]", rexNode);
          }

          return Aggregation.create(new CountAggregatorFactory(name)).filter(
              makeFilter(
                  filter == null ? nonNullFilter : new AndDimFilter(ImmutableList.of(filter, nonNullFilter)),
                  sourceRowSignature
              )
          );
        } else {
          return Aggregation.create(new CountAggregatorFactory(name)).filter(makeFilter(filter, sourceRowSignature));
        }
      } else {
        // Built-in aggregator that is not COUNT.
        final Aggregation retVal;

        final ValueType aggregationType;

        if (SqlTypeName.INT_TYPES.contains(outputType)
            || SqlTypeName.TIMESTAMP == outputType
            || SqlTypeName.DATE == outputType) {
          aggregationType = ValueType.LONG;
        } else if (SqlTypeName.FLOAT == outputType) {
          aggregationType = ValueType.FLOAT;
        } else if (SqlTypeName.FRACTIONAL_TYPES.contains(outputType)) {
          aggregationType = ValueType.DOUBLE;
        } else {
          throw new ISE(
              "Cannot determine aggregation type for SQL operator[%s] type[%s]",
              call.getAggregation().getName(),
              outputType
          );
        }

        final String fieldName;
        final String expression;
        final ExprMacroTable macroTable = plannerContext.getExprMacroTable();

        if (input.isDirectColumnAccess()) {
          fieldName = input.getDirectColumn();
          expression = null;
        } else {
          fieldName = null;
          expression = input.getExpression();
        }

        if (kind == SqlKind.SUM || kind == SqlKind.SUM0) {
          retVal = Aggregation.create(
              createSumAggregatorFactory(aggregationType, name, fieldName, expression, macroTable)
          );
        } else if (kind == SqlKind.MIN) {
          retVal = Aggregation.create(
              createMinAggregatorFactory(aggregationType, name, fieldName, expression, macroTable)
          );
        } else if (kind == SqlKind.MAX) {
          retVal = Aggregation.create(
              createMaxAggregatorFactory(aggregationType, name, fieldName, expression, macroTable)
          );
        } else if (kind == SqlKind.AVG) {
          final String sumName = String.format("%s:sum", name);
          final String countName = String.format("%s:count", name);
          final AggregatorFactory sum = createSumAggregatorFactory(
              aggregationType,
              sumName,
              fieldName,
              expression,
              macroTable
          );
          final AggregatorFactory count = new CountAggregatorFactory(countName);
          retVal = Aggregation.create(
              ImmutableList.of(sum, count),
              new ArithmeticPostAggregator(
                  name,
                  "quotient",
                  ImmutableList.of(
                      new FieldAccessPostAggregator(null, sumName),
                      new FieldAccessPostAggregator(null, countName)
                  )
              )
          );
        } else {
          // Not reached.
          throw new ISE("WTF?! Kind[%s] got into the built-in aggregator path somehow?!", kind);
        }

        return retVal.filter(makeFilter(filter, sourceRowSignature));
      }
    } else {
      // Not a built-in aggregator, check operator table.
      final SqlAggregator sqlAggregator = plannerContext.getOperatorTable()
                                                        .lookupAggregator(call.getAggregation());

      if (sqlAggregator != null) {
        return sqlAggregator.toDruidAggregation(
            name,
            sourceRowSignature,
            plannerContext,
            existingAggregations,
            project,
            call,
            makeFilter(filter, sourceRowSignature)
        );
      } else {
        return null;
      }
    }
  }

  private static DruidExpression toDruidExpressionForAggregator(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode
  )
  {
    final DruidExpression druidExpression = Expressions.toDruidExpression(plannerContext, rowSignature, rexNode);
    if (druidExpression == null) {
      return null;
    }

    if (druidExpression.isSimpleExtraction() &&
        (!druidExpression.isDirectColumnAccess()
         || rowSignature.getColumnType(druidExpression.getDirectColumn()) == ValueType.STRING)) {
      // Aggregators are unable to implicitly cast strings to numbers. So remove the simple extraction in this case.
      return druidExpression.map(simpleExtraction -> null, Function.identity());
    } else {
      return druidExpression;
    }
  }

  private static DimFilter makeFilter(final DimFilter filter, final RowSignature sourceRowSignature)
  {
    return filter == null
           ? null
           : Filtration.create(filter)
                       .optimizeFilterOnly(sourceRowSignature)
                       .getDimFilter();
  }

  private static AggregatorFactory createSumAggregatorFactory(
      final ValueType aggregationType,
      final String name,
      final String fieldName,
      final String expression,
      final ExprMacroTable macroTable
  )
  {
    switch (aggregationType) {
      case LONG:
        return new LongSumAggregatorFactory(name, fieldName, expression, macroTable);
      case FLOAT:
        return new FloatSumAggregatorFactory(name, fieldName, expression, macroTable);
      case DOUBLE:
        return new DoubleSumAggregatorFactory(name, fieldName, expression, macroTable);
      default:
        throw new ISE("Cannot create aggregator factory for type[%s]", aggregationType);
    }
  }

  private static AggregatorFactory createMinAggregatorFactory(
      final ValueType aggregationType,
      final String name,
      final String fieldName,
      final String expression,
      final ExprMacroTable macroTable
  )
  {
    switch (aggregationType) {
      case LONG:
        return new LongMinAggregatorFactory(name, fieldName, expression, macroTable);
      case FLOAT:
        return new FloatMinAggregatorFactory(name, fieldName, expression, macroTable);
      case DOUBLE:
        return new DoubleMinAggregatorFactory(name, fieldName, expression, macroTable);
      default:
        throw new ISE("Cannot create aggregator factory for type[%s]", aggregationType);
    }
  }

  private static AggregatorFactory createMaxAggregatorFactory(
      final ValueType aggregationType,
      final String name,
      final String fieldName,
      final String expression,
      final ExprMacroTable macroTable
  )
  {
    switch (aggregationType) {
      case LONG:
        return new LongMaxAggregatorFactory(name, fieldName, expression, macroTable);
      case FLOAT:
        return new FloatMaxAggregatorFactory(name, fieldName, expression, macroTable);
      case DOUBLE:
        return new DoubleMaxAggregatorFactory(name, fieldName, expression, macroTable);
      default:
        throw new ISE("Cannot create aggregator factory for type[%s]", aggregationType);
    }
  }
}
