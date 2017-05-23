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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.druid.query.groupby.orderby.DefaultLimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.segment.column.Column;
import io.druid.sql.calcite.expression.Expressions;
import io.druid.sql.calcite.expression.VirtualColumnRegistry;
import io.druid.sql.calcite.planner.DruidOperatorTable;
import io.druid.sql.calcite.rel.DruidRel;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public class SelectRules
{
  private SelectRules()
  {
    // No instantiation.
  }

  public static List<RelOptRule> rules(final DruidOperatorTable operatorTable)
  {
    return ImmutableList.of(
        new DruidSelectProjectionRule(operatorTable),
        new DruidSelectSortRule()
    );
  }

  static class DruidSelectProjectionRule extends RelOptRule
  {
    private final DruidOperatorTable operatorTable;

    public DruidSelectProjectionRule(final DruidOperatorTable operatorTable)
    {
      super(operand(Project.class, operand(DruidRel.class, none())));
      this.operatorTable = operatorTable;
    }

    @Override
    public boolean matches(RelOptRuleCall call)
    {
      final DruidRel druidRel = call.rel(1);

      return druidRel.getQueryBuilder().getSelectProjection() == null
             && druidRel.getQueryBuilder().getGrouping() == null
             && druidRel.getQueryBuilder().getLimitSpec() == null;
    }

    @Override
    public void onMatch(RelOptRuleCall call)
    {
      final Project project = call.rel(0);
      final DruidRel druidRel = call.rel(1);

      // Only push in projections that can be used by the Select query.
      // Leave anything more complicated to DruidAggregateProjectRule for possible handling in a GroupBy query.

      final RowSignature sourceRowSignature = druidRel.getSourceRowSignature();
      final VirtualColumnRegistry virtualColumnRegistry = druidRel.getQueryBuilder().getVirtualColumnRegistry();
      final List<String> rowOrder = Lists.newArrayList();

      for (int i = 0; i < project.getRowType().getFieldCount(); i++) {
        final RexNode rexNode = project.getChildExps().get(i);
        final String columnName = Expressions.toDruidColumn(
            operatorTable,
            druidRel.getPlannerContext(),
            sourceRowSignature,
            virtualColumnRegistry,
            rexNode
        );

        if (columnName == null) {
          return;
        }

        rowOrder.add(columnName);
      }

      call.transformTo(
          druidRel.withQueryBuilder(
              druidRel.getQueryBuilder()
                      .withSelectProjection(project, rowOrder)
          )
      );
    }
  }

  static class DruidSelectSortRule extends RelOptRule
  {
    private DruidSelectSortRule()
    {
      super(operand(Sort.class, operand(DruidRel.class, none())));
    }

    @Override
    public boolean matches(RelOptRuleCall call)
    {
      final DruidRel druidRel = call.rel(1);

      return druidRel.getQueryBuilder().getGrouping() == null
             && druidRel.getQueryBuilder().getLimitSpec() == null;
    }

    @Override
    public void onMatch(RelOptRuleCall call)
    {
      final Sort sort = call.rel(0);
      final DruidRel druidRel = call.rel(1);

      final DefaultLimitSpec limitSpec = GroupByRules.toLimitSpec(druidRel.getQueryBuilder().getRowOrder(), sort);
      if (limitSpec == null) {
        return;
      }

      // Only push in sorts that can be used by the Select query.
      final List<OrderByColumnSpec> orderBys = limitSpec.getColumns();
      if (orderBys.isEmpty() ||
          (orderBys.size() == 1 && orderBys.get(0).getDimension().equals(Column.TIME_COLUMN_NAME))) {
        call.transformTo(
            druidRel.withQueryBuilder(
                druidRel.getQueryBuilder()
                        .withLimitSpec(limitSpec)
            )
        );
      }
    }
  }
}
