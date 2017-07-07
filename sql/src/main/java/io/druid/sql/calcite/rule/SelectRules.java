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
import io.druid.query.groupby.orderby.DefaultLimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.segment.VirtualColumn;
import io.druid.segment.column.Column;
import io.druid.sql.calcite.expression.DruidExpression;
import io.druid.sql.calcite.expression.Expressions;
import io.druid.sql.calcite.planner.Calcites;
import io.druid.sql.calcite.rel.DruidRel;
import io.druid.sql.calcite.rel.SelectProjection;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;

import java.util.ArrayList;
import java.util.List;

public class SelectRules
{
  private SelectRules()
  {
    // No instantiation.
  }

  public static List<RelOptRule> rules()
  {
    return ImmutableList.of(
        new DruidSelectProjectionRule(),
        new DruidSelectSortRule()
    );
  }

  static class DruidSelectProjectionRule extends RelOptRule
  {
    public DruidSelectProjectionRule()
    {
      super(operand(Project.class, operand(DruidRel.class, none())));
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
      final List<DruidExpression> expressions = Expressions.toDruidExpressions(
          druidRel.getPlannerContext(),
          sourceRowSignature,
          project.getChildExps()
      );

      if (expressions == null) {
        return;
      }

      final List<String> directColumns = new ArrayList<>();
      final List<VirtualColumn> virtualColumns = new ArrayList<>();
      final List<String> rowOrder = new ArrayList<>();

      int virtualColumnNameCounter = 0;
      for (int i = 0; i < expressions.size(); i++) {
        final DruidExpression expression = expressions.get(i);
        if (expression.isDirectColumnAccess()) {
          directColumns.add(expression.getDirectColumn());
          rowOrder.add(expression.getDirectColumn());
        } else {
          String candidate = "v" + virtualColumnNameCounter++;
          while (sourceRowSignature.getColumnType(candidate) != null) {
            candidate = "v" + virtualColumnNameCounter++;
          }
          virtualColumns.add(
              expression.toVirtualColumn(
                  candidate,
                  Calcites.getValueTypeForSqlTypeName(project.getChildExps().get(i).getType().getSqlTypeName()),
                  druidRel.getPlannerContext().getExprMacroTable()
              )
          );
          rowOrder.add(candidate);
        }
      }

      call.transformTo(
          druidRel.withQueryBuilder(
              druidRel.getQueryBuilder()
                      .withSelectProjection(new SelectProjection(project, directColumns, virtualColumns), rowOrder)
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
