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
import io.druid.java.util.common.ISE;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.groupby.orderby.DefaultLimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.segment.column.Column;
import io.druid.segment.column.ValueType;
import io.druid.sql.calcite.expression.Expressions;
import io.druid.sql.calcite.expression.RowExtraction;
import io.druid.sql.calcite.planner.Calcites;
import io.druid.sql.calcite.planner.DruidOperatorTable;
import io.druid.sql.calcite.rel.DruidRel;
import io.druid.sql.calcite.rel.SelectProjection;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;

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
      final List<DimensionSpec> dimensions = Lists.newArrayList();
      final List<String> metrics = Lists.newArrayList();
      final List<String> rowOrder = Lists.newArrayList();

      int dimOutputNameCounter = 0;
      for (int i = 0; i < project.getRowType().getFieldCount(); i++) {
        final RexNode rexNode = project.getChildExps().get(i);
        final RowExtraction rex = Expressions.toRowExtraction(
            operatorTable,
            druidRel.getPlannerContext(),
            sourceRowSignature.getRowOrder(),
            rexNode
        );

        if (rex == null) {
          return;
        }

        final String column = rex.getColumn();
        final ExtractionFn extractionFn = rex.getExtractionFn();

        // Check if this field should be a dimension, a metric, or a reference to __time.
        final ValueType columnType = sourceRowSignature.getColumnType(column);

        if (columnType == ValueType.STRING || (column.equals(Column.TIME_COLUMN_NAME) && extractionFn != null)) {
          // Add to dimensions.
          do {
            dimOutputNameCounter++;
          } while (sourceRowSignature.getColumnType(GroupByRules.dimOutputName(dimOutputNameCounter)) != null);
          final String outputName = GroupByRules.dimOutputName(dimOutputNameCounter);
          final SqlTypeName sqlTypeName = rexNode.getType().getSqlTypeName();
          final ValueType outputType = Calcites.getValueTypeForSqlTypeName(sqlTypeName);
          if (outputType == null) {
            throw new ISE("Cannot translate sqlTypeName[%s] to Druid type for field[%s]", sqlTypeName, outputName);
          }
          final DimensionSpec dimensionSpec = rex.toDimensionSpec(sourceRowSignature, outputName, columnType);

          if (dimensionSpec == null) {
            // Really should have been possible due to the checks above.
            throw new ISE("WTF?! Could not create DimensionSpec for rowExtraction[%s].", rex);
          }

          dimensions.add(dimensionSpec);
          rowOrder.add(outputName);
        } else if (extractionFn == null && !column.equals(Column.TIME_COLUMN_NAME)) {
          // Add to metrics.
          metrics.add(column);
          rowOrder.add(column);
        } else if (extractionFn == null && column.equals(Column.TIME_COLUMN_NAME)) {
          // This is __time.
          rowOrder.add(Column.TIME_COLUMN_NAME);
        } else {
          // Don't know what to do!
          return;
        }
      }

      call.transformTo(
          druidRel.withQueryBuilder(
              druidRel.getQueryBuilder()
                      .withSelectProjection(
                          new SelectProjection(project, dimensions, metrics),
                          rowOrder
                      )
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
