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

import com.google.common.collect.Lists;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.ExtractionDimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.segment.column.Column;
import io.druid.segment.column.ValueType;
import io.druid.sql.calcite.expression.Expressions;
import io.druid.sql.calcite.expression.RowExtraction;
import io.druid.sql.calcite.rel.DruidRel;
import io.druid.sql.calcite.rel.SelectProjection;
import io.druid.sql.calcite.table.DruidTables;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Project;

import java.util.List;

public class DruidSelectProjectionRule extends RelOptRule
{
  private static final DruidSelectProjectionRule INSTANCE = new DruidSelectProjectionRule();

  private DruidSelectProjectionRule()
  {
    super(operand(Project.class, operand(DruidRel.class, none())));
  }

  public static DruidSelectProjectionRule instance()
  {
    return INSTANCE;
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    final Project project = call.rel(0);
    final DruidRel druidRel = call.rel(1);

    if (druidRel.getQueryBuilder().getSelectProjection() != null
        || druidRel.getQueryBuilder().getGrouping() != null
        || druidRel.getQueryBuilder().getLimitSpec() != null) {
      return;
    }

    // Only push in projections that can be used by the Select query.
    // Leave anything more complicated to DruidAggregateProjectRule for possible handling in a GroupBy query.

    final List<DimensionSpec> dimensions = Lists.newArrayList();
    final List<String> metrics = Lists.newArrayList();
    final List<String> rowOrder = Lists.newArrayList();

    int dimOutputNameCounter = 0;
    for (int i = 0; i < project.getRowType().getFieldCount(); i++) {
      final RowExtraction rex = Expressions.toRowExtraction(
          DruidTables.rowOrder(druidRel.getDruidTable()),
          project.getChildExps().get(i)
      );

      if (rex == null) {
        return;
      }

      final String column = rex.getColumn();
      final ExtractionFn extractionFn = rex.getExtractionFn();

      // Check if this field should be a dimension, a metric, or a reference to __time.
      final ValueType columnType = druidRel.getDruidTable()
                                           .getColumnType(druidRel.getDruidTable().getColumnNumber(column));

      if (columnType == ValueType.STRING || (column.equals(Column.TIME_COLUMN_NAME) && extractionFn != null)) {
        // Add to dimensions.
        do {
          dimOutputNameCounter++;
        } while (druidRel.getDruidTable().getColumnNumber(GroupByRules.dimOutputName(dimOutputNameCounter)) >= 0);
        final String outputName = GroupByRules.dimOutputName(dimOutputNameCounter);
        final DimensionSpec dimensionSpec = extractionFn == null
                                            ? new DefaultDimensionSpec(column, outputName)
                                            : new ExtractionDimensionSpec(column, outputName, extractionFn);
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
