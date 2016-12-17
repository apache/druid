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

import io.druid.query.groupby.orderby.DefaultLimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.segment.column.Column;
import io.druid.sql.calcite.rel.DruidRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Sort;

import java.util.List;

public class DruidSelectSortRule extends RelOptRule
{
  private static final DruidSelectSortRule INSTANCE = new DruidSelectSortRule();

  private DruidSelectSortRule()
  {
    super(operand(Sort.class, operand(DruidRel.class, none())));
  }

  public static DruidSelectSortRule instance()
  {
    return INSTANCE;
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    final Sort sort = call.rel(0);
    final DruidRel druidRel = call.rel(1);

    if (druidRel.getQueryBuilder().getGrouping() != null
        || druidRel.getQueryBuilder().getLimitSpec() != null) {
      return;
    }

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
