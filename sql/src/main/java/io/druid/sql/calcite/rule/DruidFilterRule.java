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

import io.druid.query.filter.DimFilter;
import io.druid.sql.calcite.expression.Expressions;
import io.druid.sql.calcite.rel.DruidRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Filter;

public class DruidFilterRule extends RelOptRule
{
  private static final DruidFilterRule INSTANCE = new DruidFilterRule();

  private DruidFilterRule()
  {
    super(operand(Filter.class, operand(DruidRel.class, none())));
  }

  public static DruidFilterRule instance()
  {
    return INSTANCE;
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    final Filter filter = call.rel(0);
    final DruidRel druidRel = call.rel(1);

    if (druidRel.getQueryBuilder().getFilter() != null
        || druidRel.getQueryBuilder().getSelectProjection() != null
        || druidRel.getQueryBuilder().getGrouping() != null) {
      return;
    }

    final DimFilter dimFilter = Expressions.toFilter(
        druidRel.getDruidTable(),
        druidRel.getQueryBuilder().getRowOrder(),
        filter.getCondition()
    );
    if (dimFilter != null) {
      call.transformTo(
          druidRel.withQueryBuilder(druidRel.getQueryBuilder().withFilter(dimFilter))
      );
    }
  }
}
