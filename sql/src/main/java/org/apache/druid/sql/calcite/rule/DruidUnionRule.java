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

package org.apache.druid.sql.calcite.rule;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Union;
import org.apache.druid.sql.calcite.rel.DruidRel;
import org.apache.druid.sql.calcite.rel.DruidUnionRel;

import java.util.List;

public class DruidUnionRule extends RelOptRule
{
  private static final DruidUnionRule INSTANCE = new DruidUnionRule();

  private DruidUnionRule()
  {
    super(operand(Union.class, unordered(operand(DruidRel.class, any()))));
  }

  public static DruidUnionRule instance()
  {
    return INSTANCE;
  }

  @Override
  public void onMatch(final RelOptRuleCall call)
  {
    final Union unionRel = call.rel(0);
    final DruidRel someDruidRel = call.rel(1);
    final List<RelNode> inputs = unionRel.getInputs();

    if (unionRel.all) {
      // Can only do UNION ALL.
      call.transformTo(DruidUnionRel.create(
          someDruidRel.getQueryMaker(),
          unionRel.getRowType(),
          inputs,
          -1
      ));
    }
  }
}
