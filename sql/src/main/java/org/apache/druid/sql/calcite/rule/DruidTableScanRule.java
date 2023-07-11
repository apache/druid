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
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.DruidQueryRel;
import org.apache.druid.sql.calcite.table.DruidTable;

public class DruidTableScanRule extends RelOptRule
{
  private final PlannerContext plannerContext;

  public DruidTableScanRule(final PlannerContext plannerContext)
  {
    super(operand(LogicalTableScan.class, any()));
    this.plannerContext = plannerContext;
  }

  @Override
  public void onMatch(final RelOptRuleCall call)
  {
    final LogicalTableScan scan = call.rel(0);
    final RelOptTable table = scan.getTable();
    final DruidTable druidTable = table.unwrap(DruidTable.class);
    if (druidTable != null) {
      call.transformTo(
          DruidQueryRel.scanTable(scan, table, druidTable, plannerContext)
      );
    }
  }
}
