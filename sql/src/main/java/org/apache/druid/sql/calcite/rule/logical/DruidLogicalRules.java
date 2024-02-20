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

package org.apache.druid.sql.calcite.rule.logical;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.logical.DruidLogicalConvention;

import java.util.ArrayList;
import java.util.List;


public class DruidLogicalRules
{
  private final PlannerContext plannerContext;

  public DruidLogicalRules(PlannerContext plannerContext)
  {
    this.plannerContext = plannerContext;
  }

  public List<RelOptRule> rules()
  {
    return new ArrayList<>(
        ImmutableList.of(
            new DruidTableScanRule(
                LogicalTableScan.class,
                Convention.NONE,
                DruidLogicalConvention.instance(),
                DruidTableScanRule.class.getSimpleName()
            ),
            new DruidAggregateRule(
                LogicalAggregate.class,
                Convention.NONE,
                DruidLogicalConvention.instance(),
                DruidAggregateRule.class.getSimpleName(),
                plannerContext
            ),
            new DruidSortRule(
                LogicalSort.class,
                Convention.NONE,
                DruidLogicalConvention.instance(),
                DruidSortRule.class.getSimpleName()
            ),
            new DruidProjectRule(
                LogicalProject.class,
                Convention.NONE,
                DruidLogicalConvention.instance(),
                DruidProjectRule.class.getSimpleName()
            ),
            new DruidFilterRule(
                LogicalFilter.class,
                Convention.NONE,
                DruidLogicalConvention.instance(),
                DruidFilterRule.class.getSimpleName()
            ),
            new DruidValuesRule(
                LogicalValues.class,
                Convention.NONE,
                DruidLogicalConvention.instance(),
                DruidValuesRule.class.getSimpleName()
            ),
            new DruidWindowRule(
                Window.class,
                Convention.NONE,
                DruidLogicalConvention.instance(),
                DruidWindowRule.class.getSimpleName()
            ),
            new DruidUnionRule(
                LogicalUnion.class,
                Convention.NONE,
                DruidLogicalConvention.instance(),
                DruidUnionRule.class.getSimpleName()
            )
        )
    );
  }
}
