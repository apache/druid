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
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.sql.calcite.rel.DruidUnnestRel;

public class DruidFilterUnnestRule extends RelOptRule
{
  private static final DruidFilterUnnestRule INSTANCE = new DruidFilterUnnestRule();

  private DruidFilterUnnestRule()
  {
    super(
        operand(
            Filter.class,
            operand(DruidUnnestRel.class, any())
        )
    );
  }

  public static DruidFilterUnnestRule instance()
  {
    return INSTANCE;
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    final Filter filter = call.rel(0);
    final DruidUnnestRel unnestDatasourceRel = call.rel(1);
    DruidUnnestRel newRel = unnestDatasourceRel.withFilter(filter);
    call.transformTo(newRel);
  }

  // This is for a special case of handling selector filters
  // on top of UnnestDataSourceRel when Calcite adds an extra
  // LogicalProject on the LogicalFilter. For e.g. #122 here
  // SELECT d3 FROM druid.numfoo, UNNEST(MV_TO_ARRAY(dim3)) as unnested (d3) where d3='b'
  // 126:LogicalProject(d3=[$17])
  //  124:LogicalCorrelate(subset=[rel#125:Subset#6.NONE.[]], correlation=[$cor0], joinType=[inner], requiredColumns=[{3}])
  //    8:LogicalTableScan(subset=[rel#114:Subset#0.NONE.[]], table=[[druid, numfoo]])
  //    122:LogicalProject(subset=[rel#123:Subset#5.NONE.[]], d3=[CAST('b':VARCHAR):VARCHAR])
  //      120:LogicalFilter(subset=[rel#121:Subset#4.NONE.[]], condition=[=($0, 'b')])
  //        118:Uncollect(subset=[rel#119:Subset#3.NONE.[]])
  //          116:LogicalProject(subset=[rel#117:Subset#2.NONE.[]], EXPR$0=[MV_TO_ARRAY($cor0.dim3)])
  //            9:LogicalValues(subset=[rel#115:Subset#1.NONE.[0]], tuples=[[{ 0 }]])

  // This logical project does a type cast only which Druid already has information about
  // So we can skip this LogicalProject only if it is a CAST for strings or LITERALS for other types
  // Extensive unit tests can be found in {@link CalciteArraysQueryTest}

  static class DruidProjectOnUnnestRule extends RelOptRule
  {
    private static final DruidProjectOnUnnestRule INSTANCE = new DruidProjectOnUnnestRule();

    private DruidProjectOnUnnestRule()
    {
      super(
          operand(
              Project.class,
              operand(DruidUnnestRel.class, any())
          )
      );
    }

    public static DruidProjectOnUnnestRule instance()
    {
      return INSTANCE;
    }

    @Override
    public boolean matches(RelOptRuleCall call)
    {
      final Project rightP = call.rel(0);
      if (rightP.getProjects().size() > 0) {
        final SqlKind rightProjectKind = rightP.getProjects().get(0).getKind();
        final SqlTypeName projectType = rightP.getProjects().get(0).getType().getSqlTypeName();
        final SqlTypeName unnestDataType = call.rel(1).getRowType().getFieldList().get(0).getType().getSqlTypeName();
        // allow rule to trigger only if project involves a cast on the same row type
        return rightP.getProjects().size() == 1 && ((rightProjectKind == SqlKind.CAST || rightProjectKind == SqlKind.LITERAL)
                                                     && projectType == unnestDataType);
      }
      return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call)
    {
      final DruidUnnestRel unnestDatasourceRel = call.rel(1);
      call.transformTo(unnestDatasourceRel);
    }
  }
}
