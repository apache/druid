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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Project;
import org.apache.druid.sql.calcite.rel.DruidOuterQueryRel;
import org.apache.druid.sql.calcite.rel.DruidRel;
import org.apache.druid.sql.calcite.rel.PartialDruidQuery;

public class DruidWindowProjectQueryRule extends DruidRules.DruidQueryRule<Project>
{
  public DruidWindowProjectQueryRule()
  {
    super(
        Project.class,
        PartialDruidQuery.Stage.WINDOW_PROJECT,
        PartialDruidQuery::withWindowProject
    );
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    final Project project = call.rel(0);
    final DruidRel<?> druidRel = call.rel(1);

    if (project.isMapping()) {
      // If the project is only doing a mapping, then we can att it to the WINDOW_PROJECT phase, so try that first.
      final DruidRel<?> newRel = druidRel.withPartialQuery(druidRel.getPartialDruidQuery().withWindowProject(project));
      if (newRel.isValidDruidQuery()) {
        call.transformTo(newRel);
        return;
      }
    }

    // If it's not a mapping or it didn't work on the WINDOW_PROJECT phase for some reason, make it the SELECT_PROJECT
    // on an outer query instead.
    final DruidOuterQueryRel outerQueryRel = DruidOuterQueryRel.create(
        druidRel,
        PartialDruidQuery.createOuterQuery(druidRel.getPartialDruidQuery())
                         .withSelectProject(project)
    );
    if (outerQueryRel.isValidDruidQuery()) {
      call.transformTo(outerQueryRel);
    }
  }
}
