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

package org.apache.druid.msq.logical.stages;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.querygen.DruidQueryGenerator.DruidNodeStack;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.apache.druid.sql.calcite.rel.Grouping;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;
import org.apache.druid.sql.calcite.rel.logical.DruidAggregate;

class ProjectStage extends FilterStage
{
  protected final RowSignature projectInputSignature;

  public ProjectStage(FilterStage stage, VirtualColumnRegistry virtualColumnRegistry, RowSignature signature)
  {
    super(stage, virtualColumnRegistry, signature);
    projectInputSignature = stage.signature;
  }

  public ProjectStage(ProjectStage stage, RowSignature rowSignature)
  {
    super(stage, stage.virtualColumnRegistry, rowSignature);
    projectInputSignature = stage.projectInputSignature;
  }

  @Override
  public LogicalStage extendWith(DruidNodeStack stack)
  {
    if (stack.getNode() instanceof DruidAggregate) {
      DruidAggregate aggregate = (DruidAggregate) stack.getNode();
      RelNode aggregateInput = aggregate.getInput();
      Project selectProject = aggregateInput instanceof Project ? (Project) aggregateInput : null;

      Grouping grouping = DruidQuery.buildGrouping(
          aggregate,
          selectProject,
          null,
          stack.getPlannerContext(),
          projectInputSignature,
          virtualColumnRegistry,
          false
      );

      return GroupByStages.buildStages(this, grouping);
    }
    return null;
  }
}
