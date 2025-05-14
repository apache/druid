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

package org.apache.druid.msq.logical;

import org.apache.calcite.rel.RelNode;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.logical.LogicalStageBuilder.RootStage;
import org.apache.druid.msq.querykit.DataSourcePlan;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.querygen.DruidQueryGenerator.DruidNodeStack;
import org.apache.druid.sql.calcite.planner.querygen.SourceDescProducer.SourceDesc;
import org.apache.druid.sql.calcite.rel.logical.DruidLogicalNode;
import org.apache.druid.sql.calcite.rel.logical.DruidTableScan;
import org.apache.druid.sql.calcite.rel.logical.DruidValues;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class DruidLogicalToQueryDefinitionTranslator
{
  private PlannerContext plannerContext;
  private LogicalStageBuilder stageBuilder;

  public DruidLogicalToQueryDefinitionTranslator(PlannerContext plannerContext)
  {
    this.plannerContext = plannerContext;
    this.stageBuilder = new LogicalStageBuilder(plannerContext);
  }

  public QueryDefinition translate(DruidLogicalNode relRoot)
  {
    DruidNodeStack stack = new DruidNodeStack();
    stack.push(relRoot);
    LogicalStage logicalStage = buildStageFor(stack);
    return logicalStage.build();
  }

  private LogicalStage buildStageFor(DruidNodeStack stack)
  {
    List<LogicalStage> newInputs = new ArrayList<>();

    for (RelNode input : stack.peekNode().getInputs()) {
      stack.push((DruidLogicalNode) input, newInputs.size());
      newInputs.add(buildStageFor(stack));
      stack.pop();
    }
    LogicalStage stage = processNodeWithInputs(stack, newInputs);
    return stage;
  }

  private LogicalStage processNodeWithInputs(DruidNodeStack stack, List<LogicalStage> newInputs)
  {
    DruidLogicalNode node = stack.peekNode();
    Optional<RootStage> stage = buildRootStage(node);
    if (stage.isPresent()) {
      return stage.get();
    }
    if (newInputs.size() == 1) {
      LogicalStage inputStage = newInputs.get(0);
      LogicalStage newStage = inputStage.extendWith(stack);
      if (newStage != null) {
        return newStage;
      }
    }
    throw DruidException.defensive().build("Unable to process relNode[%s]", node);
  }

  private Optional<RootStage> buildRootStage(DruidLogicalNode node)
  {
    if (node instanceof DruidValues) {
      return translateValues((DruidValues) node);
    }
    if (node instanceof DruidTableScan) {
      return translateTableScan((DruidTableScan) node);
    }
    return Optional.empty();
  }

  private Optional<RootStage> translateTableScan(DruidTableScan node)
  {
    SourceDesc sd = node.getSourceDesc(plannerContext, Collections.emptyList());
    DataSource ds = sd.dataSource;
    TableDataSource ids = (TableDataSource) ds;
    DataSourcePlan dsp = DataSourcePlan.forTable(
        ids,
        Intervals.ONLY_ETERNITY,
        null, null,
        false
    );
    List<InputSpec> isp = dsp.getInputSpecs();

    RootStage stage = stageBuilder.makeRootStage(sd.rowSignature, isp);
    return Optional.of(stage);
  }

  private Optional<RootStage> translateValues(DruidValues node)
  {
    SourceDesc sd = node.getSourceDesc(plannerContext, Collections.emptyList());
    DataSource ds = sd.dataSource;
    InlineDataSource ids = (InlineDataSource) ds;
    DataSourcePlan dsp = DataSourcePlan.forInline(ids, false);
    List<InputSpec> isp = dsp.getInputSpecs();

    RootStage stage = stageBuilder.makeRootStage(sd.rowSignature, isp);
    return Optional.of(stage);
  }
}
