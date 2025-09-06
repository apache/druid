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

import com.google.common.collect.Lists;
import org.apache.calcite.rel.RelNode;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.msq.logical.stages.JoinStage;
import org.apache.druid.msq.logical.stages.LogicalStage;
import org.apache.druid.msq.logical.stages.OffsetLimitStage;
import org.apache.druid.msq.logical.stages.ReadStage;
import org.apache.druid.msq.logical.stages.SortStage;
import org.apache.druid.msq.logical.stages.UnnestStage;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.querygen.DruidQueryGenerator.DruidNodeStack;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.apache.druid.sql.calcite.rel.logical.DruidJoin;
import org.apache.druid.sql.calcite.rel.logical.DruidLogicalNode;
import org.apache.druid.sql.calcite.rel.logical.DruidSort;
import org.apache.druid.sql.calcite.rule.logical.DruidUnnest;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Translates the logical plan defined by the {@link DruidLogicalNode} into a
 * {@link LogicalStage} nodes.
 *
 * The translation should be executed as a single pass over the logical plan.
 *
 * During translation all stages have access to {@link DruidNodeStack} which
 * contain all current parents of the current stage.
 */
public class DruidLogicalToQueryDefinitionTranslator
{
  private PlannerContext plannerContext;

  public DruidLogicalToQueryDefinitionTranslator(PlannerContext plannerContext)
  {
    this.plannerContext = plannerContext;
  }

  /**
   * Executes the translation of the logical plan into a query definition.
   */
  public LogicalStage translate(DruidLogicalNode relRoot)
  {
    DruidNodeStack stack = new DruidNodeStack(plannerContext);
    stack.push(relRoot);
    LogicalStage logicalStage = buildStageFor(stack);
    return logicalStage;
  }

  /**
   * Builds a logical stage for the given input.
   *
   * Conducts building the stage by first building all the inputs and then the stage itself.
   * It should build the stage corresponding to stack#peekNode
   *
   * @throws DruidException if the stage cannot be built
   */
  private LogicalStage buildStageFor(DruidNodeStack stack)
  {
    List<LogicalStage> inputStages = buildInputStages(stack);
    DruidLogicalNode node = stack.getNode();
    if (inputStages.size() == 0) {
      Optional<ReadStage> stage = ReadStage.buildReadStage(stack);
      if (stage.isPresent()) {
        return stage.get();
      }
    }
    if (inputStages.size() == 1) {
      LogicalStage inputStage = inputStages.get(0);
      LogicalStage newStage = inputStage.extendWith(stack);
      if (newStage != null) {
        return newStage;
      }
      newStage = makeSequenceStage(inputStage, stack);
      if (newStage != null) {
        return newStage;
      }
    } else {
      LogicalStage newStage = buildMultiInputStage(inputStages, stack);
      if (newStage != null) {
        return newStage;
      }
    }
    throw DruidException.defensive().build("Unable to process relNode[%s]", node);
  }

  private LogicalStage buildMultiInputStage(List<LogicalStage> inputStages, DruidNodeStack stack)
  {
    if (stack.getNode() instanceof DruidJoin) {
      return JoinStage.buildJoinStage(inputStages, stack);
    }
    return null;
  }

  private LogicalStage makeSequenceStage(LogicalStage inputStage, DruidNodeStack stack)
  {
    if (stack.getNode() instanceof DruidSort) {
      DruidSort sort = (DruidSort) stack.getNode();
      List<OrderByColumnSpec> orderBySpecs = DruidQuery.buildOrderByColumnSpecs(inputStage.getLogicalRowSignature(), sort);
      List<KeyColumn> keyColumns = Lists.transform(orderBySpecs, KeyColumn::fromOrderByColumnSpec);
      SortStage sortStage = new SortStage(inputStage, keyColumns);

      if (sort.hasLimitOrOffset()) {
        return new OffsetLimitStage(sortStage, sort.getOffsetLimit());
      } else {
        return sortStage;
      }
    }
    if (stack.getNode() instanceof DruidUnnest) {
      return UnnestStage.buildUnnestStage(inputStage, stack);
    }
    return new ReadStage(inputStage.getLogicalRowSignature(), LogicalInputSpec.of(inputStage)).extendWith(stack);
  }


  private List<LogicalStage> buildInputStages(DruidNodeStack stack)
  {
    List<LogicalStage> inputStages = new ArrayList<>();
    List<RelNode> inputs = stack.getNode().getInputs();
    for (RelNode input : inputs) {
      stack.push((DruidLogicalNode) input, inputStages.size());
      inputStages.add(buildStageFor(stack));
      stack.pop();
    }
    return inputStages;
  }
}
