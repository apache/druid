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
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.msq.input.inline.InlineInputSpec;
import org.apache.druid.msq.input.table.TableInputSpec;
import org.apache.druid.msq.logical.LogicalStageBuilder.ReadStage;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.querygen.DruidQueryGenerator.DruidNodeStack;
import org.apache.druid.sql.calcite.planner.querygen.SourceDescProducer.SourceDesc;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.apache.druid.sql.calcite.rel.logical.DruidLogicalNode;
import org.apache.druid.sql.calcite.rel.logical.DruidSort;
import org.apache.druid.sql.calcite.rel.logical.DruidTableScan;
import org.apache.druid.sql.calcite.rel.logical.DruidValues;

import java.util.ArrayList;
import java.util.Collections;
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
  private LogicalStageBuilder stageBuilder;

  public DruidLogicalToQueryDefinitionTranslator(PlannerContext plannerContext)
  {
    this.plannerContext = plannerContext;
    this.stageBuilder = new LogicalStageBuilder(plannerContext);
  }

  /**
   * Executes the translation of the logical plan into a query definition.
   */
  public LogicalStage translate(DruidLogicalNode relRoot)
  {
    DruidNodeStack stack = new DruidNodeStack();
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
      Optional<ReadStage> stage = buildReadStage(node);
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
    }
    throw DruidException.defensive().build("Unable to process relNode[%s]", node);
  }

  private Optional<ReadStage> buildReadStage(DruidLogicalNode node)
  {
    if (node instanceof DruidValues) {
      return translateValues((DruidValues) node);
    }
    if (node instanceof DruidTableScan) {
      return translateTableScan((DruidTableScan) node);
    }
    return Optional.empty();
  }

  private LogicalStage makeSequenceStage(LogicalStage inputStage, DruidNodeStack stack)
  {
    if (stack.getNode() instanceof DruidSort) {
      DruidSort sort = (DruidSort) stack.getNode();
      if (sort.hasLimitOrOffset()) {
        throw DruidException.defensive("Sort with limit or offset is not supported in MSQ logical stage builder");
      }
      List<OrderByColumnSpec> orderBySpecs = DruidQuery.buildOrderByColumnSpecs(inputStage.getLogicalRowSignature(), sort);
      List<KeyColumn> keyColumns = Lists.transform(orderBySpecs, KeyColumn::fromOrderByColumnSpec);
      return stageBuilder.makeSortStage(inputStage, keyColumns);
    }

    return stageBuilder.makeReadStage(inputStage.getLogicalRowSignature(), LogicalInputSpec.of(inputStage)).extendWith(stack);
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

  private Optional<ReadStage> translateTableScan(DruidTableScan node)
  {
    SourceDesc sd = node.getSourceDesc(plannerContext, Collections.emptyList());
    TableDataSource ids = (TableDataSource) sd.dataSource;
    TableInputSpec inputSpec = new TableInputSpec(ids.getName(), Intervals.ONLY_ETERNITY, null, null);
    ReadStage stage = stageBuilder.makeReadStage(sd.rowSignature, LogicalInputSpec.of(inputSpec));
    return Optional.of(stage);
  }

  private Optional<ReadStage> translateValues(DruidValues node)
  {
    SourceDesc sd = node.getSourceDesc(plannerContext, Collections.emptyList());
    InlineDataSource ids = (InlineDataSource) sd.dataSource;
    InlineInputSpec inputSpec = new InlineInputSpec(ids);
    ReadStage stage = stageBuilder.makeReadStage(sd.rowSignature, LogicalInputSpec.of(inputSpec));
    return Optional.of(stage);
  }
}
