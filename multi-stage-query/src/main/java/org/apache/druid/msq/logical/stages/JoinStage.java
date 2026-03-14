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

import com.google.common.collect.Lists;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.msq.exec.StageProcessor;
import org.apache.druid.msq.kernel.HashShuffleSpec;
import org.apache.druid.msq.kernel.ShuffleSpec;
import org.apache.druid.msq.logical.LogicalInputSpec;
import org.apache.druid.msq.logical.StageMaker;
import org.apache.druid.msq.querykit.QueryKitUtils;
import org.apache.druid.msq.querykit.common.SortMergeJoinStageProcessor;
import org.apache.druid.query.JoinAlgorithm;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.querygen.DruidQueryGenerator.DruidNodeStack;
import org.apache.druid.sql.calcite.planner.querygen.SourceDescProducer.SourceDesc;
import org.apache.druid.sql.calcite.rel.DruidJoinQueryRel;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;
import org.apache.druid.sql.calcite.rel.logical.DruidJoin;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a stage that reads data from input sources.
 */
public class JoinStage
{

  static class ShuffleStage extends AbstractShuffleStage
  {
    protected final List<KeyColumn> keyColumns;

    public ShuffleStage(LogicalStage inputStage, List<KeyColumn> keyColumns)
    {
      super(
          QueryKitUtils.sortableSignature(inputStage.getLogicalRowSignature(), keyColumns),
          LogicalInputSpec.of(inputStage)
      );
      this.keyColumns = keyColumns;
    }

    @Override
    public RowSignature getLogicalRowSignature()
    {
      return inputSpecs.get(0).getRowSignature();
    }

    @Override
    public ShuffleSpec buildShuffleSpec()
    {
      final ClusterBy clusterBy = new ClusterBy(keyColumns, 0);
      return new HashShuffleSpec(clusterBy, 1);
    }

    @Override
    public LogicalStage extendWith(DruidNodeStack stack)
    {
      return null;
    }
  }

  public static class SortMergeStage extends AbstractFrameProcessorStage
  {

    private String rightPrefix;
    private JoinConditionAnalysis conditionAnalysis;
    private JoinType joinType;

    public SortMergeStage(RowSignature signature, List<LogicalInputSpec> inputs, String rightPrefix,
        JoinConditionAnalysis conditionAnalysis, JoinType joinType)
    {
      super(signature, inputs);
      this.rightPrefix = rightPrefix;
      this.conditionAnalysis = conditionAnalysis;
      this.joinType = joinType;
    }

    @Override
    public LogicalStage extendWith(DruidNodeStack stack)
    {
      return null;
    }

    @Override
    public StageProcessor<?, ?> buildStageProcessor(StageMaker stageMaker)
    {
      return new SortMergeJoinStageProcessor(
          rightPrefix,
          conditionAnalysis,
          joinType
      );
    }
  }

  /** similar to {@link DruidJoinQueryRel#buildJoinSourceDesc} */
  public static LogicalStage buildJoinStage(List<LogicalStage> inputStages, DruidNodeStack stack)
  {
    DruidJoin join = (DruidJoin) stack.getNode();
    if (join.getJoinAlgorithm(stack.getPlannerContext()) == JoinAlgorithm.SORT_MERGE) {
      return buildMergeJoin(inputStages, stack, join);
    } else {
      return buildBroadcastJoin(inputStages, stack, join);
    }
  }

  private static LogicalStage buildBroadcastJoin(List<LogicalStage> inputStages, DruidNodeStack stack, DruidJoin join)
  {
    PlannerContext plannerContext = stack.getPlannerContext();
    List<LogicalInputSpec> inputDescs = new ArrayList<>();
    inputDescs.add(LogicalInputSpec.of(inputStages.get(0)));
    for (int i = 1; i < inputStages.size(); i++) {
      inputDescs.add(LogicalInputSpec.of(inputStages.get(i), i, LogicalInputSpec.InputProperty.BROADCAST));
    }
    SourceDesc unnestSD = join.getSourceDesc(plannerContext, Lists.transform(inputDescs, LogicalInputSpec::getSourceDesc));
    return new SegmentMapStage(unnestSD, inputDescs);
  }

  private static LogicalStage buildMergeJoin(List<LogicalStage> inputStages, DruidNodeStack stack, DruidJoin join)
  {
    String prefix = findUnusedJoinPrefix(inputStages.get(0).getRowSignature());

    RowSignature signature = RowSignature.builder()
        .addAll(inputStages.get(0).getLogicalRowSignature())
        .addAll(inputStages.get(1).getLogicalRowSignature().withPrefix(prefix))
        .build();

    PlannerContext plannerContext = stack.getPlannerContext();
    VirtualColumnRegistry virtualColumnRegistry = VirtualColumnRegistry.create(
        signature,
        plannerContext.getExpressionParser(),
        plannerContext.getPlannerConfig().isForceExpressionVirtualColumns()
    );
    plannerContext.setJoinExpressionVirtualColumnRegistry(virtualColumnRegistry);

    // Generate the condition for this join as a Druid expression.
    final DruidExpression condition = Expressions.toDruidExpression(
        plannerContext,
        signature,
        join.getCondition()
    );

    // Unsetting it to avoid any VC Registry leaks incase there are multiple
    // druid quries for the SQL
    // It should be fixed soon with changes in interface for
    // SqlOperatorConversion and Expressions bridge class
    plannerContext.setJoinExpressionVirtualColumnRegistry(null);

    if (!virtualColumnRegistry.isEmpty()) {
      throw DruidException.defensive("Not sure how to handle this right now - it should be fixed");
    }

    JoinConditionAnalysis analysis = JoinConditionAnalysis.forExpression(
        condition.getExpression(),
        plannerContext.parseExpression(condition.getExpression()),
        prefix
    );

    // Partition by keys given by the join condition.
    final List<List<KeyColumn>> partitionKeys = SortMergeJoinStageProcessor.toKeyColumns(
        SortMergeJoinStageProcessor.validateCondition(analysis)
    );

    List<LogicalStage> shuffleStages = new ArrayList<>();
    for (int i = 0; i < inputStages.size(); i++) {
      LogicalStage inputStage = inputStages.get(i);
      shuffleStages.add(new ShuffleStage(inputStage, partitionKeys.get(i)));

    }

    return new SortMergeStage(
        signature,
        Lists.transform(shuffleStages, LogicalInputSpec::of),
        prefix,
        analysis,
        DruidJoinQueryRel.toDruidJoinType(join.getJoinType())
    );
  }

  private static String findUnusedJoinPrefix(RowSignature rowSignature)
  {
    List<String> leftColumnNames = rowSignature.getColumnNames();
    return Calcites.findUnusedPrefixForDigits("j", leftColumnNames) + "0";
  }
}
