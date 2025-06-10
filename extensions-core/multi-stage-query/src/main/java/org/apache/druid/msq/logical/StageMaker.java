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

import org.apache.druid.error.DruidException;
import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.msq.kernel.MixShuffleSpec;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.kernel.StageDefinitionBuilder;
import org.apache.druid.msq.logical.LogicalStageBuilder.AbstractFrameProcessorStage;
import org.apache.druid.msq.logical.LogicalStageBuilder.AbstractShuffleStage;
import org.apache.druid.msq.querykit.BaseFrameProcessorFactory;
import org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessorFactory;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import java.util.ArrayList;
import java.util.List;

public class StageMaker
{

  /** Provides ids for the stages. */
  private int stageIdSeq = 0;

  private final PlannerContext plannerContext;

  private List<StageDefinitionBuilder> stageBuilders = new ArrayList<>();

  public StageMaker(PlannerContext plannerContext)
  {
    this.plannerContext = plannerContext;
  }

  public static ScanQueryFrameProcessorFactory makeScanFrameProcessor(
      VirtualColumns virtualColumns,
      RowSignature signature,
      DimFilter dimFilter)
  {
    return ScanQueryFrameProcessorFactory.makeScanFrameProcessor(virtualColumns, signature, dimFilter);
  }

  private int getNextStageId()
  {
    return stageIdSeq++;
  }

  public StageDefinitionBuilder buildStage(LogicalStage stage)
  {
    if (stage instanceof AbstractFrameProcessorStage) {
      return buildFrameProcessorStage((AbstractFrameProcessorStage) stage);
    }
    if (stage instanceof AbstractShuffleStage) {
      return buildShuffleStage((AbstractShuffleStage) stage);
    }
    throw DruidException.defensive("Cannot build type [%s]", stage.getClass().getSimpleName());
  }

  private StageDefinitionBuilder buildFrameProcessorStage(AbstractFrameProcessorStage frameProcessorStage)
  {
    List<LogicalInputSpec> inputs = frameProcessorStage.inputSpecs;
    List<InputSpec> inputSpecs = new ArrayList<>();
    for (LogicalInputSpec dagInputSpec : inputs) {
      inputSpecs.add(dagInputSpec.toInputSpec(this));
    }
    BaseFrameProcessorFactory frameProcessor = frameProcessorStage.buildFrameProcessor(this);
    StageDefinitionBuilder sdb = newStageDefinitionBuilder();
    sdb.inputs(inputSpecs);
    sdb.signature(frameProcessorStage.getLogicalRowSignature());
    sdb.processorFactory(frameProcessor);
    sdb.shuffleSpec(MixShuffleSpec.instance());
    return sdb;
  }

  private StageDefinitionBuilder buildShuffleStage(AbstractShuffleStage stage)
  {
    List<LogicalInputSpec> inputs = stage.inputSpecs;
    List<InputSpec> inputSpecs = new ArrayList<>();
    for (LogicalInputSpec dagInputSpec : inputs) {
      inputSpecs.add(dagInputSpec.toInputSpec(this));
    }
    StageDefinitionBuilder sdb = newStageDefinitionBuilder();
    sdb.inputs(inputSpecs);
    sdb.signature(stage.getSignature());
    sdb.processorFactory(makeScanFrameProcessor(VirtualColumns.EMPTY, stage.getSignature(), null));
    sdb.shuffleSpec(stage.buildShuffleSpec());
    return sdb;
  }

  private StageDefinitionBuilder newStageDefinitionBuilder()
  {
    StageDefinitionBuilder builder = StageDefinition.builder(getNextStageId());
    stageBuilders.add(builder);
    return builder;
  }

  public QueryDefinition buildQueryDefinition()
  {
    return QueryDefinition.create(makeStages(), plannerContext.queryContext());
  }

  private List<StageDefinition> makeStages()
  {
    List<StageDefinition> ret = new ArrayList<>();
    for (StageDefinitionBuilder stageDefinitionBuilder : stageBuilders) {
      ret.add(stageDefinitionBuilder.build(getIdForBuilder()));
    }
    return ret;
  }

  private String getIdForBuilder()
  {
    String dartQueryId = plannerContext.queryContext().getString(QueryContexts.CTX_DART_QUERY_ID);
    if (dartQueryId != null) {
      return dartQueryId;
    }
    return plannerContext.getSqlQueryId();
  }
}