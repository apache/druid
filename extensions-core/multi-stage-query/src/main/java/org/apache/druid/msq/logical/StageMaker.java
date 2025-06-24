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
import org.apache.druid.msq.exec.StageProcessor;
import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.msq.kernel.MixShuffleSpec;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.kernel.StageDefinitionBuilder;
import org.apache.druid.msq.logical.stages.AbstractFrameProcessorStage;
import org.apache.druid.msq.logical.stages.AbstractShuffleStage;
import org.apache.druid.msq.logical.stages.LogicalStage;
import org.apache.druid.msq.querykit.scan.ScanQueryStageProcessor;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds {@link QueryDefinition} from {@link LogicalStage}-s.
 */
public class StageMaker
{
  /** Provides ids for the stages. */
  private int stageIdSeq = 0;

  private final PlannerContext plannerContext;

  private Map<LogicalStage, StageDefinitionBuilder> builtStages = new IdentityHashMap<>();

  public StageMaker(PlannerContext plannerContext)
  {
    this.plannerContext = plannerContext;
  }

  public static ScanQueryStageProcessor makeScanStageProcessor(
      VirtualColumns virtualColumns,
      RowSignature signature,
      DimFilter dimFilter)
  {
    return ScanQueryStageProcessor.makeScanStageProcessor(virtualColumns, signature, dimFilter);
  }

  public StageDefinitionBuilder buildStage(LogicalStage stage)
  {
    if (builtStages.get(stage) != null) {
      return builtStages.get(stage);
    }
    StageDefinitionBuilder stageDef = buildStageInternal(stage);
    builtStages.put(stage, stageDef);
    return stageDef;
  }

  private StageDefinitionBuilder buildStageInternal(LogicalStage stage)
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
    List<LogicalInputSpec> inputs = frameProcessorStage.getInputSpecs();
    List<InputSpec> inputSpecs = new ArrayList<>();
    for (LogicalInputSpec dagInputSpec : inputs) {
      inputSpecs.add(dagInputSpec.toInputSpec(this));
    }
    StageProcessor<?, ?> stageProcessor = frameProcessorStage.buildStageProcessor(this);
    StageDefinitionBuilder sdb = newStageDefinitionBuilder();
    sdb.inputs(inputSpecs);
    sdb.signature(frameProcessorStage.getLogicalRowSignature());
    sdb.processor(stageProcessor);
    sdb.shuffleSpec(MixShuffleSpec.instance());
    return sdb;
  }

  private StageDefinitionBuilder buildShuffleStage(AbstractShuffleStage stage)
  {
    List<LogicalInputSpec> inputs = stage.getInputSpecs();
    List<InputSpec> inputSpecs = new ArrayList<>();
    for (LogicalInputSpec dagInputSpec : inputs) {
      inputSpecs.add(dagInputSpec.toInputSpec(this));
    }
    StageDefinitionBuilder sdb = newStageDefinitionBuilder();
    sdb.inputs(inputSpecs);
    sdb.signature(stage.getRowSignature());
    sdb.processor(makeScanStageProcessor(VirtualColumns.EMPTY, stage.getRowSignature(), null));
    sdb.shuffleSpec(stage.buildShuffleSpec());
    return sdb;
  }

  private StageDefinitionBuilder newStageDefinitionBuilder()
  {
    return StageDefinition.builder(getNextStageId());
  }

  private int getNextStageId()
  {
    return stageIdSeq++;
  }

  public QueryDefinition buildQueryDefinition()
  {
    return QueryDefinition.create(makeStages(), plannerContext.queryContext());
  }

  private List<StageDefinition> makeStages()
  {
    List<StageDefinition> ret = new ArrayList<>();
    for (StageDefinitionBuilder stageDefinitionBuilder : builtStages.values()) {
      ret.add(stageDefinitionBuilder.build(getIdForBuilder()));
    }
    ret.sort(Comparator.comparing(StageDefinition::getStageNumber));
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
