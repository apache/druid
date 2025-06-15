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

package org.apache.druid.msq.indexing.destination;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import it.unimi.dsi.fastutil.ints.Int2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.msq.indexing.MSQSpec;
import org.apache.druid.msq.indexing.MSQTuningConfig;
import org.apache.druid.msq.indexing.processor.SegmentGeneratorStageProcessor;
import org.apache.druid.msq.input.stage.ReadablePartition;
import org.apache.druid.msq.input.stage.StageInputSlice;
import org.apache.druid.msq.input.stage.StageInputSpec;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.kernel.StageDefinitionBuilder;
import org.apache.druid.msq.kernel.controller.WorkerInputs;
import org.apache.druid.query.Query;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.sql.calcite.planner.ColumnMappings;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class SegmentGenerationStageSpec implements TerminalStageSpec
{
  public static final String TYPE = "segmentGeneration";

  private static final SegmentGenerationStageSpec INSTANCE = new SegmentGenerationStageSpec();

  private SegmentGenerationStageSpec()
  {
  }

  @JsonCreator
  public static SegmentGenerationStageSpec instance()
  {
    return INSTANCE;
  }

  @Override
  public StageDefinitionBuilder constructFinalStage(QueryDefinition queryDef, MSQSpec querySpec, ObjectMapper jsonMapper, Query<?> query)
  {
    final MSQTuningConfig tuningConfig = querySpec.getTuningConfig();
    final ColumnMappings columnMappings = querySpec.getColumnMappings();
    final RowSignature querySignature = queryDef.getFinalStageDefinition().getSignature();
    final ClusterBy queryClusterBy = queryDef.getFinalStageDefinition().getClusterBy();

    // Add a segment-generation stage.
    final DataSchema dataSchema =
        SegmentGenerationUtils.makeDataSchemaForIngestion(querySpec, querySignature, queryClusterBy, columnMappings, jsonMapper, query);

    return StageDefinition.builder(queryDef.getNextStageNumber())
                       .inputs(new StageInputSpec(queryDef.getFinalStageDefinition().getStageNumber()))
                       .maxWorkerCount(tuningConfig.getMaxNumWorkers())
                       .processor(
                           new SegmentGeneratorStageProcessor(
                               dataSchema,
                               columnMappings,
                               tuningConfig
                           )
    );
  }

  public Int2ObjectMap<List<SegmentIdWithShardSpec>> getWorkerInfo(
      final WorkerInputs workerInputs,
      @Nullable final List<SegmentIdWithShardSpec> segmentsToGenerate
  )
  {
    final Int2ObjectMap<List<SegmentIdWithShardSpec>> retVal = new Int2ObjectAVLTreeMap<>();

    // Empty segments validation already happens when the stages are started -- so we cannot have both
    // isFailOnEmptyInsertEnabled and segmentsToGenerate.isEmpty() be true here.
    if (segmentsToGenerate == null || segmentsToGenerate.isEmpty()) {
      return retVal;
    }

    for (final int workerNumber : workerInputs.workers()) {
      // SegmentGenerator stage has a single input from another stage.
      final StageInputSlice stageInputSlice =
          (StageInputSlice) Iterables.getOnlyElement(workerInputs.inputsForWorker(workerNumber));

      final List<SegmentIdWithShardSpec> workerSegments = new ArrayList<>();
      retVal.put(workerNumber, workerSegments);

      for (final ReadablePartition partition : stageInputSlice.getPartitions()) {
        workerSegments.add(segmentsToGenerate.get(partition.getPartitionNumber()));
      }
    }

    return retVal;
  }
}
