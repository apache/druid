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
import org.apache.druid.frame.key.KeyColumn;
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
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.sql.calcite.planner.ColumnMappings;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
    final DataSchema dataSchema = SegmentGenerationUtils.makeDataSchemaForIngestion(
        querySpec,
        querySignature,
        queryClusterBy,
        columnMappings,
        jsonMapper,
        query
    );

    final Map<String, VirtualColumn> clusterByVirtualColumnMappings = getClusterByVirtualColumnMappings(
        query,
        queryClusterBy
    );

    return StageDefinition.builder(queryDef.getNextStageNumber())
                          .inputs(new StageInputSpec(queryDef.getFinalStageDefinition().getStageNumber()))
                          .maxWorkerCount(tuningConfig.getMaxNumWorkers())
                          .processor(
                              new SegmentGeneratorStageProcessor(
                                  dataSchema,
                                  columnMappings,
                                  clusterByVirtualColumnMappings,
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

  private static Map<String, VirtualColumn> getClusterByVirtualColumnMappings(Query<?> query, ClusterBy queryClusterBy)
  {
    final Map<String, VirtualColumn> clusterByVirtualColumns = new LinkedHashMap<>();
    if (query instanceof GroupByQuery groupByQuery) {
      final Map<String, VirtualColumn> outputToVc = new LinkedHashMap<>();
      for (DimensionSpec spec : groupByQuery.getDimensions()) {
        final VirtualColumn vc = groupByQuery.getVirtualColumns().getVirtualColumn(spec.getDimension());
        if (vc != null) {
          outputToVc.put(spec.getOutputName(), vc);
        }
      }
      for (KeyColumn column : queryClusterBy.getColumns()) {
        final VirtualColumn vc = outputToVc.get(column.columnName());
        if (vc != null) {
          clusterByVirtualColumns.put(column.columnName(), vc);
          addRequiredVirtualColumns(groupByQuery.getVirtualColumns(), vc, clusterByVirtualColumns);
        }
      }
    } else if (query instanceof ScanQuery scanQuery) {
      for (KeyColumn column : queryClusterBy.getColumns()) {
        final VirtualColumn vc = scanQuery.getVirtualColumns().getVirtualColumn(column.columnName());
        if (vc != null) {
          clusterByVirtualColumns.put(column.columnName(), vc);
          addRequiredVirtualColumns(scanQuery.getVirtualColumns(), vc, clusterByVirtualColumns);
        }
      }
    }
    return clusterByVirtualColumns;
  }

  /**
   * Recursively adds any {@link VirtualColumn#requiredColumns()} which are also virtual columns. This handles cases
   * where a cluster-by virtual column depends on other virtual columns, such as when clustering by something like
   * {@code LOWER(JSON_VALUE(obj, '$.path'))} which creates an ExpressionVirtualColumn that references a
   * NestedFieldVirtualColumn.
   */
  private static void addRequiredVirtualColumns(
      VirtualColumns allVirtualColumns,
      VirtualColumn vc,
      Map<String, VirtualColumn> collected
  )
  {
    for (String requiredColumn : vc.requiredColumns()) {
      if (!collected.containsKey(requiredColumn)) {
        final VirtualColumn requiredVc = allVirtualColumns.getVirtualColumn(requiredColumn);
        if (requiredVc != null) {
          collected.put(requiredColumn, requiredVc);
          addRequiredVirtualColumns(allVirtualColumns, requiredVc, collected);
        }
      }
    }
  }
}
