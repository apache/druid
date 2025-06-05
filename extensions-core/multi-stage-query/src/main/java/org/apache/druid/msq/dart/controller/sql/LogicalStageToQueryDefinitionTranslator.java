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

package org.apache.druid.msq.dart.controller.sql;

import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.msq.kernel.FrameProcessorFactory;
import org.apache.druid.msq.kernel.MixShuffleSpec;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.ShuffleSpec;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.kernel.StageDefinitionBuilder;
import org.apache.druid.msq.logical.DagInputSpec;
import org.apache.druid.msq.logical.LogicalStage;
import org.apache.druid.msq.logical.LogicalStageBuilder.DagStage;
import org.apache.druid.msq.logical.LogicalStageBuilder.FrameProcessorStage;
import org.apache.druid.msq.logical.LogicalStageBuilder.ShuffleStage;
import org.apache.druid.msq.querykit.QueryKitUtils;
import org.apache.druid.msq.querykit.ShuffleSpecFactories;
import org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessorFactory;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import java.util.List;
import java.util.Stack;

public class LogicalStageToQueryDefinitionTranslator
{
  private static final String IRRELEVANT = "irrelevant";

  public static class StageMaker2
  {
    private final PlannerContext plannerContext = null;
    /** Provides ids for the stages. */
    private int stageIdSeq = 0;

    StageDefinition makeScanStage(
        VirtualColumns virtualColumns,
        RowSignature signature,
        List<InputSpec> inputs,
        DimFilter dimFilter,
        List<KeyColumn> keyColumns)
    {
      ScanQueryFrameProcessorFactory scanProcessorFactory = makeScanFrameProcessor(
          virtualColumns, signature, dimFilter
      );
      StageDefinitionBuilder sdb = StageDefinition.builder(getNextStageId())
          .inputs(inputs)
          .processorFactory(scanProcessorFactory)
          .signature(signature)
          .shuffleSpec(shuffleFor(keyColumns));

      return sdb.build(getIdForBuilder());
    }

    Stack<DagStage> stack = new Stack<>();

    public void pushFrameProcessorStage(
        List<DagInputSpec> inputs,
        RowSignature signature,
        FrameProcessorFactory<?, ?, ?> processorFactory)
    {
      stack.push(new FrameProcessorStage(inputs, signature, processorFactory));
    }

    public void pushSortStage(RowSignature signature, List<KeyColumn> keyColumns)
    {
      stack.push(new ShuffleStage(stack.pop(), signature, keyColumns));
    }

    public ScanQueryFrameProcessorFactory makeScanFrameProcessor(
        VirtualColumns virtualColumns,
        RowSignature signature,
        DimFilter dimFilter)
    {
      ScanQuery s = Druids.newScanQueryBuilder()
          .dataSource(IRRELEVANT)
          .intervals(QuerySegmentSpec.ETERNITY)
          .filters(dimFilter)
          .virtualColumns(virtualColumns)
          .columns(signature.getColumnNames())
          .columnTypes(signature.getColumnTypes())
          .build();

      return new ScanQueryFrameProcessorFactory(s);
    }

    private ShuffleSpec shuffleFor(List<KeyColumn> keyColumns)
    {
      if (keyColumns == null) {
        return MixShuffleSpec.instance();
      } else {

        final Granularity segmentGranularity = Granularities.ALL;
        // FIXME:
        // QueryKitUtils.getSegmentGranularityFromContext(jsonMapper,
        // queryToRun.getContext());

        final ClusterBy clusterBy = QueryKitUtils
            .clusterByWithSegmentGranularity(new ClusterBy(keyColumns, 0), segmentGranularity);
        // FIXME targetSize == 1
        return ShuffleSpecFactories.globalSortWithMaxPartitionCount(1).build(clusterBy, false);
      }
    }


//    StageDefinition build1() {
//      DagStage node = stack.pop();
//      return node.buildStages(new StageMaker()).build(getIdForBuilder());
//    }
//
    private int getNextStageId()
    {
      return stageIdSeq++;
    }

    private String getIdForBuilder()
    {
      String dartQueryId = plannerContext.queryContext().getString(QueryContexts.CTX_DART_QUERY_ID);
      if (dartQueryId != null) {
        return dartQueryId;
      }
      return plannerContext.getSqlQueryId();
    }

    public void build(LogicalStage rootStage)
    {
      rootStage.buildCurrentStage2(this);
      if(true)
      {
        throw new RuntimeException("FIXME: Unimplemented!");
      }

    }
  }

  public static QueryDefinition translate(LogicalStage rootStage)
  {
    StageMaker stageMaker = new StageMaker();

    stageMaker.build(rootStage);

    return null;

  }

}
