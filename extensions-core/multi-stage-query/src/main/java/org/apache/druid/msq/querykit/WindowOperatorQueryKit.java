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

package org.apache.druid.msq.querykit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.frame.key.KeyOrder;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.msq.input.stage.StageInputSpec;
import org.apache.druid.msq.kernel.HashShuffleSpec;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.QueryDefinitionBuilder;
import org.apache.druid.msq.kernel.ShuffleSpec;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessorFactory;
import org.apache.druid.query.Query;
import org.apache.druid.query.operator.ColumnWithDirection;
import org.apache.druid.query.operator.NaivePartitioningOperatorFactory;
import org.apache.druid.query.operator.NaiveSortOperatorFactory;
import org.apache.druid.query.operator.OperatorFactory;
import org.apache.druid.query.operator.ScanOperatorFactory;
import org.apache.druid.query.operator.WindowOperatorQuery;
import org.apache.druid.query.operator.window.WindowOperatorFactory;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.RowSignature;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WindowOperatorQueryKit implements QueryKit<WindowOperatorQuery>
{
  private final ObjectMapper jsonMapper;
  public static final String CTX_SCAN_SIGNATURE = "scanSignature";

  public WindowOperatorQueryKit(ObjectMapper jsonMapper)
  {
    this.jsonMapper = jsonMapper;
  }

  @Override
  public QueryDefinition makeQueryDefinition(
      String queryId,
      WindowOperatorQuery originalQuery,
      QueryKit<Query<?>> queryKit,
      ShuffleSpecFactory resultShuffleSpecFactory,
      int maxWorkerCount,
      int minStageNumber
  )
  {
    // need to validate query first
    // populate the group of operators to be processed as each stage
    // the size of the operators is the number of serialized stages
    // later we should also check if these can be parallelized
    // check there is an empty over clause or not
    List<List<OperatorFactory>> operatorList = new ArrayList<>();
    boolean status = validateAndReturnOperatorList(originalQuery, operatorList);


    ShuffleSpec nextShuffleSpec = findShuffleSpecForNextWindow(operatorList.get(0), maxWorkerCount);
    // add this shuffle spec to the last stage of the inner query

    final QueryDefinitionBuilder queryDefBuilder = QueryDefinition.builder().queryId(queryId);
    if (nextShuffleSpec != null) {
      final ClusterBy windowClusterBy = nextShuffleSpec.clusterBy();
      originalQuery = (WindowOperatorQuery) originalQuery.withOverriddenContext(ImmutableMap.of(
          DataSourcePlan.NEXT_SHUFFLE_COL,
          windowClusterBy
      ));
    } else {
      nextShuffleSpec = ShuffleSpecFactories.singlePartition()
                                            .build(ClusterBy.none(), false);
    }
    final DataSourcePlan dataSourcePlan = DataSourcePlan.forDataSource(
        queryKit,
        queryId,
        originalQuery.context(),
        originalQuery.getDataSource(),
        originalQuery.getQuerySegmentSpec(),
        originalQuery.getFilter(),
        null,
        maxWorkerCount,
        minStageNumber,
        false
    );

    // Handling for case where leaf operators are present
    // leaf operators can only be a scan,
    // and they execute before any other operators,
    // so we introduce a separate stage for the leaf operators.
    // We recreate scan query from leaf operator and use the associated
    // ScanQueryFrameProcessorFactory for that particular stage.
    // The next stage will work on the output of this stage
    if (!originalQuery.getLeafOperators().isEmpty()) {
      final RowSignature originalSign = originalQuery.getRowSignature();
      for (OperatorFactory of : originalQuery.getLeafOperators()) {
        if (of instanceof ScanOperatorFactory) {
          Pair<Map<String, Object>, RowSignature> mapRowSignaturePair = updateContextWithScanSignatureIfNeeded(
              ((ScanOperatorFactory) of).getVirtualColumns(),
              ((ScanOperatorFactory) of).getProjectedColumns(),
              originalQuery.getContext(),
              originalSign
          );
          ScanQueryFrameProcessorFactory scanQueryFrameProcessorFactory = getScanQueryFrameProcessorFactory(
              originalQuery,
              (ScanOperatorFactory) of,
              mapRowSignaturePair
          );

          queryDefBuilder.add(StageDefinition.builder(minStageNumber)
                                             .inputs(dataSourcePlan.getInputSpecs())
                                             .signature(mapRowSignaturePair.rhs)
                                             .maxWorkerCount(maxWorkerCount)
                                             .shuffleSpec(nextShuffleSpec)
                                             .processorFactory(scanQueryFrameProcessorFactory));
        }
      }
    }

    dataSourcePlan.getSubQueryDefBuilder().ifPresent(queryDefBuilder::addAll);

    final int firstStageNumber = Math.max(minStageNumber, queryDefBuilder.getNextStageNumber());
    final WindowOperatorQuery queryToRun = (WindowOperatorQuery) originalQuery.withDataSource(dataSourcePlan.getNewDataSource());
    RowSignature rowSignature = queryToRun.getRowSignature();

    if (status) {
      // empty over clause found
      // moving everything to a single partition
      queryDefBuilder.add(
          StageDefinition.builder(firstStageNumber)
                         .inputs(new StageInputSpec(firstStageNumber - 1))
                         .signature(rowSignature)
                         .maxWorkerCount(maxWorkerCount)
                         .shuffleSpec(ShuffleSpecFactories.singlePartition()
                                                          .build(ClusterBy.none(), false))
                         .processorFactory(new WindowOperatorQueryFrameProcessorFactory(
                             queryToRun,
                             queryToRun.getOperators(),
                             rowSignature
                         ))
      );
    } else {
      // there are multiple windows present in the query
      // Create stages for each window in the query
      // These stages will be serialized
      // the partition by clause of the next window will be the shuffle key for the previous window
      RowSignature.Builder bob = RowSignature.builder();
      final int numberOfWindows = operatorList.size();
      final int baseSize = rowSignature.size() - numberOfWindows;
      for (int i = 0; i < baseSize; i++) {
        bob.add(rowSignature.getColumnName(i), rowSignature.getColumnType(i).get());
      }

      for (int i = 0; i < numberOfWindows; i++) {
        bob.add(rowSignature.getColumnName(baseSize + i), rowSignature.getColumnType(baseSize + i).get()).build();
        // find the shuffle spec of the next stage
        // if it is the last stage set the next shuffle spec to null
        if (i + 1 == numberOfWindows) {
          nextShuffleSpec = null;
        } else {
          nextShuffleSpec = findShuffleSpecForNextWindow(operatorList.get(i + 1), maxWorkerCount);
        }

        final RowSignature intermediateSignature = bob.build();
        final RowSignature stageRowSignature;
        if (nextShuffleSpec == null) {
          stageRowSignature = intermediateSignature;
        } else {
          stageRowSignature = QueryKitUtils.sortableSignature(
              intermediateSignature,
              nextShuffleSpec.clusterBy().getColumns()
          );
        }

        queryDefBuilder.add(
            StageDefinition.builder(firstStageNumber + i)
                           .inputs(new StageInputSpec(firstStageNumber + i - 1))
                           .signature(stageRowSignature)
                           .maxWorkerCount(maxWorkerCount)
                           .shuffleSpec(nextShuffleSpec)
                           .processorFactory(new WindowOperatorQueryFrameProcessorFactory(
                               queryToRun,
                               operatorList.get(i),
                               stageRowSignature
                           ))
        );
      }
    }
    return queryDefBuilder.queryId(queryId).build();
  }

  private ScanQueryFrameProcessorFactory getScanQueryFrameProcessorFactory(
      WindowOperatorQuery originalQuery,
      ScanOperatorFactory of,
      Pair<Map<String, Object>, RowSignature> mapRowSignaturePair
  )
  {
    ScanQuery sq = new ScanQuery(
        originalQuery.getDataSource(),
        originalQuery.getQuerySegmentSpec(),
        of.getVirtualColumns(),
        ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST,
        2048,
        of.getOffsetLimit().getOffset(),
        of.getOffsetLimit().getLimit(),
        ScanQuery.Order.NONE,
        null,
        of.getFilter(),
        of.getProjectedColumns(),
        false,
        mapRowSignaturePair.lhs
    );

    ScanQueryFrameProcessorFactory sqfpf = new ScanQueryFrameProcessorFactory(sq);
    return sqfpf;
  }

  private Pair<Map<String, Object>, RowSignature> updateContextWithScanSignatureIfNeeded(
      final VirtualColumns virtualColumns,
      final List<String> scanColumns,
      final Map<String, Object> queryContext,
      final RowSignature originalQuerySignature
  )
  {
    // The scan signature of the inner stage will contain the columns
    // specified in the projectedColumns of the leaf operators
    // We need to fund the columntype of the columns and
    // compute the scan signature for the stage with leaf operators
    final RowSignature.Builder scanSignatureBuilder = RowSignature.builder();

    for (final String columnName : scanColumns) {
      final ColumnCapabilities capabilities;
      if (virtualColumns != null) {
        capabilities = virtualColumns.getColumnCapabilitiesWithFallback(originalQuerySignature, columnName);
      } else {
        capabilities = originalQuerySignature.getColumnCapabilities(columnName);
      }

      if (capabilities == null) {
        // No type for this column. This is a planner bug.
        throw new ISE("No type for column [%s]", columnName);
      }

      scanSignatureBuilder.add(columnName, capabilities.toColumnType());
    }

    final RowSignature signature = scanSignatureBuilder.build();

    try {
      Map<String, Object> revised = new HashMap<>(queryContext);
      revised.put(
          CTX_SCAN_SIGNATURE,
          jsonMapper.writeValueAsString(signature)
      );
      return Pair.of(revised, signature);
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean validateAndReturnOperatorList(
      WindowOperatorQuery originalQuery,
      List<List<OperatorFactory>> operatorList
  )
  {
    final List<OperatorFactory> operators = originalQuery.getOperators();
    List<OperatorFactory> operatorFactoryList = new ArrayList<>();
    for (OperatorFactory of : operators) {
      operatorFactoryList.add(of);
      if (of instanceof WindowOperatorFactory) {
        operatorList.add(operatorFactoryList);
        operatorFactoryList = new ArrayList<>();
      } else if (of instanceof NaivePartitioningOperatorFactory) {
        if (((NaivePartitioningOperatorFactory) of).getPartitionColumns().isEmpty()) {
          operatorList.clear();
          operatorList.add(originalQuery.getOperators());
          return true;
        }
      }
    }
    return false;
  }

  private ShuffleSpec findShuffleSpecForNextWindow(List<OperatorFactory> operatorFactories, int maxWorkerCount)
  {
    NaivePartitioningOperatorFactory partition = null;
    NaiveSortOperatorFactory sort = null;
    List<KeyColumn> keyColsOfWindow = new ArrayList<>();
    for (OperatorFactory of : operatorFactories) {
      if (of instanceof NaivePartitioningOperatorFactory) {
        partition = (NaivePartitioningOperatorFactory) of;
      } else if (of instanceof NaiveSortOperatorFactory) {
        sort = (NaiveSortOperatorFactory) of;
      }
    }
    Map<String, ColumnWithDirection.Direction> colMap = new HashMap<>();
    if (sort != null) {
      for (ColumnWithDirection sortColumn : sort.getSortColumns()) {
        colMap.put(sortColumn.getColumn(), sortColumn.getDirection());
      }
    }
    assert partition != null;
    if (partition.getPartitionColumns().isEmpty()) {
      return null;
    }
    for (String partitionColumn : partition.getPartitionColumns()) {
      KeyColumn kc;
      if (colMap.containsKey(partitionColumn)) {
        if (colMap.get(partitionColumn) == ColumnWithDirection.Direction.ASC) {
          kc = new KeyColumn(partitionColumn, KeyOrder.ASCENDING);
        } else {
          kc = new KeyColumn(partitionColumn, KeyOrder.DESCENDING);
        }
      } else {
        kc = new KeyColumn(partitionColumn, KeyOrder.ASCENDING);
      }
      keyColsOfWindow.add(kc);
    }
    return new HashShuffleSpec(new ClusterBy(keyColsOfWindow, 0), maxWorkerCount);
  }
}
