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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.frame.key.KeyOrder;
import org.apache.druid.msq.exec.Limits;
import org.apache.druid.msq.input.stage.StageInputSpec;
import org.apache.druid.msq.kernel.HashShuffleSpec;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.QueryDefinitionBuilder;
import org.apache.druid.msq.kernel.ShuffleSpec;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.Query;
import org.apache.druid.query.operator.ColumnWithDirection;
import org.apache.druid.query.operator.NaivePartitioningOperatorFactory;
import org.apache.druid.query.operator.NaiveSortOperatorFactory;
import org.apache.druid.query.operator.OperatorFactory;
import org.apache.druid.query.operator.WindowOperatorQuery;
import org.apache.druid.query.operator.window.WindowOperatorFactory;
import org.apache.druid.segment.column.RowSignature;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WindowOperatorQueryKit implements QueryKit<WindowOperatorQuery>
{
  private final ObjectMapper jsonMapper;

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
    boolean isEmptyOverFound = ifEmptyOverPresentInWindowOperstors(originalQuery, operatorList);

    ShuffleSpec nextShuffleSpec = findShuffleSpecForNextWindow(operatorList.get(0), maxWorkerCount);
    // add this shuffle spec to the last stage of the inner query

    final QueryDefinitionBuilder queryDefBuilder = QueryDefinition.builder(queryId);
    if (nextShuffleSpec != null) {
      final ClusterBy windowClusterBy = nextShuffleSpec.clusterBy();
      originalQuery = (WindowOperatorQuery) originalQuery.withOverriddenContext(ImmutableMap.of(
          MultiStageQueryContext.NEXT_WINDOW_SHUFFLE_COL,
          windowClusterBy
      ));
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

    dataSourcePlan.getSubQueryDefBuilder().ifPresent(queryDefBuilder::addAll);

    final int firstStageNumber = Math.max(minStageNumber, queryDefBuilder.getNextStageNumber());
    final WindowOperatorQuery queryToRun = (WindowOperatorQuery) originalQuery.withDataSource(dataSourcePlan.getNewDataSource());
    final int maxRowsMaterialized;
    RowSignature rowSignature = queryToRun.getRowSignature();
    if (originalQuery.context() != null && originalQuery.context().containsKey(MultiStageQueryContext.MAX_ROWS_MATERIALIZED_IN_WINDOW)) {
      maxRowsMaterialized = (int) originalQuery.context()
                                                         .get(MultiStageQueryContext.MAX_ROWS_MATERIALIZED_IN_WINDOW);
    } else {
      maxRowsMaterialized = Limits.MAX_ROWS_MATERIALIZED_IN_WINDOW;
    }


    if (isEmptyOverFound) {
      // empty over clause found
      // moving everything to a single partition
      queryDefBuilder.add(
          StageDefinition.builder(firstStageNumber)
                         .inputs(new StageInputSpec(firstStageNumber - 1))
                         .signature(rowSignature)
                         .maxWorkerCount(maxWorkerCount)
                         .shuffleSpec(null)
                         .processorFactory(new WindowOperatorQueryFrameProcessorFactory(
                             queryToRun,
                             queryToRun.getOperators(),
                             rowSignature,
                             true,
                             maxRowsMaterialized
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
        // if it is the last stage set the next shuffle spec to single partition
        if (i + 1 == numberOfWindows) {
          nextShuffleSpec = ShuffleSpecFactories.singlePartition()
                                                .build(ClusterBy.none(), false);
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
                               stageRowSignature,
                               false,
                               maxRowsMaterialized
                           ))
        );
      }
    }
    return queryDefBuilder.build();
  }

  /**
   *
   * @param originalQuery
   * @param operatorList
   * @return true if the operator List has a partitioning operator with an empty OVER clause, false otherwise
   */
  private boolean ifEmptyOverPresentInWindowOperstors(
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
