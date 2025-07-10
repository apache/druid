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
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.frame.key.KeyOrder;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.input.stage.StageInputSpec;
import org.apache.druid.msq.kernel.HashShuffleSpec;
import org.apache.druid.msq.kernel.MixShuffleSpec;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.QueryDefinitionBuilder;
import org.apache.druid.msq.kernel.ShuffleSpec;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.kernel.StageDefinitionBuilder;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.operator.AbstractPartitioningOperatorFactory;
import org.apache.druid.query.operator.AbstractSortOperatorFactory;
import org.apache.druid.query.operator.ColumnWithDirection;
import org.apache.druid.query.operator.GlueingPartitioningOperatorFactory;
import org.apache.druid.query.operator.OperatorFactory;
import org.apache.druid.query.operator.PartitionSortOperatorFactory;
import org.apache.druid.query.operator.WindowOperatorQuery;
import org.apache.druid.query.operator.window.WindowOperatorFactory;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WindowOperatorQueryKit implements QueryKit<WindowOperatorQuery>
{
  private static final Logger log = new Logger(WindowOperatorQueryKit.class);
  private final ObjectMapper jsonMapper;
  private final boolean isOperatorTransformationEnabled;

  public WindowOperatorQueryKit(ObjectMapper jsonMapper, boolean isOperatorTransformationEnabled)
  {
    this.jsonMapper = jsonMapper;
    this.isOperatorTransformationEnabled = isOperatorTransformationEnabled;
  }

  @Override
  public QueryDefinition makeQueryDefinition(
      QueryKitSpec queryKitSpec,
      WindowOperatorQuery originalQuery,
      ShuffleSpecFactory resultShuffleSpecFactory,
      int minStageNumber
  )
  {
    final DataSourcePlan dataSourcePlan = DataSourcePlan.forDataSource(
        queryKitSpec,
        originalQuery.context(),
        originalQuery.getDataSource(),
        originalQuery.getQuerySegmentSpec(),
        originalQuery.getFilter(),
        null,
        minStageNumber,
        false
    );
    final RowSignature signatureFromInput = dataSourcePlan.getSubQueryDefBuilder()
                                                          .get()
                                                          .build()
                                                          .getFinalStageDefinition()
                                                          .getSignature();

    final WindowStages windowStages = new WindowStages(
        originalQuery,
        jsonMapper,
        queryKitSpec.getNumPartitionsForShuffle(),
        queryKitSpec.getMaxNonLeafWorkerCount(),
        resultShuffleSpecFactory,
        signatureFromInput,
        isOperatorTransformationEnabled
    );

    final ShuffleSpec nextShuffleSpec = windowStages.getStages().get(0).findShuffleSpec(queryKitSpec.getNumPartitionsForShuffle());
    final QueryDefinitionBuilder queryDefBuilder = makeQueryDefinitionBuilder(queryKitSpec.getQueryId(), dataSourcePlan, nextShuffleSpec);
    final int firstWindowStageNumber = Math.max(minStageNumber, queryDefBuilder.getNextStageNumber());

    log.info("Row signature received from last stage is [%s].", signatureFromInput);

    // Iterate over the list of window stages, and add the definition for each window stage to QueryDefinitionBuilder.
    for (int i = 0; i < windowStages.getStages().size(); i++) {
      queryDefBuilder.add(windowStages.getStageDefinitionBuilder(firstWindowStageNumber + i, i));
    }
    return queryDefBuilder.build();
  }

  /**
   * Represents the window stages to be added to {@link QueryDefinitionBuilder}.
   * This class is responsible for creating the window stages.
   */
  private static class WindowStages
  {
    private final List<WindowStage> stages;
    private final WindowOperatorQuery query;
    private final int numPartitionsForShuffle;
    private final int maxNonLeafWorkerCount;
    private final ShuffleSpec finalWindowStageShuffleSpec;
    private final RowSignature finalWindowStageRowSignature;
    private final RowSignature.Builder rowSignatureBuilder;
    private final boolean isOperatorTransformationEnabled;

    private WindowStages(
        WindowOperatorQuery query,
        ObjectMapper jsonMapper,
        int numPartitionsForShuffle,
        int maxNonLeafWorkerCount,
        ShuffleSpecFactory resultShuffleSpecFactory,
        RowSignature signatureFromInput,
        boolean isOperatorTransformationEnabled
    )
    {
      this.stages = new ArrayList<>();
      this.query = query;
      this.numPartitionsForShuffle = numPartitionsForShuffle;
      this.maxNonLeafWorkerCount = maxNonLeafWorkerCount;
      this.isOperatorTransformationEnabled = isOperatorTransformationEnabled;

      final Granularity segmentGranularity = QueryKitUtils.getSegmentGranularityFromContext(
          jsonMapper,
          query.getContext()
      );
      final ClusterBy finalWindowClusterBy = computeClusterByForFinalWindowStage(segmentGranularity);
      this.finalWindowStageShuffleSpec = computeShuffleSpecForFinalWindowStage(
          resultShuffleSpecFactory,
          finalWindowClusterBy
      );
      this.finalWindowStageRowSignature = computeSignatureForFinalWindowStage(
          query.getRowSignature(),
          finalWindowClusterBy,
          segmentGranularity
      );

      this.rowSignatureBuilder = RowSignature.builder().addAll(signatureFromInput);
      populateStages();
    }

    private void populateStages()
    {
      WindowStage currentStage = new WindowStage(getMaxRowsMaterialized());
      for (OperatorFactory of : query.getOperators()) {
        if (!currentStage.canAccept(of)) {
          stages.add(currentStage);
          currentStage = new WindowStage(getMaxRowsMaterialized());
        }
        currentStage.addOperatorFactory(of);
      }
      if (!currentStage.getOperatorFactories().isEmpty()) {
        stages.add(currentStage);
      }

      log.info("Created window stages: [%s]", stages);
    }

    private List<WindowStage> getStages()
    {
      return stages;
    }

    private RowSignature getRowSignatureForStage(int windowStageIndex, ShuffleSpec shuffleSpec)
    {
      if (windowStageIndex == stages.size() - 1) {
        return finalWindowStageRowSignature;
      }

      final WindowStage stage = stages.get(windowStageIndex);
      for (WindowOperatorFactory operatorFactory : stage.getWindowOperatorFactories()) {
        for (String columnName : operatorFactory.getProcessor().getOutputColumnNames()) {
          int indexInRowSignature = query.getRowSignature().indexOf(columnName);
          if (indexInRowSignature != -1 && rowSignatureBuilder.build().indexOf(columnName) == -1) {
            ColumnType columnType = query.getRowSignature().getColumnType(indexInRowSignature).get();
            rowSignatureBuilder.add(columnName, columnType);
          }
        }
      }

      final RowSignature intermediateSignature = rowSignatureBuilder.build();

      final RowSignature stageRowSignature;
      if (shuffleSpec == null) {
        stageRowSignature = intermediateSignature;
      } else {
        stageRowSignature = QueryKitUtils.sortableSignature(
            intermediateSignature,
            shuffleSpec.clusterBy().getColumns()
        );
      }

      log.info("Using row signature [%s] for window stage.", stageRowSignature);
      return stageRowSignature;
    }

    private StageDefinitionBuilder getStageDefinitionBuilder(int stageNumber, int windowStageIndex)
    {
      final WindowStage stage = stages.get(windowStageIndex);
      final ShuffleSpec shuffleSpec = (windowStageIndex == stages.size() - 1) ?
                                      finalWindowStageShuffleSpec :
                                      stages.get(windowStageIndex + 1).findShuffleSpec(numPartitionsForShuffle);

      final RowSignature stageRowSignature = getRowSignatureForStage(windowStageIndex, shuffleSpec);
      final List<OperatorFactory> operatorFactories = isOperatorTransformationEnabled
                                                      ? stage.getTransformedOperatorFactories()
                                                      : stage.getOperatorFactories();

      return StageDefinition.builder(stageNumber)
                            .inputs(new StageInputSpec(stageNumber - 1))
                            .signature(stageRowSignature)
                            .maxWorkerCount(maxNonLeafWorkerCount)
                            .shuffleSpec(shuffleSpec)
                            .processor(new WindowOperatorQueryStageProcessor(
                                query,
                                operatorFactories,
                                stageRowSignature,
                                getMaxRowsMaterialized(),
                                stage.getPartitionColumns()
                            ));
    }

    /**
     * Computes the ClusterBy for the final window stage. We don't have to take the CLUSTERED BY columns into account,
     * as they are handled as {@link org.apache.druid.query.scan.ScanQuery#orderBys}.
     */
    private ClusterBy computeClusterByForFinalWindowStage(Granularity segmentGranularity)
    {
      final List<KeyColumn> clusterByColumns = Collections.singletonList(new KeyColumn(
          QueryKitUtils.PARTITION_BOOST_COLUMN,
          KeyOrder.ASCENDING
      ));
      return QueryKitUtils.clusterByWithSegmentGranularity(new ClusterBy(clusterByColumns, 0), segmentGranularity);
    }

    /**
     * Computes the signature for the final window stage. The finalWindowClusterBy will always have the
     * partition boost column as computed in {@link #computeClusterByForFinalWindowStage(Granularity)}.
     */
    private RowSignature computeSignatureForFinalWindowStage(
        RowSignature rowSignature,
        ClusterBy finalWindowClusterBy,
        Granularity segmentGranularity
    )
    {
      final RowSignature.Builder finalWindowStageRowSignatureBuilder = RowSignature.builder()
                                                                                   .addAll(rowSignature)
                                                                                   .add(
                                                                                       QueryKitUtils.PARTITION_BOOST_COLUMN,
                                                                                       ColumnType.LONG
                                                                                   );
      return QueryKitUtils.sortableSignature(
          QueryKitUtils.signatureWithSegmentGranularity(
              finalWindowStageRowSignatureBuilder.build(),
              segmentGranularity
          ),
          finalWindowClusterBy.getColumns()
      );
    }

    private ShuffleSpec computeShuffleSpecForFinalWindowStage(
        ShuffleSpecFactory resultShuffleSpecFactory,
        ClusterBy finalWindowClusterBy
    )
    {
      return resultShuffleSpecFactory.build(finalWindowClusterBy, false);
    }

    private int getMaxRowsMaterialized()
    {
      return MultiStageQueryContext.getMaxRowsMaterializedInWindow(query.context());
    }
  }

  /**
   * Represents a window stage in a query execution.
   * Each stage can contain a sort operator, a partition operator, and multiple window operators.
   */
  private static class WindowStage
  {
    private AbstractSortOperatorFactory sortOperatorFactory;
    private AbstractPartitioningOperatorFactory partitioningOperatorFactory;
    private final List<WindowOperatorFactory> windowOperatorFactories;
    private final int maxRowsMaterialized;

    private WindowStage(int maxRowsMaterialized)
    {
      this.windowOperatorFactories = new ArrayList<>();
      this.maxRowsMaterialized = maxRowsMaterialized;
    }

    private void addOperatorFactory(OperatorFactory op)
    {
      if (op instanceof AbstractSortOperatorFactory) {
        this.sortOperatorFactory = (AbstractSortOperatorFactory) op;
      } else if (op instanceof AbstractPartitioningOperatorFactory) {
        this.partitioningOperatorFactory = (AbstractPartitioningOperatorFactory) op;
      } else {
        this.windowOperatorFactories.add((WindowOperatorFactory) op);
      }
    }

    private List<OperatorFactory> getOperatorFactories()
    {
      List<OperatorFactory> operatorFactories = new ArrayList<>();
      if (sortOperatorFactory != null) {
        operatorFactories.add(sortOperatorFactory);
      }
      if (partitioningOperatorFactory != null) {
        operatorFactories.add(partitioningOperatorFactory);
      }
      operatorFactories.addAll(windowOperatorFactories);
      return operatorFactories;
    }

    /**
     * This method converts the operator chain received from native plan into MSQ plan.
     * (NaiveSortOperator -> Naive/GlueingPartitioningOperator -> WindowOperator) is converted into (GlueingPartitioningOperator -> PartitionSortOperator -> WindowOperator).
     * We rely on MSQ's shuffling to do the clustering on partitioning keys for us at every stage.
     * This conversion allows us to blindly read N rows from input channel and push them into the operator chain, and repeat until the input channel isn't finished.
     * @return
     */
    private List<OperatorFactory> getTransformedOperatorFactories()
    {
      List<OperatorFactory> operatorFactories = new ArrayList<>();
      if (partitioningOperatorFactory != null) {
        operatorFactories.add(new GlueingPartitioningOperatorFactory(partitioningOperatorFactory.getPartitionColumns(), maxRowsMaterialized));
      }
      if (sortOperatorFactory != null) {
        operatorFactories.add(new PartitionSortOperatorFactory(sortOperatorFactory.getSortColumns()));
      }
      operatorFactories.addAll(windowOperatorFactories);
      return operatorFactories;
    }

    private List<WindowOperatorFactory> getWindowOperatorFactories()
    {
      return windowOperatorFactories;
    }

    private ShuffleSpec findShuffleSpec(int partitionCount)
    {
      Map<String, ColumnWithDirection.Direction> sortColumnsMap = new HashMap<>();
      if (sortOperatorFactory != null) {
        for (ColumnWithDirection sortColumn : sortOperatorFactory.getSortColumns()) {
          sortColumnsMap.put(sortColumn.getColumn(), sortColumn.getDirection());
        }
      }

      if (partitioningOperatorFactory == null) {
        // If the window stage doesn't have any partitioning factory, then we should keep the shuffle spec from previous stage.
        // This indicates that we already have the data partitioned correctly, and hence we don't need to do any shuffling.
        return null;
      }

      if (partitioningOperatorFactory.getPartitionColumns().isEmpty()) {
        return MixShuffleSpec.instance();
      }

      final List<KeyColumn> keyColsOfWindow = new ArrayList<>();
      for (String partitionColumn : partitioningOperatorFactory.getPartitionColumns()) {
        KeyColumn kc = new KeyColumn(
            partitionColumn,
            sortColumnsMap.get(partitionColumn) == ColumnWithDirection.Direction.DESC
            ? KeyOrder.DESCENDING
            : KeyOrder.ASCENDING
        );
        keyColsOfWindow.add(kc);
      }

      return new HashShuffleSpec(new ClusterBy(keyColsOfWindow, 0), partitionCount);
    }

    private boolean canAccept(OperatorFactory operatorFactory)
    {
      if (getOperatorFactories().isEmpty()) {
        return true;
      }
      if (operatorFactory instanceof AbstractSortOperatorFactory) {
        return false;
      }
      if (operatorFactory instanceof WindowOperatorFactory) {
        return true;
      }
      if (operatorFactory instanceof AbstractPartitioningOperatorFactory) {
        return sortOperatorFactory != null;
      }
      throw new ISE("Encountered unexpected operatorFactory type: [%s]", operatorFactory.getClass().getName());
    }

    private List<String> getPartitionColumns()
    {
      return partitioningOperatorFactory == null ? new ArrayList<>() : partitioningOperatorFactory.getPartitionColumns();
    }

    @Override
    public String toString()
    {
      return "WindowStage{" +
             "sortOperatorFactory=" + sortOperatorFactory +
             ", partitioningOperatorFactory=" + partitioningOperatorFactory +
             ", windowOperatorFactories=" + windowOperatorFactories +
             '}';
    }
  }

  /**
   * Override the shuffle spec of the last stage based on the shuffling required by the first window stage.
   * @param queryId
   * @param dataSourcePlan
   * @param shuffleSpec
   * @return
   */
  private QueryDefinitionBuilder makeQueryDefinitionBuilder(String queryId, DataSourcePlan dataSourcePlan, ShuffleSpec shuffleSpec)
  {
    final QueryDefinitionBuilder queryDefBuilder = QueryDefinition.builder(queryId);
    int previousStageNumber = dataSourcePlan.getSubQueryDefBuilder().get().build().getFinalStageDefinition().getStageNumber();
    for (final StageDefinition stageDef : dataSourcePlan.getSubQueryDefBuilder().get().build().getStageDefinitions()) {
      if (stageDef.getStageNumber() == previousStageNumber) {
        RowSignature rowSignature = QueryKitUtils.sortableSignature(
            stageDef.getSignature(),
            shuffleSpec.clusterBy().getColumns()
        );
        queryDefBuilder.add(StageDefinition.builder(stageDef).shuffleSpec(shuffleSpec).signature(rowSignature));
      } else {
        queryDefBuilder.add(StageDefinition.builder(stageDef));
      }
    }
    return queryDefBuilder;
  }
}
