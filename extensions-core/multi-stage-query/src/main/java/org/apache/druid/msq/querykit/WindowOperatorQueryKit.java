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
import org.apache.druid.msq.exec.Limits;
import org.apache.druid.msq.input.stage.StageInputSpec;
import org.apache.druid.msq.kernel.HashShuffleSpec;
import org.apache.druid.msq.kernel.MixShuffleSpec;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.QueryDefinitionBuilder;
import org.apache.druid.msq.kernel.ShuffleSpec;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.operator.ColumnWithDirection;
import org.apache.druid.query.operator.NaivePartitioningOperatorFactory;
import org.apache.druid.query.operator.NaiveSortOperatorFactory;
import org.apache.druid.query.operator.OperatorFactory;
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

  public WindowOperatorQueryKit(ObjectMapper jsonMapper)
  {
    this.jsonMapper = jsonMapper;
  }

  @Override
  public QueryDefinition makeQueryDefinition(
      QueryKitSpec queryKitSpec,
      WindowOperatorQuery originalQuery,
      ShuffleSpecFactory resultShuffleSpecFactory,
      int minStageNumber
  )
  {
    // Need to validate query first.
    // Populate the group of operators to be processed at each stage.
    // The size of the operators is the number of serialized stages.
    // Later we should also check if these can be parallelized.
    // Check if there is an empty OVER() clause or not.
    RowSignature rowSignature = originalQuery.getRowSignature();
    log.info("Row signature received for query is [%s].", rowSignature);

    boolean isEmptyOverPresent = originalQuery.getOperators()
                                            .stream()
                                            .filter(of -> of instanceof NaivePartitioningOperatorFactory)
                                            .map(of -> (NaivePartitioningOperatorFactory) of)
                                            .anyMatch(of -> of.getPartitionColumns().isEmpty());

    final List<WindowStage> stages = getWindowStagesFromQuery(originalQuery);
    log.info("Created window stages with operator factories: [%s]", stages);

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

    ShuffleSpec nextShuffleSpec = stages.get(0).findShuffleSpec(queryKitSpec.getNumPartitionsForShuffle());
    final QueryDefinitionBuilder queryDefBuilder =
        makeQueryDefinitionBuilder(queryKitSpec.getQueryId(), dataSourcePlan, nextShuffleSpec);

    final int firstStageNumber = Math.max(minStageNumber, queryDefBuilder.getNextStageNumber());
    final WindowOperatorQuery queryToRun = (WindowOperatorQuery) originalQuery.withDataSource(dataSourcePlan.getNewDataSource());

    // Get segment granularity from query context, and create ShuffleSpec and RowSignature to be used for the final window stage.
    final Granularity segmentGranularity = QueryKitUtils.getSegmentGranularityFromContext(jsonMapper, queryToRun.getContext());
    final ClusterBy finalWindowClusterBy = computeClusterByForFinalWindowStage(segmentGranularity);
    final ShuffleSpec finalWindowStageShuffleSpec = resultShuffleSpecFactory.build(finalWindowClusterBy, false);
    final RowSignature finalWindowStageRowSignature = computeSignatureForFinalWindowStage(rowSignature, finalWindowClusterBy, segmentGranularity);

    final int maxRowsMaterialized;
    if (originalQuery.context() != null && originalQuery.context().containsKey(MultiStageQueryContext.MAX_ROWS_MATERIALIZED_IN_WINDOW)) {
      maxRowsMaterialized = (int) originalQuery.context().get(MultiStageQueryContext.MAX_ROWS_MATERIALIZED_IN_WINDOW);
    } else {
      maxRowsMaterialized = Limits.MAX_ROWS_MATERIALIZED_IN_WINDOW;
    }

    if (isEmptyOverPresent) {
      // Move everything to a single partition since we have to load all the data on a single worker anyway to compute empty over() clause.
      log.info(
          "Empty over clause is present in the query. Creating a single stage with all operator factories [%s].",
          queryToRun.getOperators()
      );
      queryDefBuilder.add(
          StageDefinition.builder(firstStageNumber)
                         .inputs(new StageInputSpec(firstStageNumber - 1))
                         .signature(finalWindowStageRowSignature)
                         .maxWorkerCount(queryKitSpec.getMaxNonLeafWorkerCount())
                         .shuffleSpec(finalWindowStageShuffleSpec)
                         .processorFactory(new WindowOperatorQueryFrameProcessorFactory(
                             queryToRun,
                             queryToRun.getOperators(),
                             finalWindowStageRowSignature,
                             maxRowsMaterialized,
                             Collections.emptyList()
                         ))
      );
    } else {
      // There are multiple windows present in the query.
      // Create stages for each window in the query.
      // These stages will be serialized.
      // The partition by clause of the next window will be the shuffle key for the previous window.
      RowSignature.Builder bob = RowSignature.builder();
      RowSignature signatureFromInput = dataSourcePlan.getSubQueryDefBuilder().get().build().getFinalStageDefinition().getSignature();
      log.info("Row signature received from last stage is [%s].", signatureFromInput);

      for (int i = 0; i < signatureFromInput.getColumnNames().size(); i++) {
        bob.add(signatureFromInput.getColumnName(i), signatureFromInput.getColumnType(i).get());
      }

      List<String> partitionColumnNames = new ArrayList<>();

      // Iterate over the list of window stages, and add the definition for each window stage to QueryDefinitionBuilder.
      for (int i = 0; i < stages.size(); i++) {
        for (WindowOperatorFactory operatorFactory : stages.get(i).getWindowOperatorFactories()) {
          // Need to add column names which are present in outputColumnNames and rowSignature but not in bob,
          // since they need to be present in the row signature for this window stage.
          for (String columnName : operatorFactory.getProcessor().getOutputColumnNames()) {
            int indexInRowSignature = rowSignature.indexOf(columnName);
            if (indexInRowSignature != -1 && bob.build().indexOf(columnName) == -1) {
              ColumnType columnType = rowSignature.getColumnType(indexInRowSignature).get();
              bob.add(columnName, columnType);
              log.info("Added column [%s] of type [%s] to row signature for window stage.", columnName, columnType);
            } else {
              throw new ISE(
                  "Found unexpected column [%s] already present in row signature [%s].",
                  columnName,
                  rowSignature
              );
            }
          }
        }

        final RowSignature intermediateSignature = bob.build();
        final RowSignature stageRowSignature;

        if (i + 1 == stages.size()) {
          stageRowSignature = finalWindowStageRowSignature;
          nextShuffleSpec = finalWindowStageShuffleSpec;
        } else {
          nextShuffleSpec = stages.get(i + 1).findShuffleSpec(queryKitSpec.getNumPartitionsForShuffle());
          if (nextShuffleSpec == null) {
            stageRowSignature = intermediateSignature;
          } else {
            stageRowSignature = QueryKitUtils.sortableSignature(
                intermediateSignature,
                nextShuffleSpec.clusterBy().getColumns()
            );
          }
        }

        log.info("Using row signature [%s] for window stage.", stageRowSignature);

        final List<String> currentPartitionColumns = stages.get(i).getPartitionColumns();
        if (!currentPartitionColumns.isEmpty()) {
          partitionColumnNames = currentPartitionColumns;
        }

        log.info(
            "Columns which would be used to define partitioning boundaries for this window stage are [%s]",
            partitionColumnNames
        );

        queryDefBuilder.add(
            StageDefinition.builder(firstStageNumber + i)
                           .inputs(new StageInputSpec(firstStageNumber + i - 1))
                           .signature(stageRowSignature)
                           .maxWorkerCount(queryKitSpec.getMaxNonLeafWorkerCount())
                           .shuffleSpec(nextShuffleSpec)
                           .processorFactory(new WindowOperatorQueryFrameProcessorFactory(
                               queryToRun,
                               stages.get(i).getOperatorFactories(),
                               stageRowSignature,
                               maxRowsMaterialized,
                               partitionColumnNames
                           ))
        );
      }
    }
    return queryDefBuilder.build();
  }

  /**
   *
   * @param originalQuery
   * @return A list of {@link WindowStage}.
   */
  private List<WindowStage> getWindowStagesFromQuery(WindowOperatorQuery originalQuery)
  {
    final List<WindowStage> stages = new ArrayList<>();
    final List<OperatorFactory> operators = originalQuery.getOperators();
    WindowStage currentStage = new WindowStage();

    for (int i = 0; i < operators.size(); i++) {
      OperatorFactory of = operators.get(i);
      currentStage.addOperatorFactory(of);

      if (of instanceof WindowOperatorFactory) {
        // Process consecutive window operators
        while (i + 1 < operators.size() && operators.get(i + 1) instanceof WindowOperatorFactory) {
          i++;
          currentStage.addOperatorFactory(operators.get(i));
        }

        // Finalize the current stage
        stages.add(currentStage);
        currentStage = new WindowStage();
      }
    }

    // There shouldn't be any operators left in currentStage. The last operator should always be WindowOperatorFactory.
    if (!currentStage.getOperatorFactories().isEmpty()) {
      throw new ISE(
          "Found unexpected operators [%s] present in the list of operators [%s].",
          currentStage,
          operators
      );
    }

    return stages;
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

  /**
   * Computes the ClusterBy for the final window stage. We don't have to take the CLUSTERED BY columns into account,
   * as they are handled as {@link org.apache.druid.query.scan.ScanQuery#orderBys}.
   */
  private static ClusterBy computeClusterByForFinalWindowStage(Granularity segmentGranularity)
  {
    final List<KeyColumn> clusterByColumns = Collections.singletonList(new KeyColumn(QueryKitUtils.PARTITION_BOOST_COLUMN, KeyOrder.ASCENDING));
    return QueryKitUtils.clusterByWithSegmentGranularity(new ClusterBy(clusterByColumns, 0), segmentGranularity);
  }

  /**
   * Computes the signature for the final window stage. The finalWindowClusterBy will always have the
   * partition boost column as computed in {@link #computeClusterByForFinalWindowStage(Granularity)}.
   */
  private static RowSignature computeSignatureForFinalWindowStage(RowSignature rowSignature, ClusterBy finalWindowClusterBy, Granularity segmentGranularity)
  {
    final RowSignature.Builder finalWindowStageRowSignatureBuilder = RowSignature.builder()
                                                                                 .addAll(rowSignature)
                                                                                 .add(QueryKitUtils.PARTITION_BOOST_COLUMN, ColumnType.LONG);
    return QueryKitUtils.sortableSignature(
        QueryKitUtils.signatureWithSegmentGranularity(finalWindowStageRowSignatureBuilder.build(), segmentGranularity),
        finalWindowClusterBy.getColumns()
    );
  }

  /**
   * Represents a window stage in a query execution.
   * Each stage can contain a sort operator, a partition operator, and multiple window operators.
   */
  private static class WindowStage
  {
    private NaiveSortOperatorFactory sortOperatorFactory;
    private NaivePartitioningOperatorFactory partitioningOperatorFactory;
    private final List<WindowOperatorFactory> windowOperatorFactories;

    public WindowStage()
    {
      this.windowOperatorFactories = new ArrayList<>();
    }

    public void addOperatorFactory(OperatorFactory op)
    {
      if (op instanceof NaiveSortOperatorFactory) {
        this.sortOperatorFactory = (NaiveSortOperatorFactory) op;
      } else if (op instanceof NaivePartitioningOperatorFactory) {
        this.partitioningOperatorFactory = (NaivePartitioningOperatorFactory) op;
      } else {
        this.windowOperatorFactories.add((WindowOperatorFactory) op);
      }
    }

    public List<OperatorFactory> getOperatorFactories()
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

    public List<String> getPartitionColumns()
    {
      return partitioningOperatorFactory != null ? partitioningOperatorFactory.getPartitionColumns() : new ArrayList<>();
    }

    public List<WindowOperatorFactory> getWindowOperatorFactories()
    {
      return windowOperatorFactories;
    }

    public ShuffleSpec findShuffleSpec(int partitionCount)
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

      List<KeyColumn> keyColsOfWindow = new ArrayList<>();
      for (String partitionColumn : partitioningOperatorFactory.getPartitionColumns()) {
        KeyColumn kc;
        if (sortColumnsMap.get(partitionColumn) == ColumnWithDirection.Direction.DESC) {
          kc = new KeyColumn(partitionColumn, KeyOrder.DESCENDING);
        } else {
          kc = new KeyColumn(partitionColumn, KeyOrder.ASCENDING);
        }
        keyColsOfWindow.add(kc);
      }

      return new HashShuffleSpec(new ClusterBy(keyColsOfWindow, 0), partitionCount);
    }

    @Override
    public String toString()
    {
      return "WindowStage{" +
             "operatorFactories=" + getOperatorFactories() +
             '}';
    }
  }
}
