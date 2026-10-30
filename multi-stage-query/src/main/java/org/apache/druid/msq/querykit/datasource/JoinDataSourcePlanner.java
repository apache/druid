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

package org.apache.druid.msq.querykit.datasource;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.exec.Limits;
import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.msq.input.stage.StageInputSpec;
import org.apache.druid.msq.kernel.HashShuffleSpec;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.QueryDefinitionBuilder;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.kernel.StageDefinitionBuilder;
import org.apache.druid.msq.querykit.DataSourcePlan;
import org.apache.druid.msq.querykit.DataSourcePlanner;
import org.apache.druid.msq.querykit.InputNumberDataSource;
import org.apache.druid.msq.querykit.QueryKitSpec;
import org.apache.druid.msq.querykit.QueryKitUtils;
import org.apache.druid.msq.querykit.common.SortMergeJoinStageProcessor;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.JoinAlgorithm;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.planning.JoinDataSourceAnalysis;
import org.apache.druid.query.planning.PreJoinableClause;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.JoinConditionAnalysis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Planner for {@link JoinDataSource}. Dispatches to broadcast hash-join or sort-merge join, based on
 * {@link #deduceJoinAlgorithm}.
 */
public class JoinDataSourcePlanner implements DataSourcePlanner<JoinDataSource>
{
  private static final Logger log = new Logger(JoinDataSourcePlanner.class);

  @Override
  public DataSourcePlan planDataSource(
      final QueryKitSpec queryKitSpec,
      final QueryContext queryContext,
      final JoinDataSource dataSource,
      final QuerySegmentSpec querySegmentSpec,
      final int minStageNumber,
      final boolean broadcast
  )
  {
    final JoinAlgorithm preferredJoinAlgorithm = dataSource.getJoinAlgorithm();
    final JoinAlgorithm deducedJoinAlgorithm = deduceJoinAlgorithm(preferredJoinAlgorithm, dataSource);

    return switch (deducedJoinAlgorithm) {
      case BROADCAST -> planBroadcastHashJoin(
          queryKitSpec,
          queryContext,
          dataSource,
          querySegmentSpec,
          minStageNumber,
          broadcast
      );
      case SORT_MERGE -> planSortMergeJoin(
          queryKitSpec,
          queryContext,
          dataSource,
          querySegmentSpec,
          minStageNumber,
          broadcast
      );
    };
  }

  /**
   * Build a plan for broadcast hash-join.
   */
  private static DataSourcePlan planBroadcastHashJoin(
      final QueryKitSpec queryKitSpec,
      final QueryContext queryContext,
      final JoinDataSource dataSource,
      final QuerySegmentSpec querySegmentSpec,
      final int minStageNumber,
      final boolean broadcast
  )
  {
    final QueryDefinitionBuilder subQueryDefBuilder = QueryDefinition.builder(queryKitSpec.getQueryId());
    final JoinDataSourceAnalysis analysis = dataSource.getJoinAnalysisForDataSource();

    final DataSourcePlan basePlan = DataSourcePlan.forDataSource(
        queryKitSpec,
        queryContext,
        analysis.getBaseDataSource(),
        querySegmentSpec,
        Math.max(minStageNumber, subQueryDefBuilder.getNextStageNumber()),
        broadcast
    );

    DataSource newDataSource = basePlan.getNewDataSource();
    final List<InputSpec> inputSpecs = new ArrayList<>(basePlan.getInputSpecs());
    final IntSet broadcastInputs = new IntOpenHashSet(basePlan.getBroadcastInputs());
    basePlan.getSubQueryDefBuilder().ifPresent(subQueryDefBuilder::addAll);

    for (int i = 0; i < analysis.getPreJoinableClauses().size(); i++) {
      final PreJoinableClause clause = analysis.getPreJoinableClauses().get(i);
      final DataSourcePlan clausePlan = DataSourcePlan.forDataSource(
          queryKitSpec,
          queryContext,
          clause.getDataSource(),
          new MultipleIntervalSegmentSpec(Intervals.ONLY_ETERNITY),
          Math.max(minStageNumber, subQueryDefBuilder.getNextStageNumber()),
          true // Always broadcast right-hand side of the join.
      );

      // Shift all input numbers in the clausePlan.
      final int shift = inputSpecs.size();

      newDataSource = JoinDataSource.create(
          newDataSource,
          DataSourcePlannerUtils.shiftInputNumbers(clausePlan.getNewDataSource(), shift),
          clause.getPrefix(),
          clause.getCondition(),
          clause.getJoinType(),
          // First JoinDataSource (i == 0) involves the base table, so we need to propagate the base table filter.
          i == 0 ? analysis.getJoinBaseTableFilter().orElse(null) : null,
          dataSource.getJoinableFactoryWrapper(),
          clause.getJoinAlgorithm()
      );
      inputSpecs.addAll(clausePlan.getInputSpecs());
      clausePlan.getBroadcastInputs().intStream().forEach(n -> broadcastInputs.add(n + shift));
      clausePlan.getSubQueryDefBuilder().ifPresent(subQueryDefBuilder::addAll);
    }

    return new DataSourcePlan(newDataSource, inputSpecs, broadcastInputs, subQueryDefBuilder);
  }

  /**
   * Build a plan for sort-merge join.
   */
  private static DataSourcePlan planSortMergeJoin(
      final QueryKitSpec queryKitSpec,
      final QueryContext queryContext,
      final JoinDataSource dataSource,
      final QuerySegmentSpec querySegmentSpec,
      final int minStageNumber,
      final boolean broadcast
  )
  {
    DataSourcePlannerUtils.checkQuerySegmentSpecIsEternity(dataSource, querySegmentSpec);
    SortMergeJoinStageProcessor.validateCondition(dataSource.getConditionAnalysis());

    // Partition by keys given by the join condition.
    final List<List<KeyColumn>> partitionKeys = SortMergeJoinStageProcessor.toKeyColumns(
        SortMergeJoinStageProcessor.validateCondition(dataSource.getConditionAnalysis())
    );

    final QueryDefinitionBuilder subQueryDefBuilder = QueryDefinition.builder(queryKitSpec.getQueryId());

    // Plan the left input.
    // We're confident that we can cast dataSource.getLeft() to QueryDataSource, because DruidJoinQueryRel creates
    // subqueries when the join algorithm is sortMerge.
    final DataSourcePlan leftPlan = DataSourcePlan.forDataSource(
        queryKitSpec,
        queryContext,
        (QueryDataSource) dataSource.getLeft(),
        querySegmentSpec,
        Math.max(minStageNumber, subQueryDefBuilder.getNextStageNumber()),
        false
    );
    leftPlan.getSubQueryDefBuilder().ifPresent(subQueryDefBuilder::addAll);

    // Plan the right input.
    // We're confident that we can cast dataSource.getRight() to QueryDataSource, because DruidJoinQueryRel creates
    // subqueries when the join algorithm is sortMerge.
    final DataSourcePlan rightPlan = DataSourcePlan.forDataSource(
        queryKitSpec,
        queryContext,
        (QueryDataSource) dataSource.getRight(),
        querySegmentSpec,
        Math.max(minStageNumber, subQueryDefBuilder.getNextStageNumber()),
        false
    );
    rightPlan.getSubQueryDefBuilder().ifPresent(subQueryDefBuilder::addAll);

    // Build up the left stage.
    final StageDefinitionBuilder leftBuilder = subQueryDefBuilder.getStageBuilder(
        ((StageInputSpec) Iterables.getOnlyElement(leftPlan.getInputSpecs())).getStageNumber()
    );

    final List<KeyColumn> leftPartitionKey = partitionKeys.get(0);
    leftBuilder.shuffleSpec(new HashShuffleSpec(new ClusterBy(leftPartitionKey, 0), 1, true));
    leftBuilder.signature(QueryKitUtils.sortableSignature(leftBuilder.getSignature(), leftPartitionKey));
    leftBuilder.maxWorkerCount(Limits.MAX_WORKERS);

    // Build up the right stage.
    final StageDefinitionBuilder rightBuilder = subQueryDefBuilder.getStageBuilder(
        ((StageInputSpec) Iterables.getOnlyElement(rightPlan.getInputSpecs())).getStageNumber()
    );

    final List<KeyColumn> rightPartitionKey = partitionKeys.get(1);
    rightBuilder.shuffleSpec(new HashShuffleSpec(new ClusterBy(rightPartitionKey, 0), 1, true));
    rightBuilder.signature(QueryKitUtils.sortableSignature(rightBuilder.getSignature(), rightPartitionKey));
    rightBuilder.maxWorkerCount(Limits.MAX_WORKERS);

    // Compute join signature.
    final RowSignature.Builder joinSignatureBuilder = RowSignature.builder();

    for (final String leftColumn : leftBuilder.getSignature().getColumnNames()) {
      joinSignatureBuilder.add(leftColumn, leftBuilder.getSignature().getColumnType(leftColumn).orElse(null));
    }

    for (final String rightColumn : rightBuilder.getSignature().getColumnNames()) {
      joinSignatureBuilder.add(
          dataSource.getRightPrefix() + rightColumn,
          rightBuilder.getSignature().getColumnType(rightColumn).orElse(null)
      );
    }

    // Build up the join stage.
    final int stageNumber = Math.max(minStageNumber, subQueryDefBuilder.getNextStageNumber());

    subQueryDefBuilder.add(
        StageDefinition.builder(stageNumber)
                       .inputs(
                           ImmutableList.of(
                               Iterables.getOnlyElement(leftPlan.getInputSpecs()),
                               Iterables.getOnlyElement(rightPlan.getInputSpecs())
                           )
                       )
                       .maxWorkerCount(Limits.MAX_WORKERS)
                       .signature(joinSignatureBuilder.build())
                       .processor(
                           new SortMergeJoinStageProcessor(
                               dataSource.getRightPrefix(),
                               dataSource.getConditionAnalysis(),
                               dataSource.getJoinType()
                           )
                       )
    );

    return new DataSourcePlan(
        new InputNumberDataSource(0),
        Collections.singletonList(new StageInputSpec(stageNumber)),
        broadcast ? IntOpenHashSet.of(0) : IntSets.emptySet(),
        subQueryDefBuilder
    );
  }

  /**
   * Contains the logic that deduces the join algorithm to be used. Ideally, this should reside while planning the
   * native query, however we don't have the resources and the structure in place (when adding this function) to do so.
   * Therefore, this is done while planning the MSQ query
   * It takes into account the algorithm specified by "sqlJoinAlgorithm" in the query context and the join condition
   * that is present in the query.
   */
  private static JoinAlgorithm deduceJoinAlgorithm(
      final JoinAlgorithm preferredJoinAlgorithm,
      final JoinDataSource joinDataSource
  )
  {
    final JoinAlgorithm deducedJoinAlgorithm;
    if (JoinAlgorithm.BROADCAST.equals(preferredJoinAlgorithm)) {
      deducedJoinAlgorithm = JoinAlgorithm.BROADCAST;
    } else if (canUseSortMergeJoin(joinDataSource.getConditionAnalysis())) {
      deducedJoinAlgorithm = JoinAlgorithm.SORT_MERGE;
    } else {
      deducedJoinAlgorithm = JoinAlgorithm.BROADCAST;
    }

    if (deducedJoinAlgorithm != preferredJoinAlgorithm) {
      log.debug(
          "User wanted to plan join [%s] as [%s], however the join will be executed as [%s]",
          joinDataSource,
          preferredJoinAlgorithm.toString(),
          deducedJoinAlgorithm.toString()
      );
    }

    return deducedJoinAlgorithm;
  }

  /**
   * Checks if the sortMerge algorithm can execute a particular join condition.
   * <p>
   * One check: join condition on two tables "table1" and "table2" is of the form
   * table1.columnA = table2.columnA && table1.columnB = table2.columnB && ....
   */
  private static boolean canUseSortMergeJoin(final JoinConditionAnalysis joinConditionAnalysis)
  {
    return joinConditionAnalysis
        .getEquiConditions()
        .stream()
        .allMatch(equality -> equality.getLeftExpr().isIdentifier());
  }
}
