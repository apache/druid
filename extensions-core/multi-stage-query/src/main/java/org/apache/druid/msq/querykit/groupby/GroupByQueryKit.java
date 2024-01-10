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

package org.apache.druid.msq.querykit.groupby;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.frame.key.KeyOrder;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.msq.input.stage.StageInputSpec;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.QueryDefinitionBuilder;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.querykit.DataSourcePlan;
import org.apache.druid.msq.querykit.QueryKit;
import org.apache.druid.msq.querykit.QueryKitUtils;
import org.apache.druid.msq.querykit.ShuffleSpecFactories;
import org.apache.druid.msq.querykit.ShuffleSpecFactory;
import org.apache.druid.msq.querykit.common.OffsetLimitFrameProcessorFactory;
import org.apache.druid.query.Query;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.having.AlwaysHavingSpec;
import org.apache.druid.query.groupby.having.DimFilterHavingSpec;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.NoopLimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.ordering.StringComparator;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class GroupByQueryKit implements QueryKit<GroupByQuery>
{
  private final ObjectMapper jsonMapper;

  public GroupByQueryKit(ObjectMapper jsonMapper)
  {
    this.jsonMapper = jsonMapper;
  }

  @Override
  public QueryDefinition makeQueryDefinition(
      final String queryId,
      final GroupByQuery originalQuery,
      final QueryKit<Query<?>> queryKit,
      final ShuffleSpecFactory resultShuffleSpecFactory,
      final int maxWorkerCount,
      final int minStageNumber
  )
  {
    validateQuery(originalQuery);

    final QueryDefinitionBuilder queryDefBuilder = QueryDefinition.builder().queryId(queryId);
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

    final GroupByQuery queryToRun = (GroupByQuery) originalQuery.withDataSource(dataSourcePlan.getNewDataSource());
    final int firstStageNumber = Math.max(minStageNumber, queryDefBuilder.getNextStageNumber());

    final Granularity segmentGranularity =
        QueryKitUtils.getSegmentGranularityFromContext(jsonMapper, queryToRun.getContext());
    final RowSignature intermediateSignature = computeIntermediateSignature(queryToRun);
    final ClusterBy resultClusterByWithoutGranularity = computeClusterByForResults(queryToRun);
    final ClusterBy resultClusterByWithoutPartitionBoost =
        QueryKitUtils.clusterByWithSegmentGranularity(resultClusterByWithoutGranularity, segmentGranularity);
    final ClusterBy intermediateClusterBy = computeIntermediateClusterBy(queryToRun);
    final boolean doOrderBy = !resultClusterByWithoutPartitionBoost.equals(intermediateClusterBy);
    final boolean doLimitOrOffset =
        queryToRun.getLimitSpec() instanceof DefaultLimitSpec
        && (((DefaultLimitSpec) queryToRun.getLimitSpec()).isLimited()
            || ((DefaultLimitSpec) queryToRun.getLimitSpec()).isOffset());

    final ShuffleSpecFactory shuffleSpecFactoryPreAggregation;
    final ShuffleSpecFactory shuffleSpecFactoryPostAggregation;
    boolean partitionBoost;

    if (intermediateClusterBy.isEmpty() && resultClusterByWithoutPartitionBoost.isEmpty()) {
      // Ignore shuffleSpecFactory, since we know only a single partition will come out, and we can save some effort.
      // This condition will be triggered when we don't have a grouping dimension, no partitioning granularity
      // (PARTITIONED BY ALL) and no ordering/clustering dimensions
      // For example: INSERT INTO foo SELECT COUNT(*) FROM bar PARTITIONED BY ALL
      shuffleSpecFactoryPreAggregation = ShuffleSpecFactories.singlePartition();
      shuffleSpecFactoryPostAggregation = ShuffleSpecFactories.singlePartition();
      partitionBoost = false;
    } else if (doOrderBy) {
      // There can be a situation where intermediateClusterBy is empty, while the resultClusterBy is non-empty
      // if we have PARTITIONED BY on anything except ALL, however we don't have a grouping dimension
      // (i.e. no GROUP BY clause)
      // __time in such queries is generated using either an aggregator (e.g. sum(metric) as __time) or using a
      // post-aggregator (e.g. TIMESTAMP '2000-01-01' as __time)
      // For example: INSERT INTO foo SELECT COUNT(*), TIMESTAMP '2000-01-01' AS __time FROM bar PARTITIONED BY DAY
      shuffleSpecFactoryPreAggregation = intermediateClusterBy.isEmpty()
                                         ? ShuffleSpecFactories.singlePartition()
                                         : ShuffleSpecFactories.globalSortWithMaxPartitionCount(maxWorkerCount);
      shuffleSpecFactoryPostAggregation = doLimitOrOffset
                                          ? ShuffleSpecFactories.singlePartition()
                                          : resultShuffleSpecFactory;
      partitionBoost = true;
    } else {
      shuffleSpecFactoryPreAggregation = doLimitOrOffset
                                         ? ShuffleSpecFactories.singlePartition()
                                         : resultShuffleSpecFactory;

      // null: retain partitions from input (i.e. from preAggregation).
      shuffleSpecFactoryPostAggregation = null;
      partitionBoost = false;
    }

    queryDefBuilder.add(
        StageDefinition.builder(firstStageNumber)
                       .inputs(dataSourcePlan.getInputSpecs())
                       .broadcastInputs(dataSourcePlan.getBroadcastInputs())
                       .signature(intermediateSignature)
                       .shuffleSpec(shuffleSpecFactoryPreAggregation.build(intermediateClusterBy, true))
                       .maxWorkerCount(dataSourcePlan.isSingleWorker() ? 1 : maxWorkerCount)
                       .processorFactory(new GroupByPreShuffleFrameProcessorFactory(queryToRun))
    );

    ClusterBy resultClusterBy = computeResultClusterBy(
        queryToRun,
        segmentGranularity,
        partitionBoost
    );
    RowSignature resultSignature = computeResultSignature(
        queryToRun,
        segmentGranularity,
        resultClusterBy,
        partitionBoost
    );

    queryDefBuilder.add(
        StageDefinition.builder(firstStageNumber + 1)
                       .inputs(new StageInputSpec(firstStageNumber))
                       .signature(resultSignature)
                       .maxWorkerCount(maxWorkerCount)
                       .shuffleSpec(
                           shuffleSpecFactoryPostAggregation != null
                           ? shuffleSpecFactoryPostAggregation.build(resultClusterBy, false)
                           : null
                       )
                       .processorFactory(new GroupByPostShuffleFrameProcessorFactory(queryToRun))
    );

    if (doLimitOrOffset) {
      final DefaultLimitSpec limitSpec = (DefaultLimitSpec) queryToRun.getLimitSpec();
      queryDefBuilder.add(
          StageDefinition.builder(firstStageNumber + 2)
                         .inputs(new StageInputSpec(firstStageNumber + 1))
                         .signature(resultSignature)
                         .maxWorkerCount(1)
                         .shuffleSpec(null) // no shuffling should be required after a limit processor.
                         .processorFactory(
                             new OffsetLimitFrameProcessorFactory(
                                 limitSpec.getOffset(),
                                 limitSpec.isLimited() ? (long) limitSpec.getLimit() : null
                             )
                         )
      );
    }

    return queryDefBuilder.queryId(queryId).build();
  }

  /**
   * Intermediate signature of a particular {@link GroupByQuery}. Does not include post-aggregators, and all
   * aggregations are nonfinalized.
   */
  private static RowSignature computeIntermediateSignature(final GroupByQuery query)
  {
    final RowSignature postAggregationSignature = query.getResultRowSignature(RowSignature.Finalization.NO);
    final RowSignature.Builder builder = RowSignature.builder();

    for (int i = 0; i < query.getResultRowSizeWithoutPostAggregators(); i++) {
      builder.add(
          postAggregationSignature.getColumnName(i),
          postAggregationSignature.getColumnType(i).orElse(null)
      );
    }

    return builder.build();
  }

  /**
   * Result signature of a particular {@link GroupByQuery}. Includes post-aggregators, and aggregations are
   * finalized by default. (But may be nonfinalized, depending on {@link #isFinalize}.
   */
  private static RowSignature computeResultSignature(final GroupByQuery query)
  {
    final RowSignature.Finalization finalization =
        isFinalize(query) ? RowSignature.Finalization.YES : RowSignature.Finalization.NO;
    return query.getResultRowSignature(finalization);
  }

  /**
   * Computes the result clusterBy which may or may not have the partition boosted column, depending on the
   * {@code partitionBoost} parameter passed
   */
  private static ClusterBy computeResultClusterBy(
      final GroupByQuery query,
      final Granularity segmentGranularity,
      final boolean partitionBoost
  )
  {
    final ClusterBy resultClusterByWithoutGranularity = computeClusterByForResults(query);
    final ClusterBy resultClusterByWithoutPartitionBoost =
        QueryKitUtils.clusterByWithSegmentGranularity(resultClusterByWithoutGranularity, segmentGranularity);
    if (!partitionBoost) {
      return resultClusterByWithoutPartitionBoost;
    }
    List<KeyColumn> resultClusterByWithPartitionBoostColumns = new ArrayList<>(resultClusterByWithoutPartitionBoost.getColumns());
    resultClusterByWithPartitionBoostColumns.add(new KeyColumn(
        QueryKitUtils.PARTITION_BOOST_COLUMN,
        KeyOrder.ASCENDING
    ));
    return new ClusterBy(
        resultClusterByWithPartitionBoostColumns,
        resultClusterByWithoutPartitionBoost.getBucketByCount()
    );
  }

  /**
   * Computes the result signature which may or may not have the partition boosted column depending on the
   * {@code partitionBoost} passed. It expects that the clusterBy already has the partition boost column
   * if the parameter {@code partitionBoost} is set as true.
   */
  private static RowSignature computeResultSignature(
      final GroupByQuery query,
      final Granularity segmentGranularity,
      final ClusterBy resultClusterBy,
      final boolean partitionBoost
  )
  {
    final RowSignature resultSignatureWithoutPartitionBoost =
        QueryKitUtils.signatureWithSegmentGranularity(computeResultSignature(query), segmentGranularity);

    if (!partitionBoost) {
      return QueryKitUtils.sortableSignature(resultSignatureWithoutPartitionBoost, resultClusterBy.getColumns());
    }

    final RowSignature resultSignatureWithPartitionBoost =
        RowSignature.builder().addAll(resultSignatureWithoutPartitionBoost)
                    .add(QueryKitUtils.PARTITION_BOOST_COLUMN, ColumnType.LONG)
                    .build();

    return QueryKitUtils.sortableSignature(resultSignatureWithPartitionBoost, resultClusterBy.getColumns());
  }

  /**
   * Whether aggregations appearing in the result of a query must be finalized.
   *
   * There is a discrepancy here with native execution. By default, native execution finalizes outer queries only.
   * Here, we finalize all queries, including subqueries.
   */
  static boolean isFinalize(final GroupByQuery query)
  {
    return query.context().isFinalize(true);
  }

  /**
   * Clustering for the intermediate shuffle in a groupBy query.
   */
  static ClusterBy computeIntermediateClusterBy(final GroupByQuery query)
  {
    final List<KeyColumn> columns = new ArrayList<>();

    for (final DimensionSpec dimension : query.getDimensions()) {
      columns.add(new KeyColumn(dimension.getOutputName(), KeyOrder.ASCENDING));
    }

    // Note: ignoring time because we assume granularity = all.
    return new ClusterBy(columns, 0);
  }

  /**
   * Clustering for the results of a groupBy query.
   */
  static ClusterBy computeClusterByForResults(final GroupByQuery query)
  {
    if (query.getLimitSpec() instanceof DefaultLimitSpec) {
      final DefaultLimitSpec defaultLimitSpec = (DefaultLimitSpec) query.getLimitSpec();

      if (!defaultLimitSpec.getColumns().isEmpty()) {
        final List<KeyColumn> clusterByColumns = new ArrayList<>();

        for (final OrderByColumnSpec orderBy : defaultLimitSpec.getColumns()) {
          clusterByColumns.add(
              new KeyColumn(
                  orderBy.getDimension(),
                  orderBy.getDirection() == OrderByColumnSpec.Direction.DESCENDING
                  ? KeyOrder.DESCENDING
                  : KeyOrder.ASCENDING
              )
          );
        }

        return new ClusterBy(clusterByColumns, 0);
      }
    }

    return computeIntermediateClusterBy(query);
  }

  /**
   * Returns silently if the provided {@link GroupByQuery} is supported by this kit. Throws an exception otherwise.
   *
   * @throws IllegalStateException if the query is not supported
   */
  private static void validateQuery(final GroupByQuery query)
  {
    // Misc features that we do not support right now.
    Preconditions.checkState(!query.getContextSortByDimsFirst(), "Must not sort by dims first");
    Preconditions.checkState(query.getSubtotalsSpec() == null, "Must not have 'subtotalsSpec'");
    // Matches condition in GroupByPostShuffleWorker.makeHavingFilter.
    Preconditions.checkState(
        query.getHavingSpec() == null
        || query.getHavingSpec() instanceof DimFilterHavingSpec
        || query.getHavingSpec() instanceof AlwaysHavingSpec,
        "Must use 'filter' or 'always' havingSpec"
    );
    Preconditions.checkState(query.getGranularity().equals(Granularities.ALL), "Must have granularity 'all'");
    Preconditions.checkState(
        query.getLimitSpec() instanceof NoopLimitSpec || query.getLimitSpec() instanceof DefaultLimitSpec,
        "Must have noop or default limitSpec"
    );

    final RowSignature resultSignature = computeResultSignature(query);
    QueryKitUtils.verifyRowSignature(resultSignature);

    if (query.getLimitSpec() instanceof DefaultLimitSpec) {
      final DefaultLimitSpec defaultLimitSpec = (DefaultLimitSpec) query.getLimitSpec();

      for (final OrderByColumnSpec column : defaultLimitSpec.getColumns()) {
        final Optional<ColumnType> type = resultSignature.getColumnType(column.getDimension());

        if (!type.isPresent() || !isNaturalComparator(type.get().getType(), column.getDimensionComparator())) {
          throw new ISE(
              "Must use natural comparator for column [%s] of type [%s]",
              column.getDimension(),
              type.orElse(null)
          );
        }
      }
    }
  }

  /**
   * Only allow ordering the queries from the MSQ engine, ignoring the comparator that is set in the query. This
   * function checks if it is safe to do so, which is the case if the natural comparator is used for the dimension.
   * Since MSQ executes the queries planned by the SQL layer, this is a sanity check as we always add the natural
   * comparator for the dimensions there
   */
  private static boolean isNaturalComparator(final ValueType type, final StringComparator comparator)
  {
    if (StringComparators.NATURAL.equals(comparator)) {
      return true;
    }
    return ((type == ValueType.STRING && StringComparators.LEXICOGRAPHIC.equals(comparator))
            || (type.isNumeric() && StringComparators.NUMERIC.equals(comparator)))
           && !type.isArray();
  }
}
