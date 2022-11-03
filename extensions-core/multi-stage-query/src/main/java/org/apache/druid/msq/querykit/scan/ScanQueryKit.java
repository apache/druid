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

package org.apache.druid.msq.querykit.scan;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.SortColumn;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.msq.input.stage.StageInputSpec;
import org.apache.druid.msq.kernel.MaxCountShuffleSpec;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.QueryDefinitionBuilder;
import org.apache.druid.msq.kernel.ShuffleSpec;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.querykit.DataSourcePlan;
import org.apache.druid.msq.querykit.QueryKit;
import org.apache.druid.msq.querykit.QueryKitUtils;
import org.apache.druid.msq.querykit.ShuffleSpecFactory;
import org.apache.druid.msq.querykit.common.OffsetLimitFrameProcessorFactory;
import org.apache.druid.query.Query;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.rel.DruidQuery;

import java.util.ArrayList;
import java.util.List;

public class ScanQueryKit implements QueryKit<ScanQuery>
{
  private final ObjectMapper jsonMapper;

  public ScanQueryKit(final ObjectMapper jsonMapper)
  {
    this.jsonMapper = jsonMapper;
  }

  public static RowSignature getAndValidateSignature(final ScanQuery scanQuery, final ObjectMapper jsonMapper)
  {
    RowSignature scanSignature;
    try {
      final String s = scanQuery.context().getString(DruidQuery.CTX_SCAN_SIGNATURE);
      scanSignature = jsonMapper.readValue(s, RowSignature.class);
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
    // Verify the signature prior to any actual processing.
    QueryKitUtils.verifyRowSignature(scanSignature);
    return scanSignature;
  }

  /**
   * We ignore the resultShuffleSpecFactory in case:
   *  1. There is no cluster by
   *  2. This is an offset which means everything gets funneled into a single partition hence we use MaxCountShuffleSpec
   */
  // No ordering, but there is a limit or an offset. These work by funneling everything through a single partition.
  // So there is no point in forcing any particular partitioning. Since everything is funneled into a single
  // partition without a ClusterBy, we don't need to necessarily create it via the resultShuffleSpecFactory provided
  @Override
  public QueryDefinition makeQueryDefinition(
      final String queryId,
      final ScanQuery originalQuery,
      final QueryKit<Query<?>> queryKit,
      final ShuffleSpecFactory resultShuffleSpecFactory,
      final int maxWorkerCount,
      final int minStageNumber
  )
  {
    final QueryDefinitionBuilder queryDefBuilder = QueryDefinition.builder().queryId(queryId);
    final DataSourcePlan dataSourcePlan = DataSourcePlan.forDataSource(
        queryKit,
        queryId,
        originalQuery.getDataSource(),
        originalQuery.getQuerySegmentSpec(),
        originalQuery.getFilter(),
        maxWorkerCount,
        minStageNumber,
        false
    );

    dataSourcePlan.getSubQueryDefBuilder().ifPresent(queryDefBuilder::addAll);

    final ScanQuery queryToRun = originalQuery.withDataSource(dataSourcePlan.getNewDataSource());
    final int firstStageNumber = Math.max(minStageNumber, queryDefBuilder.getNextStageNumber());
    final RowSignature scanSignature = getAndValidateSignature(queryToRun, jsonMapper);
    final ShuffleSpec shuffleSpec;
    final RowSignature signatureToUse;
    final boolean hasLimitOrOffset = queryToRun.isLimited() || queryToRun.getScanRowsOffset() > 0;


    // We ignore the resultShuffleSpecFactory in case:
    //  1. There is no cluster by
    //  2. There is an offset which means everything gets funneled into a single partition hence we use MaxCountShuffleSpec
    if (queryToRun.getOrderBys().isEmpty() && hasLimitOrOffset) {
      shuffleSpec = new MaxCountShuffleSpec(ClusterBy.none(), 1, false);
      signatureToUse = scanSignature;
    } else {
      final RowSignature.Builder signatureBuilder = RowSignature.builder().addAll(scanSignature);
      final Granularity segmentGranularity = QueryKitUtils.getSegmentGranularityFromContext(queryToRun.getContext());
      final List<SortColumn> clusterByColumns = new ArrayList<>();

      // Add regular orderBys.
      for (final ScanQuery.OrderBy orderBy : queryToRun.getOrderBys()) {
        clusterByColumns.add(
            new SortColumn(
                orderBy.getColumnName(),
                orderBy.getOrder() == ScanQuery.Order.DESCENDING
            )
        );
      }

      // Add partition boosting column.
      clusterByColumns.add(new SortColumn(QueryKitUtils.PARTITION_BOOST_COLUMN, false));
      signatureBuilder.add(QueryKitUtils.PARTITION_BOOST_COLUMN, ColumnType.LONG);

      final ClusterBy clusterBy =
          QueryKitUtils.clusterByWithSegmentGranularity(new ClusterBy(clusterByColumns, 0), segmentGranularity);
      shuffleSpec = resultShuffleSpecFactory.build(clusterBy, false);
      signatureToUse = QueryKitUtils.sortableSignature(
          QueryKitUtils.signatureWithSegmentGranularity(signatureBuilder.build(), segmentGranularity),
          clusterBy.getColumns()
      );
    }

    queryDefBuilder.add(
        StageDefinition.builder(Math.max(minStageNumber, queryDefBuilder.getNextStageNumber()))
                       .inputs(dataSourcePlan.getInputSpecs())
                       .broadcastInputs(dataSourcePlan.getBroadcastInputs())
                       .shuffleSpec(shuffleSpec)
                       .signature(signatureToUse)
                       .maxWorkerCount(dataSourcePlan.isSingleWorker() ? 1 : maxWorkerCount)
                       .processorFactory(new ScanQueryFrameProcessorFactory(queryToRun))
    );

    if (hasLimitOrOffset) {
      queryDefBuilder.add(
          StageDefinition.builder(firstStageNumber + 1)
                         .inputs(new StageInputSpec(firstStageNumber))
                         .signature(signatureToUse)
                         .maxWorkerCount(1)
                         .shuffleSpec(new MaxCountShuffleSpec(ClusterBy.none(), 1, false))
                         .processorFactory(
                             new OffsetLimitFrameProcessorFactory(
                                 queryToRun.getScanRowsOffset(),
                                 queryToRun.isLimited() ? queryToRun.getScanRowsLimit() : null
                             )
                         )
      );
    }

    return queryDefBuilder.build();
  }
}
