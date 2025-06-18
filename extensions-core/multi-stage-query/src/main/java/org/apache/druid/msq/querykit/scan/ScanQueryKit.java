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
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.frame.key.KeyOrder;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.msq.input.stage.StageInputSpec;
import org.apache.druid.msq.kernel.MixShuffleSpec;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.QueryDefinitionBuilder;
import org.apache.druid.msq.kernel.ShuffleSpec;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.querykit.DataSourcePlan;
import org.apache.druid.msq.querykit.QueryKit;
import org.apache.druid.msq.querykit.QueryKitSpec;
import org.apache.druid.msq.querykit.QueryKitUtils;
import org.apache.druid.msq.querykit.ShuffleSpecFactories;
import org.apache.druid.msq.querykit.ShuffleSpecFactory;
import org.apache.druid.msq.querykit.common.OffsetLimitStageProcessor;
import org.apache.druid.query.Order;
import org.apache.druid.query.OrderBy;
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
      if (s == null) {
        scanSignature = scanQuery.getRowSignature();
      } else {
        scanSignature = jsonMapper.readValue(s, RowSignature.class);
      }
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
   * 1. There is no cluster by
   * 2. This is an offset which means everything gets funneled into a single partition hence we use MaxCountShuffleSpec
   */
  // No ordering, but there is a limit or an offset. These work by funneling everything through a single partition.
  // So there is no point in forcing any particular partitioning. Since everything is funneled into a single
  // partition without a ClusterBy, we don't need to necessarily create it via the resultShuffleSpecFactory provided
  @Override
  public QueryDefinition makeQueryDefinition(
      final QueryKitSpec queryKitSpec,
      final ScanQuery originalQuery,
      final ShuffleSpecFactory resultShuffleSpecFactory,
      final int minStageNumber
  )
  {
    final QueryDefinitionBuilder queryDefBuilder = QueryDefinition.builder(queryKitSpec.getQueryId());
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

    dataSourcePlan.getSubQueryDefBuilder().ifPresent(queryDefBuilder::addAll);

    final ScanQuery queryToRun = originalQuery.withDataSource(dataSourcePlan.getNewDataSource());
    final int firstStageNumber = Math.max(minStageNumber, queryDefBuilder.getNextStageNumber());
    final RowSignature scanSignature = getAndValidateSignature(queryToRun, jsonMapper);
    final boolean hasLimitOrOffset = queryToRun.isLimited() || queryToRun.getScanRowsOffset() > 0;

    final RowSignature.Builder signatureBuilder = RowSignature.builder().addAll(scanSignature);
    final Granularity segmentGranularity =
        QueryKitUtils.getSegmentGranularityFromContext(jsonMapper, queryToRun.getContext());
    final List<KeyColumn> clusterByColumns = new ArrayList<>();

    // Add regular orderBys.
    for (final OrderBy orderBy : queryToRun.getOrderBys()) {
      clusterByColumns.add(
          new KeyColumn(
              orderBy.getColumnName(),
              orderBy.getOrder() == Order.DESCENDING ? KeyOrder.DESCENDING : KeyOrder.ASCENDING
          )
      );
    }

    clusterByColumns.add(new KeyColumn(QueryKitUtils.PARTITION_BOOST_COLUMN, KeyOrder.ASCENDING));
    signatureBuilder.add(QueryKitUtils.PARTITION_BOOST_COLUMN, ColumnType.LONG);

    final ClusterBy clusterBy =
        QueryKitUtils.clusterByWithSegmentGranularity(new ClusterBy(clusterByColumns, 0), segmentGranularity);
    final ShuffleSpec finalShuffleSpec = resultShuffleSpecFactory.build(clusterBy, false);

    final RowSignature signatureToUse = QueryKitUtils.sortableSignature(
        QueryKitUtils.signatureWithSegmentGranularity(signatureBuilder.build(), segmentGranularity),
        clusterBy.getColumns()
    );

    ShuffleSpec scanShuffleSpec;
    if (hasLimitOrOffset) {
      // If there is a limit spec, check if there are any non-boost columns to sort in.
      boolean requiresSort =
          clusterByColumns.stream()
                          .anyMatch(keyColumn -> !QueryKitUtils.PARTITION_BOOST_COLUMN.equals(keyColumn.columnName()));
      if (requiresSort) {
        // If yes, do a sort into a single partition.
        final long limitHint;

        if (queryToRun.isLimited()
            && queryToRun.getScanRowsOffset() + queryToRun.getScanRowsLimit() > 0 /* overflow check */) {
          limitHint = queryToRun.getScanRowsOffset() + queryToRun.getScanRowsLimit();
        } else {
          limitHint = ShuffleSpec.UNLIMITED;
        }

        scanShuffleSpec = ShuffleSpecFactories.singlePartitionWithLimit(limitHint).build(clusterBy, false);
      } else {
        // If the only clusterBy column is the boost column, we just use a mix shuffle to avoid unused shuffling.
        // Note that we still need the boost column to be present in the row signature, since the limit stage would
        // need it to be populated to do its own shuffling later.
        scanShuffleSpec = MixShuffleSpec.instance();
      }
    } else {
      // If there is no limit spec, apply the final shuffling here itself. This will ensure partition sizes etc are respected.
      scanShuffleSpec = finalShuffleSpec;
    }

    queryDefBuilder.add(
        StageDefinition.builder(Math.max(minStageNumber, queryDefBuilder.getNextStageNumber()))
                       .inputs(dataSourcePlan.getInputSpecs())
                       .broadcastInputs(dataSourcePlan.getBroadcastInputs())
                       .shuffleSpec(scanShuffleSpec)
                       .signature(signatureToUse)
                       .maxWorkerCount(dataSourcePlan.getMaxWorkerCount(queryKitSpec))
                       .processor(new ScanQueryStageProcessor(queryToRun))
    );

    if (hasLimitOrOffset) {
      queryDefBuilder.add(
          StageDefinition.builder(firstStageNumber + 1)
                         .inputs(new StageInputSpec(firstStageNumber))
                         .signature(signatureToUse)
                         .maxWorkerCount(1)
                         .shuffleSpec(finalShuffleSpec) // Apply the final shuffling after limit spec.
                         .processor(
                             new OffsetLimitStageProcessor(
                                 queryToRun.getScanRowsOffset(),
                                 queryToRun.isLimited() ? queryToRun.getScanRowsLimit() : null
                             )
                         )
      );
    }

    return queryDefBuilder.build();
  }
}
