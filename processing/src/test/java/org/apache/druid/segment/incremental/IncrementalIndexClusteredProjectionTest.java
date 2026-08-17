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

package org.apache.druid.segment.incremental;

import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.ClusteredValueGroupsBaseTableProjectionSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupingEngine;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec.Direction;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class IncrementalIndexClusteredProjectionTest extends InitializedNullHandlingTest
{
  private static final long T0 = DateTimes.of("2026-01-01T00:00:00").getMillis();
  private static final TimestampSpec TIMESTAMP_SPEC = new TimestampSpec("ts", "millis", null);

  private static MapBasedInputRow row(long ts, String tenant, String region, long x)
  {
    final Map<String, Object> event = new HashMap<>();
    event.put("ts", ts);
    event.put("tenant", tenant);
    event.put("region", region);
    event.put("x", x);
    return new MapBasedInputRow(ts, List.of("tenant", "region", "x"), event);
  }

  private static OnheapIncrementalIndex clusteredWithProjection()
  {
    final ClusteredValueGroupsBaseTableProjectionSpec spec = ClusteredValueGroupsBaseTableProjectionSpec.builder()
        .columns(
            new StringDimensionSchema("tenant"),
            new StringDimensionSchema("region"),
            new LongDimensionSchema("x"),
            new LongDimensionSchema("__time")
        )
        .clusteringColumns("tenant")
        .build();
    final AggregateProjectionSpec projectionSpec =
        AggregateProjectionSpec.builder("proj")
                               .groupingColumns(new StringDimensionSchema("tenant"))
                               .aggregators(
                                   new CountAggregatorFactory("cnt"),
                                   new LongSumAggregatorFactory("sum_x", "x")
                               )
                               .build();
    final IncrementalIndexSchema schema = IncrementalIndexSchema.builder()
        .withMinTimestamp(T0)
        .withTimestampSpec(TIMESTAMP_SPEC)
        .withQueryGranularity(Granularities.NONE)
        .withDimensionsSpec(spec.getDimensionsSpec())
        .withRollup(false)
        .withClusterSpec(spec)
        .withProjections(List.of(projectionSpec))
        .build();
    final OnheapIncrementalIndex index = (OnheapIncrementalIndex) new OnheapIncrementalIndex.Builder()
        .setIndexSchema(schema)
        .setMaxRowCount(10_000)
        .build();
    // Add tenants out of clustering order to prove the plain scan still walks groups in clustering-sorted order.
    index.add(row(T0 + 2, "globex", "eu-west-1", 7));
    index.add(row(T0, "acme", "us-east-1", 10));
    index.add(row(T0 + 1, "acme", "us-west-2", 5));
    return index;
  }

  /**
   * A group-by on the projection's grouping column + aggregators, granularity ALL over ETERNITY. Built through the
   * real {@link GroupingEngine#makeCursorBuildSpec} path so the resulting spec is what production planning produces.
   */
  private static CursorBuildSpec projectionMatchingBuildSpec()
  {
    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setGranularity(Granularities.ALL)
                    .setInterval(Intervals.ETERNITY)
                    .addDimension("tenant")
                    .addAggregator(new CountAggregatorFactory("cnt"))
                    .addAggregator(new LongSumAggregatorFactory("sum_x", "x"))
                    .addOrderByColumn("tenant", Direction.ASCENDING)
                    .build();
    return GroupingEngine.makeCursorBuildSpec(query, null);
  }

  @Test
  void testProjectionMaterializedOnClusteredIndex()
  {
    try (OnheapIncrementalIndex index = clusteredWithProjection()) {
      // Construction did not throw (the OnheapIncrementalIndex guard against clusterSpec + projections was removed)
      // and the projection accumulated rows during ingest.
      Assertions.assertNotNull(index.getProjection("proj"));
    }
  }

  @Test
  void testProjectionSelectedAndAggregatesCorrectlyOnClusteredIndex()
  {
    try (OnheapIncrementalIndex index = clusteredWithProjection()) {
      final CursorBuildSpec buildSpec = projectionMatchingBuildSpec();

      // The projection must actually be selected for this spec on a clustered index -- not merely present. If this is
      // null, guard removal alone is insufficient and the query path needs more work.
      Assertions.assertNotNull(
          index.getProjection(buildSpec),
          "projection 'proj' was not selected for the group-by build spec on the clustered index"
      );

      final IncrementalIndexCursorFactory factory = new IncrementalIndexCursorFactory(index);
      try (CursorHolder holder = factory.makeCursorHolder(buildSpec)) {
        // The chosen holder is the aggregate-projection cursor: it serves pre-aggregated rows.
        Assertions.assertTrue(holder.isPreAggregated(), "expected the projection (pre-aggregated) cursor holder");

        final Cursor cursor = holder.asCursor();
        final DimensionSelector tenantSel =
            cursor.getColumnSelectorFactory().makeDimensionSelector(DefaultDimensionSpec.of("tenant"));
        final ColumnValueSelector<?> cntSel = cursor.getColumnSelectorFactory().makeColumnValueSelector("cnt");
        final ColumnValueSelector<?> sumSel = cursor.getColumnSelectorFactory().makeColumnValueSelector("sum_x");

        final Map<String, long[]> byTenant = new LinkedHashMap<>();
        while (!cursor.isDone()) {
          final String tenant = tenantSel.lookupName(tenantSel.getRow().get(0));
          byTenant.put(tenant, new long[]{cntSel.getLong(), sumSel.getLong()});
          cursor.advance();
        }

        // acme: 2 rows (x=10, x=5) -> cnt=2, sum_x=15 ; globex: 1 row (x=7) -> cnt=1, sum_x=7
        Assertions.assertEquals(2, byTenant.size(), "expected one pre-aggregated row per tenant");
        Assertions.assertArrayEquals(new long[]{2L, 15L}, byTenant.get("acme"));
        Assertions.assertArrayEquals(new long[]{1L, 7L}, byTenant.get("globex"));
      }
    }
  }

  @Test
  void testBaseClusteredScanStillWalksGroupsInClusteringOrder()
  {
    try (OnheapIncrementalIndex index = clusteredWithProjection()) {
      final IncrementalIndexCursorFactory factory = new IncrementalIndexCursorFactory(index);
      // FULL_SCAN does not match the projection, so it falls through to the clustered base-table path and walks the
      // cluster groups in clustering-ascending order (acme rows before globex), exactly as the non-projection
      // clustered index does.
      try (CursorHolder holder = factory.makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
        Assertions.assertFalse(
            holder.isPreAggregated(),
            "FULL_SCAN must not pick the projection; it should use the clustered base-table cursor"
        );
        final Cursor cursor = holder.asCursor();
        final DimensionSelector tenantSel =
            cursor.getColumnSelectorFactory().makeDimensionSelector(DefaultDimensionSpec.of("tenant"));
        final DimensionSelector regionSel =
            cursor.getColumnSelectorFactory().makeDimensionSelector(DefaultDimensionSpec.of("region"));
        final List<List<String>> out = new ArrayList<>();
        while (!cursor.isDone()) {
          final String tenant = tenantSel.getRow().size() == 0 ? null : tenantSel.lookupName(tenantSel.getRow().get(0));
          final String region = regionSel.getRow().size() == 0 ? null : regionSel.lookupName(regionSel.getRow().get(0));
          out.add(Arrays.asList(tenant, region));
          cursor.advance();
        }
        Assertions.assertEquals(
            List.of(
                List.of("acme", "us-east-1"),
                List.of("acme", "us-west-2"),
                List.of("globex", "eu-west-1")
            ),
            out
        );
      }
    }
  }
}
