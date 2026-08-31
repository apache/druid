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

package org.apache.druid.segment;

import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.ClusteredValueGroupsBaseTableProjectionSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Order;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupingEngine;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec.Direction;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.projections.ClusteredValueGroupsBaseTableSchema;
import org.apache.druid.segment.projections.TableClusterGroupSpec;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.ClusterGroupTuples;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * End-to-end write-side coverage for clustered base-table segments: ingest into a clustered
 * {@link org.apache.druid.segment.incremental.OnheapIncrementalIndex}, persist + merge through
 * {@link IndexMergerV10}, load with {@link IndexIO}, and read back through the real clustered query path.
 */
class IndexMergerV10ClusteredTest extends InitializedNullHandlingTest
{
  private static final long T0 = DateTimes.of("2026-01-01T00:00:00").getMillis();
  private static final TimestampSpec TIMESTAMP_SPEC = new TimestampSpec("ts", "millis", null);

  @TempDir
  File tempDir;

  private static ClusteredValueGroupsBaseTableProjectionSpec tenantClusterSpec()
  {
    return ClusteredValueGroupsBaseTableProjectionSpec.builder()
        .columns(
            new StringDimensionSchema("tenant"),
            new StringDimensionSchema("region"),
            new LongDimensionSchema("__time")
        )
        .clusteringColumns("tenant")
        .build();
  }

  private static IncrementalIndexSchema clusteredSchema(ClusteredValueGroupsBaseTableProjectionSpec spec)
  {
    return IncrementalIndexSchema.builder()
                                 .withMinTimestamp(T0)
                                 .withTimestampSpec(TIMESTAMP_SPEC)
                                 .withQueryGranularity(Granularities.NONE)
                                 .withDimensionsSpec(spec.getDimensionsSpec())
                                 .withRollup(false)
                                 .withClusterSpec(spec)
                                 .build();
  }

  private static InputRow row(long ts, String tenant, String region)
  {
    final Map<String, Object> event = new HashMap<>();
    event.put("ts", ts);
    event.put("tenant", tenant);
    event.put("region", region);
    return new MapBasedInputRow(ts, List.of("tenant", "region"), event);
  }

  private QueryableIndex buildSegment(String dirName, List<InputRow> rows)
  {
    return IndexBuilder.create()
                       .useV10()
                       .tmpDir(new File(tempDir, dirName))
                       .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                       .schema(clusteredSchema(tenantClusterSpec()))
                       .rows(rows)
                       .buildMMappedIndex();
  }

  /**
   * Walk all (tenant, region) pairs out of the clustered segment via the real cursor path (cluster-group
   * concatenation + injected clustering constants).
   */
  private static List<List<String>> scanTenantRegion(QueryableIndex index)
  {
    final QueryableIndexCursorFactory factory = new QueryableIndexCursorFactory(
        index,
        QueryableIndexTimeBoundaryInspector.create(index)
    );
    final List<List<String>> out = new ArrayList<>();
    try (CursorHolder holder = factory.makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
      final Cursor cursor = holder.asCursor();
      final DimensionSelector tenantSel =
          cursor.getColumnSelectorFactory().makeDimensionSelector(DefaultDimensionSpec.of("tenant"));
      final DimensionSelector regionSel =
          cursor.getColumnSelectorFactory().makeDimensionSelector(DefaultDimensionSpec.of("region"));
      while (!cursor.isDone()) {
        final String tenant = tenantSel.getRow().size() == 0 ? null : tenantSel.lookupName(tenantSel.getRow().get(0));
        final String region = regionSel.getRow().size() == 0 ? null : regionSel.lookupName(regionSel.getRow().get(0));
        out.add(Arrays.asList(tenant, region));
        cursor.advance();
      }
    }
    return out;
  }

  private static ClusteredValueGroupsBaseTableProjectionSpec timeOrderedClusterSpec()
  {
    // __time declared as the first non-clustering column, so the segment ordering is [tenant, __time, region] and each
    // cluster group is individually __time-sorted -- the precondition for the time-ordered merge cursor.
    return ClusteredValueGroupsBaseTableProjectionSpec.builder()
        .columns(
            new StringDimensionSchema("tenant"),
            new LongDimensionSchema("__time"),
            new StringDimensionSchema("region")
        )
        .clusteringColumns("tenant")
        .build();
  }

  private QueryableIndex buildTimeOrderedSegment(String dirName, List<InputRow> rows)
  {
    return IndexBuilder.create()
                       .useV10()
                       .tmpDir(new File(tempDir, dirName))
                       .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                       .schema(clusteredSchema(timeOrderedClusterSpec()))
                       .rows(rows)
                       .buildMMappedIndex();
  }

  /**
   * Walk (@code __time}, {@code region}) pairs from a holder's scalar cursor, in whatever order the holder produces.
   */
  private static List<List<Object>> scanTimeRegion(CursorHolder holder)
  {
    final Cursor cursor = holder.asCursor();
    final ColumnValueSelector timeSelector =
        cursor.getColumnSelectorFactory().makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME);
    final DimensionSelector regionSelector =
        cursor.getColumnSelectorFactory().makeDimensionSelector(DefaultDimensionSpec.of("region"));
    final List<List<Object>> out = new ArrayList<>();
    while (!cursor.isDone()) {
      final String region =
          regionSelector.getRow().size() == 0 ? null : regionSelector.lookupName(regionSelector.getRow().get(0));
      out.add(Arrays.asList(timeSelector.getLong(), region));
      cursor.advance();
    }
    return out;
  }

  /**
   * Cluster spec with a numeric column {@code x} (in addition to {@code tenant}/{@code region}) so an aggregate
   * projection can sum it. Clustering is still on {@code tenant}.
   */
  private static ClusteredValueGroupsBaseTableProjectionSpec tenantClusterSpecWithX()
  {
    return ClusteredValueGroupsBaseTableProjectionSpec.builder()
        .columns(
            new StringDimensionSchema("tenant"),
            new StringDimensionSchema("region"),
            new LongDimensionSchema("x"),
            new LongDimensionSchema("__time")
        )
        .clusteringColumns("tenant")
        .build();
  }

  /**
   * The aggregate projection persisted alongside the clustered base table: group-by {@code tenant} with
   * {@code cnt}=count and {@code sum_x}=sum(x).
   */
  private static AggregateProjectionSpec tenantProjectionSpec()
  {
    return AggregateProjectionSpec.builder("proj")
                                  .groupingColumns(new StringDimensionSchema("tenant"))
                                  .aggregators(
                                      new CountAggregatorFactory("cnt"),
                                      new LongSumAggregatorFactory("sum_x", "x")
                                  )
                                  .build();
  }

  private static IncrementalIndexSchema clusteredSchemaWithProjection(
      ClusteredValueGroupsBaseTableProjectionSpec spec,
      AggregateProjectionSpec projectionSpec
  )
  {
    return IncrementalIndexSchema.builder()
                                 .withMinTimestamp(T0)
                                 .withTimestampSpec(TIMESTAMP_SPEC)
                                 .withQueryGranularity(Granularities.NONE)
                                 .withDimensionsSpec(spec.getDimensionsSpec())
                                 .withRollup(false)
                                 .withClusterSpec(spec)
                                 .withProjections(List.of(projectionSpec))
                                 .build();
  }

  private static InputRow row(long ts, String tenant, String region, long x)
  {
    final Map<String, Object> event = new HashMap<>();
    event.put("ts", ts);
    event.put("tenant", tenant);
    event.put("region", region);
    event.put("x", x);
    return new MapBasedInputRow(ts, List.of("tenant", "region", "x"), event);
  }

  private QueryableIndex buildSegmentWithProjection(String dirName, List<InputRow> rows)
  {
    return IndexBuilder.create()
                       .useV10()
                       .tmpDir(new File(tempDir, dirName))
                       .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                       .schema(clusteredSchemaWithProjection(tenantClusterSpecWithX(), tenantProjectionSpec()))
                       .rows(rows)
                       .buildMMappedIndex();
  }

  /**
   * A group-by on the projection's grouping column + aggregators, granularity ALL over ETERNITY, built through the
   * real {@link GroupingEngine#makeCursorBuildSpec} path so the spec matches what production planning produces.
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

  /**
   * Run the projection-matching group-by against a loaded/merged clustered {@link QueryableIndex} via the real
   * {@link QueryableIndexCursorFactory}, asserting the projection was actually selected (the holder is
   * pre-aggregated), and return tenant -> {cnt, sum_x}.
   */
  private static Map<String, long[]> queryProjection(QueryableIndex index)
  {
    final CursorBuildSpec buildSpec = projectionMatchingBuildSpec();

    // The loader must surface the persisted aggregate projection on the clustered segment, and it must be selected for
    // this spec. If this is null the IndexIO loader isn't exposing the projection for clustered segments.
    Assertions.assertNotNull(
        index.getProjection(buildSpec),
        "aggregate projection 'proj' was not selected for the group-by build spec on the loaded clustered segment"
    );

    final QueryableIndexCursorFactory factory = new QueryableIndexCursorFactory(
        index,
        QueryableIndexTimeBoundaryInspector.create(index)
    );
    final Map<String, long[]> byTenant = new LinkedHashMap<>();
    try (CursorHolder holder = factory.makeCursorHolder(buildSpec)) {
      // The chosen holder must be the aggregate-projection cursor serving pre-aggregated rows.
      Assertions.assertTrue(
          holder.isPreAggregated(),
          "expected the projection (pre-aggregated) cursor holder on the loaded clustered segment"
      );
      final Cursor cursor = holder.asCursor();
      final DimensionSelector tenantSel =
          cursor.getColumnSelectorFactory().makeDimensionSelector(DefaultDimensionSpec.of("tenant"));
      final ColumnValueSelector<?> cntSel = cursor.getColumnSelectorFactory().makeColumnValueSelector("cnt");
      final ColumnValueSelector<?> sumSel = cursor.getColumnSelectorFactory().makeColumnValueSelector("sum_x");
      while (!cursor.isDone()) {
        final String tenant = tenantSel.lookupName(tenantSel.getRow().get(0));
        byTenant.put(tenant, new long[]{cntSel.getLong(), sumSel.getLong()});
        cursor.advance();
      }
    }
    return byTenant;
  }

  @Test
  void testPersistAndLoadClusteredSegmentWithProjection()
  {
    // Ingest out of clustering order; two acme rows (x=10, x=5) and one globex row (x=7).
    final QueryableIndex index = buildSegmentWithProjection(
        "persist-projection",
        List.of(
            row(T0 + 2, "globex", "eu-west-1", 7),
            row(T0, "acme", "us-east-1", 10),
            row(T0 + 1, "acme", "us-west-2", 5)
        )
    );

    // Base clustered table is still written: cluster groups present in clustering order.
    final ClusteredValueGroupsBaseTableSchema summary = index.getClusteredBaseSummary();
    Assertions.assertNotNull(summary, "clustered base summary must still be present alongside the projection");
    Assertions.assertEquals(
        List.of("acme", "globex"),
        summary.getClusteringDictionaries().getStringDictionary()
    );
    final List<TableClusterGroupSpec> groups = summary.getClusterGroups();
    Assertions.assertEquals(2, groups.size());
    Assertions.assertEquals(List.of(0), groups.get(0).getClusteringValueIds());
    Assertions.assertEquals(2, groups.get(0).getNumRows());
    Assertions.assertEquals(List.of(1), groups.get(1).getClusteringValueIds());
    Assertions.assertEquals(1, groups.get(1).getNumRows());

    // The persisted projection must be surfaced by the loader, selected for the projection query, and aggregate
    // correctly: acme -> cnt=2,sum_x=15 ; globex -> cnt=1,sum_x=7.
    final Map<String, long[]> projected = queryProjection(index);
    Assertions.assertEquals(2, projected.size(), "expected one pre-aggregated row per tenant");
    Assertions.assertArrayEquals(new long[]{2L, 15L}, projected.get("acme"));
    Assertions.assertArrayEquals(new long[]{1L, 7L}, projected.get("globex"));

    // Base cluster-group querying remains correct alongside the projection (FULL_SCAN walks groups in clustering
    // order, constants injected).
    Assertions.assertEquals(
        List.of(
            List.of("acme", "us-east-1"),
            List.of("acme", "us-west-2"),
            List.of("globex", "eu-west-1")
        ),
        scanTenantRegion(index)
    );
  }

  @Test
  void testMergeClusteredSegmentsWithProjectionCombinesAggregates() throws Exception
  {
    // Segment 1: acme (x=10, x=5), globex (x=7). Segment 2: globex (x=3), initech (x=4). The merged projection must
    // combine the globex aggregates across both segments.
    final QueryableIndex segment1 = buildSegmentWithProjection(
        "merge-projection-input-1",
        List.of(
            row(T0, "acme", "us-east-1", 10),
            row(T0 + 1, "acme", "us-west-2", 5),
            row(T0 + 2, "globex", "eu-west-1", 7)
        )
    );
    final QueryableIndex segment2 = buildSegmentWithProjection(
        "merge-projection-input-2",
        List.of(
            row(T0 + 3, "globex", "eu-central-1", 3),
            row(T0 + 4, "initech", "us-east-1", 4)
        )
    );

    final IndexBuilder builderForMerger = IndexBuilder.create().useV10();
    final IndexIO indexIO = builderForMerger.getIndexIO();
    final IndexMergerV10 merger = new IndexMergerV10(
        TestHelper.makeJsonMapper(),
        indexIO,
        OffHeapMemorySegmentWriteOutMediumFactory.instance()
    );

    final File mergedDir = new File(tempDir, "merged-projection");
    merger.mergeQueryableIndex(
        List.of(segment1, segment2),
        false,
        new AggregatorFactory[0],
        mergedDir,
        IndexSpec.getDefault(),
        OffHeapMemorySegmentWriteOutMediumFactory.instance(),
        -1
    );
    final QueryableIndex mergedIndex = indexIO.loadIndex(mergedDir);

    // Base table still merges correctly: dictionary [acme, globex, initech] and 3 groups.
    final ClusteredValueGroupsBaseTableSchema summary = mergedIndex.getClusteredBaseSummary();
    Assertions.assertNotNull(summary);
    Assertions.assertEquals(
        List.of("acme", "globex", "initech"),
        summary.getClusteringDictionaries().getStringDictionary()
    );
    final List<TableClusterGroupSpec> groups = summary.getClusterGroups();
    Assertions.assertEquals(3, groups.size());
    Assertions.assertEquals(List.of(0), groups.get(0).getClusteringValueIds());
    Assertions.assertEquals(2, groups.get(0).getNumRows());   // acme: both rows from segment 1
    Assertions.assertEquals(List.of(1), groups.get(1).getClusteringValueIds());
    Assertions.assertEquals(2, groups.get(1).getNumRows());   // globex: one row from each segment
    Assertions.assertEquals(List.of(2), groups.get(2).getClusteringValueIds());
    Assertions.assertEquals(1, groups.get(2).getNumRows());   // initech: from segment 2
    Assertions.assertEquals(5, mergedIndex.getNumRows());

    // The merged projection must combine aggregates across segments: acme -> cnt=2,sum_x=15 ;
    // globex -> cnt=2,sum_x=10 (7 from seg1 + 3 from seg2) ; initech -> cnt=1,sum_x=4.
    final Map<String, long[]> projected = queryProjection(mergedIndex);
    Assertions.assertEquals(3, projected.size(), "expected one pre-aggregated row per tenant after merge");
    Assertions.assertArrayEquals(new long[]{2L, 15L}, projected.get("acme"));
    Assertions.assertArrayEquals(new long[]{2L, 10L}, projected.get("globex"));
    Assertions.assertArrayEquals(new long[]{1L, 4L}, projected.get("initech"));

    // Base cluster-group querying remains correct: all 5 base rows in clustering order (region-sorted within group).
    Assertions.assertEquals(
        List.of(
            List.of("acme", "us-east-1"),
            List.of("acme", "us-west-2"),
            List.of("globex", "eu-central-1"),
            List.of("globex", "eu-west-1"),
            List.of("initech", "us-east-1")
        ),
        scanTenantRegion(mergedIndex)
    );
  }

  @Test
  void testPersistAndLoadClusteredSegment()
  {
    // Ingest tenants out of clustering order to prove the writer sorts groups by clustering value.
    final QueryableIndex index = buildSegment(
        "persist",
        List.of(
            row(T0 + 2, "globex", "eu-west-1"),
            row(T0, "acme", "us-east-1"),
            row(T0 + 1, "acme", "us-west-2")
        )
    );

    final ClusteredValueGroupsBaseTableSchema summary = index.getClusteredBaseSummary();
    Assertions.assertNotNull(summary);
    Assertions.assertEquals(List.of("acme", "globex"), summary.getClusteringDictionaries().getStringDictionary());

    final List<TableClusterGroupSpec> groups = summary.getClusterGroups();
    Assertions.assertEquals(2, groups.size());
    // Groups are written in clustering (dictionary id) order: acme then globex.
    Assertions.assertEquals(List.of(0), groups.get(0).getClusteringValueIds());
    Assertions.assertEquals(2, groups.get(0).getNumRows());
    Assertions.assertEquals(List.of(1), groups.get(1).getClusteringValueIds());
    Assertions.assertEquals(1, groups.get(1).getNumRows());
    Assertions.assertEquals(3, index.getNumRows());

    // Per-group QueryableIndex (merge/persist view, withClusteringColumns=false): region physically present,
    // clustering column not.
    final QueryableIndex acmeGroup = index.getClusterGroupQueryableIndex(groups.get(0), false);
    Assertions.assertNotNull(acmeGroup);
    Assertions.assertEquals(2, acmeGroup.getNumRows());
    Assertions.assertNotNull(acmeGroup.getColumnHolder("region"));
    Assertions.assertNull(acmeGroup.getColumnHolder("tenant"));
    // Query view (withClusteringColumns=true): the clustering column is fabricated as a constant column.
    final QueryableIndex acmeGroupForQuery = index.getClusterGroupQueryableIndex(groups.get(0), true);
    Assertions.assertNotNull(acmeGroupForQuery.getColumnHolder("tenant"));

    // Full scan through the real clustered cursor path: groups in clustering order, constants injected.
    Assertions.assertEquals(
        List.of(
            List.of("acme", "us-east-1"),
            List.of("acme", "us-west-2"),
            List.of("globex", "eu-west-1")
        ),
        scanTenantRegion(index)
    );

    // This is exactly what the appenderator does at publish time to populate DataSegment.clusterGroups: load the
    // merged segment, read its clustered summary, and project it to the broker-facing tuples.
    final ClusterGroupTuples tuples = summary.toClusterGroupTuples();
    Assertions.assertEquals(summary.getClusteringColumns(), tuples.clusteringColumns());
    Assertions.assertEquals(
        List.of(
            Collections.singletonList("acme"),
            Collections.singletonList("globex")
        ),
        tuples.tuples()
    );
  }

  @Test
  void testTimeOrderedMergeAcrossGroups()
  {
    // Two tenants (=> two cluster groups) with interleaved timestamps, ingested out of order.
    final QueryableIndex index = buildTimeOrderedSegment(
        "time-merge",
        List.of(
            row(T0 + 4, "acme", "a4"),
            row(T0 + 1, "acme", "a1"),
            row(T0 + 3, "globex", "g3"),
            row(T0 + 2, "globex", "g2")
        )
    );

    // Layout precondition: __time is the first non-clustering column, so each group is individually __time-sorted.
    final ClusteredValueGroupsBaseTableSchema summary = index.getClusteredBaseSummary();
    Assertions.assertEquals(
        ColumnHolder.TIME_COLUMN_NAME,
        summary.getGroupOrdering().get(0).getColumnName()
    );

    final QueryableIndexCursorFactory factory = new QueryableIndexCursorFactory(
        index,
        QueryableIndexTimeBoundaryInspector.create(index)
    );

    // No preferred ordering => concatenation: clustering-first ordering, NOT globally __time-ordered (each group's
    // rows are contiguous: acme's two rows, then globex's two rows).
    try (CursorHolder holder = factory.makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
      Assertions.assertEquals("tenant", holder.getOrdering().get(0).getColumnName());
      Assertions.assertEquals(
          List.of(
              Arrays.asList(T0 + 1, "a1"),
              Arrays.asList(T0 + 4, "a4"),
              Arrays.asList(T0 + 2, "g2"),
              Arrays.asList(T0 + 3, "g3")
          ),
          scanTimeRegion(holder)
      );
    }

    // Ascending __time preferred => the time-ordered merge cursor: holder advertises __time ASC and rows are globally
    // time-ordered across groups.
    final CursorBuildSpec ascending = CursorBuildSpec.builder(CursorBuildSpec.FULL_SCAN)
        .setPreferredOrdering(List.of(OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME)))
        .build();
    try (CursorHolder holder = factory.makeCursorHolder(ascending)) {
      Assertions.assertEquals(Cursors.ascendingTimeOrder(), holder.getOrdering());
      Assertions.assertEquals(Order.ASCENDING, holder.getTimeOrder());
      Assertions.assertFalse(holder.canVectorize(), "scalar-first: the merge holder is not vectorizable");
      Assertions.assertEquals(
          List.of(
              Arrays.asList(T0 + 1, "a1"),
              Arrays.asList(T0 + 2, "g2"),
              Arrays.asList(T0 + 3, "g3"),
              Arrays.asList(T0 + 4, "a4")
          ),
          scanTimeRegion(holder)
      );
    }

    // Descending __time preferred => descending merge (each per-group cursor reverses, merged with a max-heap).
    final CursorBuildSpec descending = CursorBuildSpec.builder(CursorBuildSpec.FULL_SCAN)
        .setPreferredOrdering(List.of(OrderBy.descending(ColumnHolder.TIME_COLUMN_NAME)))
        .build();
    try (CursorHolder holder = factory.makeCursorHolder(descending)) {
      Assertions.assertEquals(Cursors.descendingTimeOrder(), holder.getOrdering());
      Assertions.assertEquals(Order.DESCENDING, holder.getTimeOrder());
      Assertions.assertEquals(
          List.of(
              Arrays.asList(T0 + 4, "a4"),
              Arrays.asList(T0 + 3, "g3"),
              Arrays.asList(T0 + 2, "g2"),
              Arrays.asList(T0 + 1, "a1")
          ),
          scanTimeRegion(holder)
      );
    }
  }

  @Test
  void testMergeClusteredSegmentsAlignsGroupsAcrossSegments() throws Exception
  {
    // Segment 1: acme (2 rows), globex (1 row). Segment 2: globex (1 row), initech (1 row). The merged segment
    // must re-key both inputs into the merged dictionary [acme, globex, initech] and combine the globex groups.
    final QueryableIndex segment1 = buildSegment(
        "merge-input-1",
        List.of(
            row(T0, "acme", "us-east-1"),
            row(T0 + 1, "acme", "us-west-2"),
            row(T0 + 2, "globex", "eu-west-1")
        )
    );
    final QueryableIndex segment2 = buildSegment(
        "merge-input-2",
        List.of(
            row(T0 + 3, "globex", "eu-central-1"),
            row(T0 + 4, "initech", "us-east-1")
        )
    );

    final IndexBuilder builderForMerger = IndexBuilder.create().useV10();
    final IndexIO indexIO = builderForMerger.getIndexIO();
    final IndexMergerV10 merger = new IndexMergerV10(
        TestHelper.makeJsonMapper(),
        indexIO,
        OffHeapMemorySegmentWriteOutMediumFactory.instance()
    );

    final File mergedDir = new File(tempDir, "merged");
    merger.mergeQueryableIndex(
        List.of(segment1, segment2),
        false,
        new AggregatorFactory[0],
        mergedDir,
        IndexSpec.getDefault(),
        OffHeapMemorySegmentWriteOutMediumFactory.instance(),
        -1
    );
    final QueryableIndex mergedIndex = indexIO.loadIndex(mergedDir);

    final ClusteredValueGroupsBaseTableSchema summary = mergedIndex.getClusteredBaseSummary();
    Assertions.assertNotNull(summary);
    Assertions.assertEquals(
        List.of("acme", "globex", "initech"),
        summary.getClusteringDictionaries().getStringDictionary()
    );

    final List<TableClusterGroupSpec> groups = summary.getClusterGroups();
    Assertions.assertEquals(3, groups.size());
    Assertions.assertEquals(List.of(0), groups.get(0).getClusteringValueIds());
    Assertions.assertEquals(2, groups.get(0).getNumRows());   // acme: both rows from segment 1
    Assertions.assertEquals(List.of(1), groups.get(1).getClusteringValueIds());
    Assertions.assertEquals(2, groups.get(1).getNumRows());   // globex: one row from each segment
    Assertions.assertEquals(List.of(2), groups.get(2).getClusteringValueIds());
    Assertions.assertEquals(1, groups.get(2).getNumRows());   // initech: from segment 2
    Assertions.assertEquals(5, mergedIndex.getNumRows());

    // region is now an explicit ordering column (dimensions = [tenant, region, __time]), so within the globex group
    // rows are region-sorted ascending (eu-central-1 before eu-west-1), not left in time/insertion order.
    Assertions.assertEquals(
        List.of(
            List.of("acme", "us-east-1"),
            List.of("acme", "us-west-2"),
            List.of("globex", "eu-central-1"),
            List.of("globex", "eu-west-1"),
            List.of("initech", "us-east-1")
        ),
        scanTenantRegion(mergedIndex)
    );
  }

  @Test
  void testMergeClusteredWithNonClusteredRejected()
  {
    final QueryableIndex clustered = buildSegment("reject-clustered", List.of(row(T0, "acme", "us-east-1")));
    final QueryableIndex regular = IndexBuilder.create()
                                               .useV10()
                                               .tmpDir(new File(tempDir, "reject-regular"))
                                               .schema(
                                                   IncrementalIndexSchema.builder()
                                                       .withMinTimestamp(T0)
                                                       .withTimestampSpec(TIMESTAMP_SPEC)
                                                       .withQueryGranularity(Granularities.NONE)
                                                       .withDimensionsSpec(tenantClusterSpec().getDimensionsSpec())
                                                       .withMetrics(new CountAggregatorFactory("count"))
                                                       .withRollup(false)
                                                       .build()
                                               )
                                               .rows(List.of(row(T0 + 1, "globex", "eu-west-1")))
                                               .buildMMappedIndex();

    final IndexBuilder builderForMerger = IndexBuilder.create().useV10();
    final IndexMergerV10 merger = new IndexMergerV10(
        TestHelper.makeJsonMapper(),
        builderForMerger.getIndexIO(),
        OffHeapMemorySegmentWriteOutMediumFactory.instance()
    );

    Assertions.assertThrows(
        Exception.class,
        () -> merger.mergeQueryableIndex(
            List.of(clustered, regular),
            false,
            new AggregatorFactory[]{new LongSumAggregatorFactory("count", "count")},
            new File(tempDir, "reject-merged"),
            IndexSpec.getDefault(),
            OffHeapMemorySegmentWriteOutMediumFactory.instance(),
            -1
        )
    );
  }

}
