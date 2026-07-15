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

import org.apache.druid.collections.CloseableDefaultBlockingPool;
import org.apache.druid.collections.CloseableStupidPool;
import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.ClusteredValueGroupsBaseTableProjectionSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.Druids;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.ColumnIndexSelector;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.RangeFilter;
import org.apache.druid.query.filter.TypedInFilter;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByResourcesReservationPool;
import org.apache.druid.query.groupby.GroupByStatsProvider;
import org.apache.druid.query.groupby.GroupingEngine;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryEngine;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.filter.AndFilter;
import org.apache.druid.segment.filter.OrFilter;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.index.BitmapColumnIndex;
import org.apache.druid.segment.vector.VectorCursor;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.joda.time.Interval;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * End-to-end coverage for {@link QueryableIndexCursorFactory}'s clustered dispatch. Builds real clustered base-table
 * segments directly via {@link IndexBuilder} (ingesting flat rows that the writer partitions into cluster groups by
 * clustering value) and runs the full per-group cursor pipeline against the actual on-disk clustered segment data.
 */
class QueryableIndexCursorFactoryClusteredTest
{
  private static final Interval INTERVAL = Intervals.of("2025-01-01/2025-01-02");

  private static final ClusteredValueGroupsBaseTableProjectionSpec CLUSTER_SPEC =
      ClusteredValueGroupsBaseTableProjectionSpec.builder()
          .columns(
              new StringDimensionSchema("tenant"),
              StringDimensionSchema.create("region"),
              new LongDimensionSchema("__time")
          )
          .clusteringColumns("tenant")
          .build();

  private static Closer engineCloser;
  private static GroupingEngine groupingEngine;
  private static TimeseriesQueryEngine timeseriesEngine;
  private static NonBlockingPool<ByteBuffer> nonBlockingPool;

  @BeforeAll
  static void setUpEngines()
  {
    // Some tests build segments with ExpressionVirtualColumn clustering / materialized columns, which require the
    // expression processing module to be initialized.
    ExpressionProcessing.initializeForTests();
    engineCloser = Closer.create();
    nonBlockingPool = engineCloser.register(
        new CloseableStupidPool<>("ClusteredCursorFactoryTest-bufferPool", () -> ByteBuffer.allocate(50000))
    );
    final GroupByResourcesReservationPool resourcesReservationPool = new GroupByResourcesReservationPool(
        engineCloser.register(new CloseableDefaultBlockingPool<>(() -> ByteBuffer.allocate(50000), 2)),
        new GroupByQueryConfig()
    );
    groupingEngine = new GroupingEngine(
        new DruidProcessingConfig(),
        GroupByQueryConfig::new,
        resourcesReservationPool,
        TestHelper.makeJsonMapper(),
        TestHelper.makeSmileMapper(),
        (query, future) -> {},
        new GroupByStatsProvider()
    );
    timeseriesEngine = new TimeseriesQueryEngine(nonBlockingPool);
  }

  @AfterAll
  static void tearDownEngines() throws Exception
  {
    engineCloser.close();
  }

  @TempDir
  public File tmpDir;

  private QueryableIndex segmentIndex;

  @AfterEach
  void tearDown() throws java.io.IOException
  {
    if (segmentIndex != null) {
      segmentIndex.close();
    }
  }

  /**
   * Cluster spec for a segment clustered on {@code tenant_lower := lower(tenant)} (a clustering column produced by a
   * group VC; raw {@code tenant} is NOT a stored column) plus a non-clustering materialized
   * {@code region_upper := upper(region)} column. Columns: {@code [tenant_lower (clustering), region, region_upper,
   * __time]}.
   */
  private static final ClusteredValueGroupsBaseTableProjectionSpec VIRTUAL_CLUSTER_SPEC =
      ClusteredValueGroupsBaseTableProjectionSpec.builder()
          .virtualColumns(VirtualColumns.create(
              new ExpressionVirtualColumn("tenant_lower", "lower(tenant)", ColumnType.STRING, TestExprMacroTable.INSTANCE),
              new ExpressionVirtualColumn("region_upper", "upper(region)", ColumnType.STRING, TestExprMacroTable.INSTANCE)
          ))
          .columns(
              new StringDimensionSchema("tenant_lower"),
              StringDimensionSchema.create("region"),
              StringDimensionSchema.create("region_upper"),
              new LongDimensionSchema("__time")
          )
          .clusteringColumns("tenant_lower")
          .build();

  @Test
  void testQueryVcEquivalentToClusteringColumnReadsMaterializedColumnViaAsCursor()
  {
    // Raw `tenant` is NOT stored; query VC v0 := lower(tenant) is equivalent to the clustering column tenant_lower.
    // The scalar selector remap must substitute the materialized tenant_lower clustering constant — recompute would
    // be null. Read via asCursor() (non-vector) to exercise the scalar ClusteringColumnSelectorFactory path.
    segmentIndex = buildVirtualClusteringSegment();
    final QueryableIndexCursorFactory factory = new QueryableIndexCursorFactory(
        segmentIndex,
        QueryableIndexTimeBoundaryInspector.create(segmentIndex)
    );
    final CursorBuildSpec buildSpec = CursorBuildSpec.builder()
        .setVirtualColumns(VirtualColumns.create(
            new ExpressionVirtualColumn("v0", "lower(tenant)", ColumnType.STRING, TestExprMacroTable.INSTANCE)
        ))
        .build();
    try (CursorHolder holder = factory.makeCursorHolder(buildSpec)) {
      Assertions.assertEquals(List.of("acme", "acme", "globex"), collectDimension(holder.asCursor(), "v0"));
    }
  }

  @Test
  void testQueryVcEquivalentToNonClusteringMaterializedColumnReadsMaterializedColumnViaAsCursor()
  {
    // Query VC v1 := upper(region) is equivalent to the non-clustering materialized column region_upper. The remap
    // makes makeDimensionSelector("v1") read the per-group physical region_upper column.
    segmentIndex = buildVirtualClusteringSegment();
    final QueryableIndexCursorFactory factory = new QueryableIndexCursorFactory(
        segmentIndex,
        QueryableIndexTimeBoundaryInspector.create(segmentIndex)
    );
    final CursorBuildSpec buildSpec = CursorBuildSpec.builder()
        .setVirtualColumns(VirtualColumns.create(
            new ExpressionVirtualColumn("v1", "upper(region)", ColumnType.STRING, TestExprMacroTable.INSTANCE)
        ))
        .build();
    try (CursorHolder holder = factory.makeCursorHolder(buildSpec)) {
      Assertions.assertEquals(
          List.of("US-EAST-1", "US-WEST-2", "EU-WEST-1"),
          collectDimension(holder.asCursor(), "v1")
      );
    }
  }

  @Test
  void testQueryVcWithNoEquivalentStillRecomputesViaAsCursor()
  {
    // No-regression: query VC with no materialized equivalent (lower(region_upper)) is recomputed from the stored
    // region_upper column, not remapped.
    segmentIndex = buildVirtualClusteringSegment();
    final QueryableIndexCursorFactory factory = new QueryableIndexCursorFactory(
        segmentIndex,
        QueryableIndexTimeBoundaryInspector.create(segmentIndex)
    );
    final CursorBuildSpec buildSpec = CursorBuildSpec.builder()
        .setVirtualColumns(VirtualColumns.create(
            new ExpressionVirtualColumn("v2", "lower(region_upper)", ColumnType.STRING, TestExprMacroTable.INSTANCE)
        ))
        .build();
    try (CursorHolder holder = factory.makeCursorHolder(buildSpec)) {
      Assertions.assertEquals(
          List.of("us-east-1", "us-west-2", "eu-west-1"),
          collectDimension(holder.asCursor(), "v2")
      );
    }
  }

  @Test
  void testQueryVcEquivalentToClusteringColumnSingleGroupReadsMaterializedColumn()
  {
    // Single surviving group (filter on the equivalent VC prunes to one group), exercising the single-group cursor
    // holder path's scalar remap wrap. Raw `tenant` is not stored, so the materialized clustering constant is the
    // only correct source.
    segmentIndex = buildVirtualClusteringSegment();
    final QueryableIndexCursorFactory factory = new QueryableIndexCursorFactory(
        segmentIndex,
        QueryableIndexTimeBoundaryInspector.create(segmentIndex)
    );
    final CursorBuildSpec buildSpec = CursorBuildSpec.builder()
        .setVirtualColumns(VirtualColumns.create(
            new ExpressionVirtualColumn("v0", "lower(tenant)", ColumnType.STRING, TestExprMacroTable.INSTANCE)
        ))
        .setFilter(new EqualityFilter("v0", ColumnType.STRING, "globex", null))
        .build();
    try (CursorHolder holder = factory.makeCursorHolder(buildSpec)) {
      Assertions.assertEquals(List.of("globex"), collectDimension(holder.asCursor(), "v0"));
    }
  }

  @Test
  void testEquivalentVcFilterPrunesToSingleGroupNotConcatenated()
  {
    // A filter on the ORIGINAL expression used to build the clustering column (lower(tenant) = 'acme', planned as a
    // query VC) must prune to the single matching cluster group and take the single-group cursor path -- NOT survive
    // every group and post-filter through the ConcatenatingCursor.
    segmentIndex = buildVirtualClusteringSegment();
    final QueryableIndexCursorFactory factory = new QueryableIndexCursorFactory(
        segmentIndex,
        QueryableIndexTimeBoundaryInspector.create(segmentIndex)
    );
    final VirtualColumns queryVcs = VirtualColumns.create(
        new ExpressionVirtualColumn("v0", "lower(tenant)", ColumnType.STRING, TestExprMacroTable.INSTANCE)
    );

    // Control: unfiltered -> both groups survive -> multi-group ConcatenatingCursor.
    try (CursorHolder holder =
             factory.makeCursorHolder(CursorBuildSpec.builder().setVirtualColumns(queryVcs).build())) {
      Assertions.assertInstanceOf(ConcatenatingCursor.class, holder.asCursor());
    }

    // Filtered on the equivalent VC -> the clustering leaf resolves to tenant_lower and folds, pruning to the single
    // acme group -> single-group cursor (not a ConcatenatingCursor), and no residual filter on the missing raw column.
    final CursorBuildSpec buildSpec = CursorBuildSpec.builder()
        .setVirtualColumns(queryVcs)
        .setFilter(new EqualityFilter("v0", ColumnType.STRING, "acme", null))
        .build();
    try (CursorHolder holder = factory.makeCursorHolder(buildSpec)) {
      final Cursor cursor = holder.asCursor();
      Assertions.assertFalse(cursor instanceof ConcatenatingCursor);
      Assertions.assertEquals(List.of("us-east-1", "us-west-2"), collectDimension(cursor, "region"));
    }
  }

  @Test
  void testResidualPhysicalFilterPreservedAlongsideRemappedClusteringVc()
  {
    // A remapped clustering-VC leaf (v0 := lower(tenant) folds against the group tuple) combined with a residual
    // predicate on a non-remapped physical column (region). Once the clustering leaf folds to TRUE, the per-group
    // filter is region = 'us-east-1', which references no remap key -- the remap rewrite must preserve it (identity
    // fallback) rather than throw for a column missing from {v0 -> tenant_lower}.
    segmentIndex = buildVirtualClusteringSegment();
    final QueryableIndexCursorFactory factory = new QueryableIndexCursorFactory(
        segmentIndex,
        QueryableIndexTimeBoundaryInspector.create(segmentIndex)
    );
    final CursorBuildSpec buildSpec = CursorBuildSpec.builder()
        .setVirtualColumns(VirtualColumns.create(
            new ExpressionVirtualColumn("v0", "lower(tenant)", ColumnType.STRING, TestExprMacroTable.INSTANCE)
        ))
        .setFilter(new AndFilter(List.of(
            new EqualityFilter("v0", ColumnType.STRING, "acme", null),
            new EqualityFilter("region", ColumnType.STRING, "us-east-1", null)
        )))
        .build();
    try (CursorHolder holder = factory.makeCursorHolder(buildSpec)) {
      // Only the Acme / us-east-1 row survives; v0 still resolves to the materialized tenant_lower clustering constant.
      Assertions.assertEquals(List.of("acme"), collectDimension(holder.asCursor(), "v0"));
    }
    try (CursorHolder holder = factory.makeCursorHolder(buildSpec)) {
      Assertions.assertEquals(List.of("us-east-1"), collectDimension(holder.asCursor(), "region"));
    }
  }

  @Test
  void testResidualPhysicalFilterPreservedAlongsideRemappedNonClusteringVc()
  {
    // Reviewer's case: a matched non-clustering VC filter (v1 := upper(region), equivalent to region_upper) AND a
    // residual physical predicate (region = 'us-east-1'). No clustering leaf folds, so the per-group filter stays an
    // AndFilter whose recursion hits the residual region leaf; the identity fallback must keep region as-is while
    // remapping v1 -> region_upper.
    segmentIndex = buildVirtualClusteringSegment();
    final QueryableIndexCursorFactory factory = new QueryableIndexCursorFactory(
        segmentIndex,
        QueryableIndexTimeBoundaryInspector.create(segmentIndex)
    );
    final CursorBuildSpec buildSpec = CursorBuildSpec.builder()
        .setVirtualColumns(VirtualColumns.create(
            new ExpressionVirtualColumn("v1", "upper(region)", ColumnType.STRING, TestExprMacroTable.INSTANCE)
        ))
        .setFilter(new AndFilter(List.of(
            new EqualityFilter("v1", ColumnType.STRING, "US-EAST-1", null),
            new EqualityFilter("region", ColumnType.STRING, "us-east-1", null)
        )))
        .build();
    try (CursorHolder holder = factory.makeCursorHolder(buildSpec)) {
      Assertions.assertEquals(List.of("us-east-1"), collectDimension(holder.asCursor(), "region"));
    }
  }

  @Test
  void testQueryVcEquivalentToClusteringColumnReadsMaterializedColumnViaVectorCursor()
  {
    // the query-VC -> materialized-column remap is applied on the vector factory too, so a
    // vectorized read of v0 := lower(tenant) resolves to the materialized clustering column tenant_lower (raw tenant
    // not stored -> recompute would be null), and the remap no longer forces the scalar path.
    segmentIndex = buildVirtualClusteringSegment();
    final QueryableIndexCursorFactory factory = new QueryableIndexCursorFactory(
        segmentIndex,
        QueryableIndexTimeBoundaryInspector.create(segmentIndex)
    );
    final CursorBuildSpec buildSpec = CursorBuildSpec.builder()
        .setVirtualColumns(VirtualColumns.create(
            new ExpressionVirtualColumn("v0", "lower(tenant)", ColumnType.STRING, TestExprMacroTable.INSTANCE)
        ))
        .build();
    try (CursorHolder holder = factory.makeCursorHolder(buildSpec)) {
      Assertions.assertTrue(holder.canVectorize());
      Assertions.assertEquals(List.of("acme", "acme", "globex"), collectObjectVector(holder.asVectorCursor(), "v0"));
    }
  }

  @Test
  void testQueryVcEquivalentToNonClusteringMaterializedColumnReadsMaterializedColumnViaVectorCursor()
  {
    // non-clustering: v1 := upper(region) resolves to the materialized region_upper physical column.
    segmentIndex = buildVirtualClusteringSegment();
    final QueryableIndexCursorFactory factory = new QueryableIndexCursorFactory(
        segmentIndex,
        QueryableIndexTimeBoundaryInspector.create(segmentIndex)
    );
    final CursorBuildSpec buildSpec = CursorBuildSpec.builder()
        .setVirtualColumns(VirtualColumns.create(
            new ExpressionVirtualColumn("v1", "upper(region)", ColumnType.STRING, TestExprMacroTable.INSTANCE)
        ))
        .build();
    try (CursorHolder holder = factory.makeCursorHolder(buildSpec)) {
      Assertions.assertTrue(holder.canVectorize());
      Assertions.assertEquals(
          List.of("US-EAST-1", "US-WEST-2", "EU-WEST-1"),
          collectObjectVector(holder.asVectorCursor(), "v1")
      );
    }
  }

  @Test
  void testQueryVcEquivalentToClusteringColumnSingleGroupVectorCursor()
  {
    // single surviving group (filter on the equivalent VC prunes to one group): exercises the single-group holder's
    // vector remap wrap.
    segmentIndex = buildVirtualClusteringSegment();
    final QueryableIndexCursorFactory factory = new QueryableIndexCursorFactory(
        segmentIndex,
        QueryableIndexTimeBoundaryInspector.create(segmentIndex)
    );
    final CursorBuildSpec buildSpec = CursorBuildSpec.builder()
        .setVirtualColumns(VirtualColumns.create(
            new ExpressionVirtualColumn("v0", "lower(tenant)", ColumnType.STRING, TestExprMacroTable.INSTANCE)
        ))
        .setFilter(new EqualityFilter("v0", ColumnType.STRING, "globex", null))
        .build();
    try (CursorHolder holder = factory.makeCursorHolder(buildSpec)) {
      Assertions.assertTrue(holder.canVectorize());
      Assertions.assertEquals(List.of("globex"), collectObjectVector(holder.asVectorCursor(), "v0"));
    }
  }

  @Test
  void testGetRowSignatureCombinesClusteringFromSummaryAndDataFromGroup()
  {
    segmentIndex = standardTwoGroup();
    final QueryableIndexCursorFactory factory = new QueryableIndexCursorFactory(
        segmentIndex,
        QueryableIndexTimeBoundaryInspector.create(segmentIndex)
    );

    final RowSignature sig = factory.getRowSignature();
    Assertions.assertEquals(ColumnType.STRING, sig.getColumnType("tenant").orElseThrow());
    Assertions.assertEquals(ColumnType.LONG, sig.getColumnType(ColumnHolder.TIME_COLUMN_NAME).orElseThrow());
    Assertions.assertEquals(ColumnType.STRING, sig.getColumnType("region").orElseThrow());
  }

  @Test
  void testGetColumnCapabilitiesForClusteringColumnFromSummary()
  {
    segmentIndex = standardTwoGroup();
    final QueryableIndexCursorFactory factory = new QueryableIndexCursorFactory(
        segmentIndex,
        QueryableIndexTimeBoundaryInspector.create(segmentIndex)
    );

    final ColumnCapabilities tenantCaps = factory.getColumnCapabilities("tenant");
    Assertions.assertNotNull(tenantCaps);
    Assertions.assertTrue(tenantCaps.is(ValueType.STRING));
  }

  @Test
  void testGetColumnCapabilitiesForDataColumnFromFirstGroup()
  {
    segmentIndex = standardTwoGroup();
    final QueryableIndexCursorFactory factory = new QueryableIndexCursorFactory(
        segmentIndex,
        QueryableIndexTimeBoundaryInspector.create(segmentIndex)
    );

    final ColumnCapabilities regionCaps = factory.getColumnCapabilities("region");
    Assertions.assertNotNull(regionCaps);
    Assertions.assertTrue(regionCaps.is(ValueType.STRING));
  }

  @Test
  void testGetColumnCapabilitiesForUnknownColumnIsNull()
  {
    segmentIndex = standardTwoGroup();
    final QueryableIndexCursorFactory factory = new QueryableIndexCursorFactory(
        segmentIndex,
        QueryableIndexTimeBoundaryInspector.create(segmentIndex)
    );

    Assertions.assertNull(factory.getColumnCapabilities("nope"));
  }

  @Test
  void testUnfilteredScanWalksAllGroupsAndInjectsClusteringConstants()
  {
    segmentIndex = standardTwoGroup();
    final QueryableIndexCursorFactory factory = new QueryableIndexCursorFactory(
        segmentIndex,
        QueryableIndexTimeBoundaryInspector.create(segmentIndex)
    );

    try (CursorHolder holder = factory.makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
      final List<List<String>> rows = collectTenantRegionRows(holder.asCursor());
      // acme group rows come first (clustering-ascending order), then globex. Tenants injected per-group.
      Assertions.assertEquals(
          List.of(
              List.of("acme", "us-east-1"),
              List.of("acme", "us-west-2"),
              List.of("globex", "eu-west-1")
          ),
          rows
      );
    }
  }

  @Test
  void testFilterOnClusteringColumnPrunesNonMatchingGroups()
  {
    segmentIndex = standardTwoGroup();
    final QueryableIndexCursorFactory factory = new QueryableIndexCursorFactory(
        segmentIndex,
        QueryableIndexTimeBoundaryInspector.create(segmentIndex)
    );

    // tenant=acme — only the acme group survives the pruner. Its filter is rewritten to TRUE and dropped, so the
    // per-group QueryableIndex never sees a leaf referencing "tenant" (which it doesn't physically carry).
    final Filter filter = new EqualityFilter("tenant", ColumnType.STRING, "acme", null);
    try (CursorHolder holder = factory.makeCursorHolder(specWith(filter))) {
      final List<List<String>> rows = collectTenantRegionRows(holder.asCursor());
      Assertions.assertEquals(
          List.of(
              List.of("acme", "us-east-1"),
              List.of("acme", "us-west-2")
          ),
          rows
      );
    }
  }

  @Test
  void testRangeFilterOnClusteringColumnMatchesViaFabricatedColumn()
  {
    segmentIndex = standardTwoGroup();
    final QueryableIndexCursorFactory factory = new QueryableIndexCursorFactory(
        segmentIndex,
        QueryableIndexTimeBoundaryInspector.create(segmentIndex)
    );

    // A RangeFilter on the clustering column is NOT folded by the pruner (only Equality/In/Null fold), so it survives
    // to the per-group cursor. Clustering columns are constant-per-group and not stored on disk, so before the
    // per-group index fabricated them as constant columns this returned nothing; now the range resolves against the
    // fabricated column's value matcher and both groups (tenant in [a, z]) match all rows.
    final Filter filter = new RangeFilter("tenant", ColumnType.STRING, "a", "z", false, false, null);
    try (CursorHolder holder = factory.makeCursorHolder(specWith(filter))) {
      final List<List<String>> rows = collectTenantRegionRows(holder.asCursor());
      Assertions.assertEquals(
          List.of(
              List.of("acme", "us-east-1"),
              List.of("acme", "us-west-2"),
              List.of("globex", "eu-west-1")
          ),
          rows
      );
    }
  }

  @Test
  void testResidualRangeFilterOnClusteringColumnSingleGroup()
  {
    segmentIndex = standardTwoGroup();
    final QueryableIndexCursorFactory factory = new QueryableIndexCursorFactory(
        segmentIndex,
        QueryableIndexTimeBoundaryInspector.create(segmentIndex)
    );

    // tenant = 'acme' folds and prunes to the single acme group; the RangeFilter on the clustering column survives as
    // a residual leaf on the single-group cursor. It must match against the fabricated constant column ('acme' is in
    // [a, m]) rather than an absent physical column.
    final Filter filter = new AndFilter(List.of(
        new EqualityFilter("tenant", ColumnType.STRING, "acme", null),
        new RangeFilter("tenant", ColumnType.STRING, "a", "m", false, false, null)
    ));
    try (CursorHolder holder = factory.makeCursorHolder(specWith(filter))) {
      final List<List<String>> rows = collectTenantRegionRows(holder.asCursor());
      Assertions.assertEquals(
          List.of(
              List.of("acme", "us-east-1"),
              List.of("acme", "us-west-2")
          ),
          rows
      );
    }
  }

  @Test
  void testReportedOrderingIsTheFullSegmentOrderingRegardlessOfGroupPruning()
  {
    // A clustered segment must advertise the SAME ordering whether a filter prunes to one group or many — otherwise
    // an ORDER BY __time scan would intermittently succeed (single group reporting time-first) or fail (multiple
    // groups reporting clustering-first) based purely on filter selectivity. Both must report the full segment
    // ordering [tenant ASC, region ASC, __time ASC], which is clustering-first (not time-first).
    segmentIndex = standardTwoGroup();
    final QueryableIndexCursorFactory factory = new QueryableIndexCursorFactory(
        segmentIndex,
        QueryableIndexTimeBoundaryInspector.create(segmentIndex)
    );

    final List<OrderBy> expected = segmentIndex.getClusteredBaseSummary().getOrdering();
    Assertions.assertEquals(ColumnHolder.TIME_COLUMN_NAME, expected.get(expected.size() - 1).getColumnName());
    Assertions.assertNotEquals(ColumnHolder.TIME_COLUMN_NAME, expected.get(0).getColumnName());

    // Single surviving group (tenant=acme).
    try (CursorHolder single = factory.makeCursorHolder(
        specWith(new EqualityFilter("tenant", ColumnType.STRING, "acme", null))
    )) {
      Assertions.assertEquals(expected, single.getOrdering());
    }
    // Multiple surviving groups (full scan).
    try (CursorHolder multi = factory.makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
      Assertions.assertEquals(expected, multi.getOrdering());
    }
  }

  @Test
  void testFilterOnNonClusteringColumnRunsBitmapOnEverySurvivingGroup()
  {
    segmentIndex = standardTwoGroup();
    final QueryableIndexCursorFactory factory = new QueryableIndexCursorFactory(
        segmentIndex,
        QueryableIndexTimeBoundaryInspector.create(segmentIndex)
    );

    // region=us-east-1 — pruner keeps both groups (filter references non-clustering data → UNKNOWN); rewriter
    // leaves the leaf untouched; per-group QueryableIndex's bitmap index drives row selection. Only the acme group
    // has a us-east-1 row.
    final Filter filter = new EqualityFilter("region", ColumnType.STRING, "us-east-1", null);
    try (CursorHolder holder = factory.makeCursorHolder(specWith(filter))) {
      final List<List<String>> rows = collectTenantRegionRows(holder.asCursor());
      Assertions.assertEquals(List.of(List.of("acme", "us-east-1")), rows);
    }
  }

  @Test
  void testMixedAndFilterPrunesByClusteringAndBitmapsTheRest()
  {
    segmentIndex = standardTwoGroup();
    final QueryableIndexCursorFactory factory = new QueryableIndexCursorFactory(
        segmentIndex,
        QueryableIndexTimeBoundaryInspector.create(segmentIndex)
    );

    // tenant=acme AND region=us-west-2 — pruner keeps acme only (clustering leaf TRUE on acme, FALSE on globex);
    // rewriter folds the clustering TRUE out of the AND, leaving just `region=us-west-2` for the per-group bitmap.
    final LinkedHashSet<Filter> children = new LinkedHashSet<>();
    children.add(new EqualityFilter("tenant", ColumnType.STRING, "acme", null));
    children.add(new EqualityFilter("region", ColumnType.STRING, "us-west-2", null));
    try (CursorHolder holder = factory.makeCursorHolder(specWith(new AndFilter(children)))) {
      final List<List<String>> rows = collectTenantRegionRows(holder.asCursor());
      Assertions.assertEquals(List.of(List.of("acme", "us-west-2")), rows);
    }
  }

  @Test
  void testAllGroupsPrunedReturnsEmptyCursor()
  {
    segmentIndex = standardTwoGroup();
    final QueryableIndexCursorFactory factory = new QueryableIndexCursorFactory(
        segmentIndex,
        QueryableIndexTimeBoundaryInspector.create(segmentIndex)
    );

    final Filter filter = new EqualityFilter("tenant", ColumnType.STRING, "initech", null);
    try (CursorHolder holder = factory.makeCursorHolder(specWith(filter))) {
      // Empty cluster-group cursor — no surviving group, so the cursor is already done.
      final Cursor cursor = holder.asCursor();
      Assertions.assertTrue(cursor.isDone());
      // Query engines commonly create selectors from a non-null cursor before checking isDone(); the selector
      // factory must hand out harmless nil selectors rather than throwing so the engine sees an empty result.
      final DimensionSelector tenantSel =
          cursor.getColumnSelectorFactory().makeDimensionSelector(DefaultDimensionSpec.of("tenant"));
      Assertions.assertNotNull(tenantSel);
      Assertions.assertNotNull(cursor.getColumnSelectorFactory().makeColumnValueSelector("region"));
      Assertions.assertNull(cursor.getColumnSelectorFactory().getColumnCapabilities("tenant"));
    }
  }

  @Test
  void testMultiGroupCanVectorizeAccountsForEveryGroupRewrite()
  {
    segmentIndex = standardTwoGroup();
    final QueryableIndexCursorFactory factory = new QueryableIndexCursorFactory(
        segmentIndex,
        QueryableIndexTimeBoundaryInspector.create(segmentIndex)
    );

    // OR(tenant=acme, nonVectorizable(region=us-east-1)) — both groups survive (UNKNOWN keeps globex), but the
    // rewrites differ: acme folds to TRUE (null filter, trivially vectorizable) while globex keeps the
    // non-vectorizable residual leaf. The combined holder must NOT report canVectorize even though the first
    // group's holder would, otherwise the engine picks the vector path and the later group blows up.
    final LinkedHashSet<Filter> children = new LinkedHashSet<>();
    children.add(new EqualityFilter("tenant", ColumnType.STRING, "acme", null));
    children.add(new NonVectorizableFilter(new EqualityFilter("region", ColumnType.STRING, "us-east-1", null)));
    try (CursorHolder holder = factory.makeCursorHolder(specWith(new OrFilter(children)))) {
      Assertions.assertFalse(holder.canVectorize());
      // The non-vector path still works and only acme rows + the matching globex row would pass the residual; the
      // acme group's TRUE rewrite keeps all of its rows.
      final List<List<String>> rows = collectTenantRegionRows(holder.asCursor());
      Assertions.assertEquals(
          List.of(
              List.of("acme", "us-east-1"),
              List.of("acme", "us-west-2")
          ),
          rows
      );
    }

    // Control: the same shape with a vectorizable residual leaf keeps the vector path available.
    final LinkedHashSet<Filter> vectorizableChildren = new LinkedHashSet<>();
    vectorizableChildren.add(new EqualityFilter("tenant", ColumnType.STRING, "acme", null));
    vectorizableChildren.add(new EqualityFilter("region", ColumnType.STRING, "us-east-1", null));
    try (CursorHolder holder = factory.makeCursorHolder(specWith(new OrFilter(vectorizableChildren)))) {
      Assertions.assertTrue(holder.canVectorize());
    }
  }

  @Test
  void testAllGroupsPrunedTimeseriesReturnsEmptyResult()
  {
    segmentIndex = standardTwoGroup();
    final QueryableIndexCursorFactory factory = new QueryableIndexCursorFactory(
        segmentIndex,
        QueryableIndexTimeBoundaryInspector.create(segmentIndex)
    );

    // End-to-end version of the selector-on-done-cursor contract: a real engine runs a query whose filter prunes
    // every cluster group and must come back with an empty result (for timeseries with ALL granularity, a single
    // zero-count bucket) rather than an error from selector creation on the done cursor.
    final TimeseriesQuery query = newTimeseries()
        .filters(new EqualityFilter("tenant", ColumnType.STRING, "initech", null))
        .build();
    final List<Result<TimeseriesResultValue>> results = timeseriesEngine.process(query, factory, null, null).toList();
    Assertions.assertEquals(1, results.size());
    Assertions.assertEquals(0L, results.get(0).getValue().getLongMetric("count").longValue());
  }

  @Test
  void testAllGroupsPrunedSupportsForceVectorize()
  {
    segmentIndex = standardTwoGroup();
    final QueryableIndexCursorFactory factory = new QueryableIndexCursorFactory(
        segmentIndex,
        QueryableIndexTimeBoundaryInspector.create(segmentIndex)
    );

    // The empty holder must report canVectorize and serve a (done) vector cursor — otherwise "vectorize": "force"
    // queries fail outright when the filter prunes every cluster group, instead of returning an empty result.
    final TimeseriesQuery query = newTimeseries()
        .filters(new EqualityFilter("tenant", ColumnType.STRING, "initech", null))
        .context(Map.of(QueryContexts.VECTORIZE_KEY, "force"))
        .build();
    final List<Result<TimeseriesResultValue>> results = timeseriesEngine.process(query, factory, null, null).toList();
    Assertions.assertTrue(
        results.isEmpty() || results.get(0).getValue().getLongMetric("count").longValue() == 0L,
        () -> "expected empty result, got " + results
    );
  }

  @Test
  void testSingleGroupMatchUsesSingleGroupCursorHolderPath()
  {
    segmentIndex = standardTwoGroup();
    final QueryableIndexCursorFactory factory = new QueryableIndexCursorFactory(
        segmentIndex,
        QueryableIndexTimeBoundaryInspector.create(segmentIndex)
    );

    // tenant IN ('globex') matches a single group → single-group cursor-holder path. Filter rewriter still folds
    // the leaf to TRUE for the surviving group, and the cursor injects tenant=globex via the selector wrapper.
    final Filter filter = new TypedInFilter("tenant", ColumnType.STRING, List.of("globex"), null, null);
    try (CursorHolder holder = factory.makeCursorHolder(specWith(filter))) {
      Assertions.assertEquals(
          List.of(List.of("globex", "eu-west-1")),
          collectTenantRegionRows(holder.asCursor())
      );
    }
  }

  @Test
  void testMultiGroupTypedInFilterKeepsBothGroups()
  {
    segmentIndex = standardTwoGroup();
    final QueryableIndexCursorFactory factory = new QueryableIndexCursorFactory(
        segmentIndex,
        QueryableIndexTimeBoundaryInspector.create(segmentIndex)
    );

    // tenant IN ('acme', 'globex') → both groups survive, full concatenated walk.
    final Filter filter = new TypedInFilter("tenant", ColumnType.STRING, List.of("acme", "globex"), null, null);
    try (CursorHolder holder = factory.makeCursorHolder(specWith(filter))) {
      Assertions.assertEquals(
          List.of(
              List.of("acme", "us-east-1"),
              List.of("acme", "us-west-2"),
              List.of("globex", "eu-west-1")
          ),
          collectTenantRegionRows(holder.asCursor())
      );
    }
  }

  @Test
  void testTimeseriesCountAcrossClusterGroups()
  {
    segmentIndex = standardTwoGroup();
    final QueryableIndexCursorFactory factory = new QueryableIndexCursorFactory(
        segmentIndex,
        QueryableIndexTimeBoundaryInspector.create(segmentIndex)
    );

    // No filter — both groups walked; aggregator must accumulate count across the group transition.
    final TimeseriesQuery query = newTimeseries().build();
    final List<Result<TimeseriesResultValue>> results = timeseriesEngine.process(query, factory, null, null).toList();
    Assertions.assertEquals(1, results.size());
    Assertions.assertEquals(3L, results.get(0).getValue().getLongMetric("count").longValue());
  }

  @Test
  void testTimeseriesCountWithClusteringFilter()
  {
    segmentIndex = standardTwoGroup();
    final QueryableIndexCursorFactory factory = new QueryableIndexCursorFactory(
        segmentIndex,
        QueryableIndexTimeBoundaryInspector.create(segmentIndex)
    );

    // tenant=acme — only the acme group survives pruning; count should be 2.
    final TimeseriesQuery query = newTimeseries()
        .filters(new EqualityFilter("tenant", ColumnType.STRING, "acme", null))
        .build();
    final List<Result<TimeseriesResultValue>> results = timeseriesEngine.process(query, factory, null, null).toList();
    Assertions.assertEquals(1, results.size());
    Assertions.assertEquals(2L, results.get(0).getValue().getLongMetric("count").longValue());
  }

  @Test
  void testGroupByOnClusteringColumnAcrossGroups()
  {
    segmentIndex = standardTwoGroup();
    final QueryableIndexCursorFactory factory = new QueryableIndexCursorFactory(
        segmentIndex,
        QueryableIndexTimeBoundaryInspector.create(segmentIndex)
    );

    // GROUP BY tenant — one bucket per surviving group; exercises the injected constant clustering selector under
    // the grouping engine's selector-holding hot loop.
    final GroupByQuery query = GroupByQuery.builder()
                                           .setDataSource("test")
                                           .setGranularity(Granularities.ALL)
                                           .setInterval(Intervals.ETERNITY)
                                           .addDimension("tenant")
                                           .addOrderByColumn("tenant")
                                           .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                           .build();
    final List<ResultRow> results = groupingEngine.process(query, factory, null, nonBlockingPool, null).toList();
    Assertions.assertEquals(2, results.size());
    // ResultRow layout for a GroupBy with one dim + one agg: [dim0, agg0]
    Assertions.assertEquals("acme", results.get(0).get(0));
    Assertions.assertEquals(2L, ((Number) results.get(0).get(1)).longValue());
    Assertions.assertEquals("globex", results.get(1).get(0));
    Assertions.assertEquals(1L, ((Number) results.get(1).get(1)).longValue());
  }

  @Test
  void testGroupByOnNonClusteringColumnRollsAcrossGroups()
  {
    // Three groups that share a region value, so the GroupBy on `region` must roll matching rows across cluster
    // group transitions into the same bucket — proving the grouping engine's selector identity survives the
    // wrapper-factory's setDelegate transitions and the hash bucket isn't accidentally segmented per group.
    segmentIndex = buildSegment(List.of(
        row("acme", "2025-01-01T00:00:00", "us-east-1"),
        row("acme", "2025-01-01T00:30:00", "us-east-1"),
        row("globex", "2025-01-01T01:00:00", "us-east-1")
    ));
    final QueryableIndexCursorFactory factory = new QueryableIndexCursorFactory(
        segmentIndex,
        QueryableIndexTimeBoundaryInspector.create(segmentIndex)
    );

    final GroupByQuery query = GroupByQuery.builder()
                                           .setDataSource("test")
                                           .setGranularity(Granularities.ALL)
                                           .setInterval(Intervals.ETERNITY)
                                           .addDimension("region")
                                           .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                           .build();
    final List<ResultRow> results = groupingEngine.process(query, factory, null, nonBlockingPool, null).toList();
    Assertions.assertEquals(1, results.size());
    Assertions.assertEquals("us-east-1", results.get(0).get(0));
    Assertions.assertEquals(3L, ((Number) results.get(0).get(1)).longValue());
  }

  @Test
  void testGroupByOnNonClusteringColumnWithinSingleGroup()
  {
    // Filter to a single cluster group (tenant=acme), then GROUP BY a non-clustering column. Exercises the
    // single-group cursor-holder path end-to-end: `region` groups correctly via the group's own dictionary, and the
    // pruned-out globex group contributes nothing.
    segmentIndex = buildSegment(List.of(
        row("acme", "2025-01-01T00:00:00", "us-east-1"),
        row("acme", "2025-01-01T00:10:00", "us-east-1"),
        row("acme", "2025-01-01T01:00:00", "us-west-2"),
        row("globex", "2025-01-01T00:30:00", "eu-west-1")
    ));
    final QueryableIndexCursorFactory factory = new QueryableIndexCursorFactory(
        segmentIndex,
        QueryableIndexTimeBoundaryInspector.create(segmentIndex)
    );

    final GroupByQuery query = GroupByQuery.builder()
                                           .setDataSource("test")
                                           .setGranularity(Granularities.ALL)
                                           .setInterval(Intervals.ETERNITY)
                                           .setDimFilter(new EqualityFilter("tenant", ColumnType.STRING, "acme", null))
                                           .addDimension("region")
                                           .addOrderByColumn("region")
                                           .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                           .build();
    final List<ResultRow> results = groupingEngine.process(query, factory, null, nonBlockingPool, null).toList();
    Assertions.assertEquals(2, results.size());
    Assertions.assertEquals("us-east-1", results.get(0).get(0));
    Assertions.assertEquals(2L, ((Number) results.get(0).get(1)).longValue());
    Assertions.assertEquals("us-west-2", results.get(1).get(0));
    Assertions.assertEquals(1L, ((Number) results.get(1).get(1)).longValue());
  }

  @Test
  void testGroupByClusteringColumnWithinSingleGroup()
  {
    // Filter to a single group, then GROUP BY the clustering column itself. The single-group factory advertises the
    // clustering column as a one-entry dictionary, so this rides the dictionary-id grouping path over the constant
    // clustering selector: one bucket, the constant value, full count.
    segmentIndex = standardTwoGroup();
    final QueryableIndexCursorFactory factory = new QueryableIndexCursorFactory(
        segmentIndex,
        QueryableIndexTimeBoundaryInspector.create(segmentIndex)
    );
    final GroupByQuery query = GroupByQuery.builder()
                                           .setDataSource("test")
                                           .setGranularity(Granularities.ALL)
                                           .setInterval(Intervals.ETERNITY)
                                           .setDimFilter(new EqualityFilter("tenant", ColumnType.STRING, "acme", null))
                                           .addDimension("tenant")
                                           .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                           .build();
    final List<ResultRow> results = groupingEngine.process(query, factory, null, nonBlockingPool, null).toList();
    Assertions.assertEquals(1, results.size());
    Assertions.assertEquals("acme", results.get(0).get(0));
    Assertions.assertEquals(2L, ((Number) results.get(0).get(1)).longValue());
  }

  private static InputRow row(String tenant, String ts, String region)
  {
    return new MapBasedInputRow(
        DateTimes.of(ts),
        List.of("tenant", "region"),
        Map.of("tenant", tenant, "region", region)
    );
  }

  private QueryableIndex buildSegment(List<InputRow> rows)
  {
    final IncrementalIndexSchema schema =
        IncrementalIndexSchema.builder()
                              .withMinTimestamp(INTERVAL.getStartMillis())
                              .withTimestampSpec(new TimestampSpec("__time", "auto", null))
                              .withQueryGranularity(Granularities.NONE)
                              .withDimensionsSpec(CLUSTER_SPEC.getDimensionsSpec())
                              .withRollup(false)
                              .withClusterSpec(CLUSTER_SPEC)
                              .build();
    return IndexBuilder.create()
                       .useV10()
                       .tmpDir(tmpDir)
                       .schema(schema)
                       .rows(rows)
                       .buildMMappedIndex(INTERVAL);
  }

  private QueryableIndex buildVirtualClusteringSegment()
  {
    final IncrementalIndexSchema schema =
        IncrementalIndexSchema.builder()
                              .withMinTimestamp(INTERVAL.getStartMillis())
                              .withTimestampSpec(new TimestampSpec("__time", "auto", null))
                              .withQueryGranularity(Granularities.NONE)
                              .withDimensionsSpec(VIRTUAL_CLUSTER_SPEC.getDimensionsSpec())
                              .withRollup(false)
                              .withClusterSpec(VIRTUAL_CLUSTER_SPEC)
                              .build();
    return IndexBuilder.create()
                       .useV10()
                       .tmpDir(tmpDir)
                       .schema(schema)
                       .rows(List.of(
                           row("Acme", "2025-01-01T00:00:00", "us-east-1"),
                           row("Acme", "2025-01-01T01:00:00", "us-west-2"),
                           row("Globex", "2025-01-01T00:30:00", "eu-west-1")
                       ))
                       .buildMMappedIndex(INTERVAL);
  }

  private QueryableIndex standardTwoGroup()
  {
    return buildSegment(List.of(
        row("acme", "2025-01-01T00:00:00", "us-east-1"),
        row("acme", "2025-01-01T01:00:00", "us-west-2"),
        row("globex", "2025-01-01T00:30:00", "eu-west-1")
    ));
  }

  private static Druids.TimeseriesQueryBuilder newTimeseries()
  {
    return Druids.newTimeseriesQueryBuilder()
                 .dataSource("test")
                 .granularity(Granularities.ALL)
                 .intervals(List.of(Intervals.ETERNITY))
                 .aggregators(new CountAggregatorFactory("count"));
  }

  private static List<String> collectDimension(Cursor cursor, String column)
  {
    final DimensionSelector sel =
        cursor.getColumnSelectorFactory().makeDimensionSelector(DefaultDimensionSpec.of(column));
    final List<String> out = new ArrayList<>();
    while (!cursor.isDone()) {
      out.add(sel.getRow().size() == 0 ? null : sel.lookupName(sel.getRow().get(0)));
      cursor.advance();
    }
    return out;
  }

  private static List<String> collectObjectVector(VectorCursor cursor, String column)
  {
    final VectorObjectSelector sel = cursor.getColumnSelectorFactory().makeObjectSelector(column);
    final List<String> out = new ArrayList<>();
    while (!cursor.isDone()) {
      final Object[] vector = sel.getObjectVector();
      final int size = cursor.getCurrentVectorSize();
      for (int i = 0; i < size; i++) {
        out.add((String) vector[i]);
      }
      cursor.advance();
    }
    return out;
  }

  /**
   * Iterate the (tenant, region) pairs out of {@code cursor} until done. Verifies that the clustering-column
   * selector returns the right per-group constant and that the rewritten filter / bitmap-index path on the
   * per-group QueryableIndex produces the rows it should.
   */
  private static List<List<String>> collectTenantRegionRows(Cursor cursor)
  {
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
    return out;
  }

  private static CursorBuildSpec specWith(Filter filter)
  {
    return CursorBuildSpec.builder().setFilter(filter).build();
  }

  /**
   * Filter wrapper whose value matcher cannot vectorize and which exposes no bitmap index, forcing the matcher
   * path. The cluster-group filter walker doesn't recognize the wrapper, so it passes through rewrites unchanged.
   */
  private static final class NonVectorizableFilter implements Filter
  {
    private final Filter delegate;

    private NonVectorizableFilter(Filter delegate)
    {
      this.delegate = delegate;
    }

    @Override
    public BitmapColumnIndex getBitmapColumnIndex(ColumnIndexSelector selector)
    {
      return null;
    }

    @Override
    public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
    {
      return delegate.makeMatcher(factory);
    }

    @Override
    public Set<String> getRequiredColumns()
    {
      return delegate.getRequiredColumns();
    }
  }
}
