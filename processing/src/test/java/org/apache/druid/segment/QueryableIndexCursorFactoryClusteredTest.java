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
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.Druids;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.ColumnIndexSelector;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.Filter;
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

  private QueryableIndex standardTwoGroup()
  {
    return buildSegment(List.of(
        row("acme", "2025-01-01T00:00:00", "us-east-1"),
        row("acme", "2025-01-01T01:00:00", "us-west-2"),
        row("globex", "2025-01-01T00:30:00", "eu-west-1")
    ));
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
    // single-group cursor-holder path + SingleGroupClusteringColumnSelectorFactory end-to-end: `region` groups
    // correctly via the group's own (stable) dictionary, and the pruned-out globex group contributes nothing.
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

  private static Druids.TimeseriesQueryBuilder newTimeseries()
  {
    return Druids.newTimeseriesQueryBuilder()
                 .dataSource("test")
                 .granularity(Granularities.ALL)
                 .intervals(List.of(Intervals.ETERNITY))
                 .aggregators(new CountAggregatorFactory("count"));
  }
}
