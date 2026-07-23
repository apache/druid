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
import org.apache.druid.data.input.impl.ClusteredValueGroupsBaseTableProjectionSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.RangeFilter;
import org.apache.druid.query.filter.TypedInFilter;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.filter.AndFilter;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

class IncrementalIndexCursorFactoryClusteredTest extends InitializedNullHandlingTest
{
  private static final long T0 = DateTimes.of("2026-01-01T00:00:00").getMillis();
  private static final TimestampSpec TIMESTAMP_SPEC = new TimestampSpec("ts", "millis", null);

  private static MapBasedInputRow row(long ts, String tenant, String region)
  {
    final Map<String, Object> event = new HashMap<>();
    event.put("ts", ts);
    event.put("tenant", tenant);
    event.put("region", region);
    return new MapBasedInputRow(ts, List.of("tenant", "region"), event);
  }

  private static OnheapIncrementalIndex standardTwoGroup()
  {
    final ClusteredValueGroupsBaseTableProjectionSpec spec = ClusteredValueGroupsBaseTableProjectionSpec.builder()
        .columns(
            new StringDimensionSchema("tenant"),
            new StringDimensionSchema("region"),
            new LongDimensionSchema("__time")
        )
        .clusteringColumns("tenant")
        .build();
    final IncrementalIndexSchema schema = IncrementalIndexSchema.builder()
        .withMinTimestamp(T0)
        .withTimestampSpec(TIMESTAMP_SPEC)
        .withQueryGranularity(Granularities.NONE)
        .withDimensionsSpec(spec.getDimensionsSpec())
        .withRollup(false)
        .withClusterSpec(spec)
        .build();
    final OnheapIncrementalIndex index = (OnheapIncrementalIndex) new OnheapIncrementalIndex.Builder()
        .setIndexSchema(schema)
        .setMaxRowCount(10_000)
        .build();
    // Add tenants out of clustering order to prove the cursor walks groups in clustering-sorted order.
    index.add(row(T0 + 2, "globex", "eu-west-1"));
    index.add(row(T0, "acme", "us-east-1"));
    index.add(row(T0 + 1, "acme", "us-west-2"));
    return index;
  }

  private static CursorBuildSpec specWith(Filter filter)
  {
    return CursorBuildSpec.builder().setFilter(filter).build();
  }

  private static List<List<String>> scanTenantRegion(CursorHolder holder)
  {
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
    return out;
  }

  @Test
  void testNonClusteringVirtualColumnDimensionIsMaterialized()
  {
    // A non-clustering column declared as a virtual-column output (region_upper = upper(region)) is computed at
    // ingest through the VC-aware selector and stored like any other column — VCs aren't limited to clustering
    // columns. region_upper is never in the input row, so a null here would mean the VC was not applied.
    final ClusteredValueGroupsBaseTableProjectionSpec spec = ClusteredValueGroupsBaseTableProjectionSpec.builder()
        .virtualColumns(VirtualColumns.create(
            new ExpressionVirtualColumn("region_upper", "upper(region)", ColumnType.STRING, TestExprMacroTable.INSTANCE)
        ))
        .columns(
            new StringDimensionSchema("tenant"),
            new StringDimensionSchema("region"),
            new StringDimensionSchema("region_upper"),
            new LongDimensionSchema("__time")
        )
        .clusteringColumns("tenant")
        .build();
    final IncrementalIndexSchema schema = IncrementalIndexSchema.builder()
        .withMinTimestamp(T0)
        .withTimestampSpec(TIMESTAMP_SPEC)
        .withQueryGranularity(Granularities.NONE)
        .withDimensionsSpec(spec.getDimensionsSpec())
        .withRollup(false)
        .withClusterSpec(spec)
        .build();
    try (OnheapIncrementalIndex index = (OnheapIncrementalIndex) new OnheapIncrementalIndex.Builder()
        .setIndexSchema(schema)
        .setMaxRowCount(10_000)
        .build()) {
      index.add(row(T0, "acme", "us-east-1"));
      index.add(row(T0 + 1, "acme", "us-west-2"));

      final IncrementalIndexCursorFactory factory = new IncrementalIndexCursorFactory(index);
      try (CursorHolder holder = factory.makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
        final Cursor cursor = holder.asCursor();
        final DimensionSelector regionSel =
            cursor.getColumnSelectorFactory().makeDimensionSelector(DefaultDimensionSpec.of("region"));
        final DimensionSelector upperSel =
            cursor.getColumnSelectorFactory().makeDimensionSelector(DefaultDimensionSpec.of("region_upper"));
        final List<List<String>> out = new ArrayList<>();
        while (!cursor.isDone()) {
          out.add(Arrays.asList(
              regionSel.lookupName(regionSel.getRow().get(0)),
              upperSel.lookupName(upperSel.getRow().get(0))
          ));
          cursor.advance();
        }
        Assertions.assertEquals(
            List.of(
                List.of("us-east-1", "US-EAST-1"),
                List.of("us-west-2", "US-WEST-2")
            ),
            out
        );
      }
    }
  }

  /**
   * Build an index clustered on {@code tenant_lower := lower(tenant)} (a clustering column produced by a group VC) with
   * a non-clustering materialized {@code region_upper := upper(region)} column. The raw inputs {@code tenant} and
   * {@code region} are retained as stored columns, so the query-VC -> materialized-column remap is a pure optimization
   * can be tested. Columns are {@code [tenant_lower (clustering), tenant, region, region_upper, __time]}.
   */
  private static OnheapIncrementalIndex virtualClusteringIndex()
  {
    final ClusteredValueGroupsBaseTableProjectionSpec spec = ClusteredValueGroupsBaseTableProjectionSpec.builder()
        .virtualColumns(VirtualColumns.create(
            new ExpressionVirtualColumn("tenant_lower", "lower(tenant)", ColumnType.STRING, TestExprMacroTable.INSTANCE),
            new ExpressionVirtualColumn("region_upper", "upper(region)", ColumnType.STRING, TestExprMacroTable.INSTANCE)
        ))
        .columns(
            new StringDimensionSchema("tenant_lower"),
            new StringDimensionSchema("tenant"),
            new StringDimensionSchema("region"),
            new StringDimensionSchema("region_upper"),
            new LongDimensionSchema("__time")
        )
        .clusteringColumns("tenant_lower")
        .build();
    final IncrementalIndexSchema schema = IncrementalIndexSchema.builder()
        .withMinTimestamp(T0)
        .withTimestampSpec(TIMESTAMP_SPEC)
        .withQueryGranularity(Granularities.NONE)
        .withDimensionsSpec(spec.getDimensionsSpec())
        .withRollup(false)
        .withClusterSpec(spec)
        .build();
    final OnheapIncrementalIndex index = (OnheapIncrementalIndex) new OnheapIncrementalIndex.Builder()
        .setIndexSchema(schema)
        .setMaxRowCount(10_000)
        .build();
    index.add(row(T0, "Acme", "us-east-1"));
    index.add(row(T0 + 1, "Acme", "us-west-2"));
    index.add(row(T0 + 2, "Globex", "eu-west-1"));
    return index;
  }

  @Test
  void testQueryVirtualColumnEquivalentToClusteringColumnReadsMaterializedColumn()
  {
    // Query VC v0 := lower(tenant) is equivalent to the clustering column tenant_lower (also lower(tenant)). The remap
    // substitutes the materialized tenant_lower clustering constant (instead of recomputing lower(tenant) from the
    // retained raw column) so makeDimensionSelector("v0") returns the per-group value.
    try (OnheapIncrementalIndex index = virtualClusteringIndex()) {
      final IncrementalIndexCursorFactory factory = new IncrementalIndexCursorFactory(index);
      final CursorBuildSpec buildSpec = CursorBuildSpec.builder()
          .setVirtualColumns(VirtualColumns.create(
              new ExpressionVirtualColumn("v0", "lower(tenant)", ColumnType.STRING, TestExprMacroTable.INSTANCE)
          ))
          .build();
      try (CursorHolder holder = factory.makeCursorHolder(buildSpec)) {
        final Cursor cursor = holder.asCursor();
        final DimensionSelector v0Sel =
            cursor.getColumnSelectorFactory().makeDimensionSelector(DefaultDimensionSpec.of("v0"));
        final List<String> out = new ArrayList<>();
        while (!cursor.isDone()) {
          out.add(v0Sel.getRow().size() == 0 ? null : v0Sel.lookupName(v0Sel.getRow().get(0)));
          cursor.advance();
        }
        // acme group (tenant_lower=acme) first, then globex; both materialized from the clustering constant.
        Assertions.assertEquals(List.of("acme", "acme", "globex"), out);
      }
    }
  }

  @Test
  void testQueryVirtualColumnEquivalentToNonClusteringMaterializedColumnReadsMaterializedColumn()
  {
    // Query VC v1 := upper(region) is equivalent to the non-clustering materialized column region_upper. The remap
    // makes makeDimensionSelector("v1") read the per-group physical region_upper column.
    try (OnheapIncrementalIndex index = virtualClusteringIndex()) {
      final IncrementalIndexCursorFactory factory = new IncrementalIndexCursorFactory(index);
      final CursorBuildSpec buildSpec = CursorBuildSpec.builder()
          .setVirtualColumns(VirtualColumns.create(
              new ExpressionVirtualColumn("v1", "upper(region)", ColumnType.STRING, TestExprMacroTable.INSTANCE)
          ))
          .build();
      try (CursorHolder holder = factory.makeCursorHolder(buildSpec)) {
        final Cursor cursor = holder.asCursor();
        final DimensionSelector v1Sel =
            cursor.getColumnSelectorFactory().makeDimensionSelector(DefaultDimensionSpec.of("v1"));
        final List<String> out = new ArrayList<>();
        while (!cursor.isDone()) {
          out.add(v1Sel.getRow().size() == 0 ? null : v1Sel.lookupName(v1Sel.getRow().get(0)));
          cursor.advance();
        }
        Assertions.assertEquals(List.of("US-EAST-1", "US-WEST-2", "EU-WEST-1"), out);
      }
    }
  }

  @Test
  void testQueryVirtualColumnWithoutEquivalentStillRecomputes()
  {
    // a query VC with NO materialized equivalent (upper(region_upper) -> a different expression) is not remapped and
    // is recomputed normally from the stored region_upper column.
    try (OnheapIncrementalIndex index = virtualClusteringIndex()) {
      final IncrementalIndexCursorFactory factory = new IncrementalIndexCursorFactory(index);
      final CursorBuildSpec buildSpec = CursorBuildSpec.builder()
          .setVirtualColumns(VirtualColumns.create(
              new ExpressionVirtualColumn("v2", "lower(region_upper)", ColumnType.STRING, TestExprMacroTable.INSTANCE)
          ))
          .build();
      try (CursorHolder holder = factory.makeCursorHolder(buildSpec)) {
        final Cursor cursor = holder.asCursor();
        final DimensionSelector v2Sel =
            cursor.getColumnSelectorFactory().makeDimensionSelector(DefaultDimensionSpec.of("v2"));
        final List<String> out = new ArrayList<>();
        while (!cursor.isDone()) {
          out.add(v2Sel.getRow().size() == 0 ? null : v2Sel.lookupName(v2Sel.getRow().get(0)));
          cursor.advance();
        }
        // lower(upper(region)) recomputed from the materialized region_upper column.
        Assertions.assertEquals(List.of("us-east-1", "us-west-2", "eu-west-1"), out);
      }
    }
  }

  @Test
  void testRowSignatureExposesClusteringAndNonClusteringColumns()
  {
    // Sink.getSignature() -> IncrementalIndexCursorFactory.getRowSignature() must expose the full logical schema
    // for a clustered in-memory index (clustering + non-clustering + __time), not an empty signature.
    try (OnheapIncrementalIndex index = standardTwoGroup()) {
      final IncrementalIndexCursorFactory factory = new IncrementalIndexCursorFactory(index);
      final RowSignature sig = factory.getRowSignature();
      Assertions.assertEquals(ColumnType.STRING, sig.getColumnType("tenant").orElseThrow());
      Assertions.assertEquals(ColumnType.STRING, sig.getColumnType("region").orElseThrow());
      Assertions.assertEquals(
          ColumnType.LONG,
          sig.getColumnType(ColumnHolder.TIME_COLUMN_NAME).orElseThrow()
      );
    }
  }

  @Test
  void testUnfilteredScanWalksAllGroupsInClusteringOrderWithInjectedConstants()
  {
    try (OnheapIncrementalIndex index = standardTwoGroup()) {
      final IncrementalIndexCursorFactory factory = new IncrementalIndexCursorFactory(index);
      try (CursorHolder holder = factory.makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
        // acme group first (clustering ascending), then globex. tenant injected per-group as a constant.
        Assertions.assertEquals(
            List.of(
                List.of("acme", "us-east-1"),
                List.of("acme", "us-west-2"),
                List.of("globex", "eu-west-1")
            ),
            scanTenantRegion(holder)
        );
      }
    }
  }

  @Test
  void testFilterOnClusteringColumnPrunesNonMatchingGroups()
  {
    try (OnheapIncrementalIndex index = standardTwoGroup()) {
      final IncrementalIndexCursorFactory factory = new IncrementalIndexCursorFactory(index);
      // only the acme group survives; its clustering leaf rewrites to TRUE and is dropped, so the per-group cursor
      // never sees a filter on "tenant" (which the group's facts holder doesn't carry).
      final Filter filter = new EqualityFilter("tenant", ColumnType.STRING, "acme", null);
      try (CursorHolder holder = factory.makeCursorHolder(specWith(filter))) {
        Assertions.assertEquals(
            List.of(
                List.of("acme", "us-east-1"),
                List.of("acme", "us-west-2")
            ),
            scanTenantRegion(holder)
        );
      }
    }
  }

  @Test
  void testRangeFilterOnClusteringColumnResolvesViaClusteringConstant()
  {
    try (OnheapIncrementalIndex index = standardTwoGroup()) {
      final IncrementalIndexCursorFactory factory = new IncrementalIndexCursorFactory(index);
      // A RangeFilter on the clustering column is NOT folded by the pruner (only Equality/In/Null fold), so it
      // survives to the per-group cursor still referencing "tenant". OnHeapClusterGroup doesn't store the clustering
      // column, so the per-group selector factory must expose it as a per-group constant (mirroring the historical
      // fabricated-constant path) or the filter matches a missing/all-null column and returns nothing. tenant in
      // ['a','b'] selects the acme group and excludes globex.
      final Filter filter = new RangeFilter("tenant", ColumnType.STRING, "a", "b", false, false, null);
      try (CursorHolder holder = factory.makeCursorHolder(specWith(filter))) {
        Assertions.assertEquals(
            List.of(
                List.of("acme", "us-east-1"),
                List.of("acme", "us-west-2")
            ),
            scanTenantRegion(holder)
        );
      }
    }
  }

  @Test
  void testVirtualColumnReadingClusteringColumnResolvesViaClusteringConstant()
  {
    try (OnheapIncrementalIndex index = standardTwoGroup()) {
      final IncrementalIndexCursorFactory factory = new IncrementalIndexCursorFactory(index);
      // v := concat(tenant, '_x') reads the clustering column `tenant` (not stored per-row in the group). It has no
      // materialized equivalent so it is retained (recomputed at read). The VC's `tenant` input must resolve to the
      // group's clustering constant, not a missing column.
      final CursorBuildSpec spec = CursorBuildSpec.builder()
          .setVirtualColumns(VirtualColumns.create(
              new ExpressionVirtualColumn("v", "concat(tenant, '_x')", ColumnType.STRING, TestExprMacroTable.INSTANCE)
          ))
          .build();
      try (CursorHolder holder = factory.makeCursorHolder(spec)) {
        final Cursor cursor = holder.asCursor();
        final DimensionSelector sel =
            cursor.getColumnSelectorFactory().makeDimensionSelector(DefaultDimensionSpec.of("v"));
        final List<String> out = new ArrayList<>();
        while (!cursor.isDone()) {
          out.add(sel.getRow().size() == 0 ? null : sel.lookupName(sel.getRow().get(0)));
          cursor.advance();
        }
        Assertions.assertEquals(List.of("acme_x", "acme_x", "globex_x"), out);
      }
    }
  }

  @Test
  void testFilterOnVirtualColumnReadingClusteringColumnResolvesViaClusteringConstant()
  {
    try (OnheapIncrementalIndex index = standardTwoGroup()) {
      final IncrementalIndexCursorFactory factory = new IncrementalIndexCursorFactory(index);
      // A filter on a retained VC that reads the clustering column (v := upper(tenant)) must evaluate the VC against
      // the group's clustering constant, not a missing column. The VC resolves its `tenant` input through its own
      // selector factory, so the clustering constants must be visible below the virtual-column layer or the filter
      // silently rejects every realtime row. upper(tenant)='ACME' selects the acme group.
      final CursorBuildSpec spec = CursorBuildSpec.builder()
          .setVirtualColumns(VirtualColumns.create(
              new ExpressionVirtualColumn("v", "upper(tenant)", ColumnType.STRING, TestExprMacroTable.INSTANCE)
          ))
          .setFilter(new EqualityFilter("v", ColumnType.STRING, "ACME", null))
          .build();
      try (CursorHolder holder = factory.makeCursorHolder(spec)) {
        Assertions.assertEquals(
            List.of(
                List.of("acme", "us-east-1"),
                List.of("acme", "us-west-2")
            ),
            scanTenantRegion(holder)
        );
      }
    }
  }

  @Test
  void testFilterOnNonClusteringColumnRunsOnEverySurvivingGroup()
  {
    try (OnheapIncrementalIndex index = standardTwoGroup()) {
      final IncrementalIndexCursorFactory factory = new IncrementalIndexCursorFactory(index);
      // region=us-east-1 references non-clustering data → both groups survive (UNKNOWN); each group's cursor
      // applies the residual filter. Only the acme group has a us-east-1 row.
      final Filter filter = new EqualityFilter("region", ColumnType.STRING, "us-east-1", null);
      try (CursorHolder holder = factory.makeCursorHolder(specWith(filter))) {
        Assertions.assertEquals(List.of(List.of("acme", "us-east-1")), scanTenantRegion(holder));
      }
    }
  }

  @Test
  void testMixedAndFilterPrunesByClusteringAndFiltersTheRest()
  {
    try (OnheapIncrementalIndex index = standardTwoGroup()) {
      final IncrementalIndexCursorFactory factory = new IncrementalIndexCursorFactory(index);
      // tenant=acme AND region=us-west-2 — pruner keeps acme only; rewriter folds the clustering TRUE out of the
      // AND, leaving just region=us-west-2 for the per-group cursor.
      final LinkedHashSet<Filter> children = new LinkedHashSet<>();
      children.add(new EqualityFilter("tenant", ColumnType.STRING, "acme", null));
      children.add(new EqualityFilter("region", ColumnType.STRING, "us-west-2", null));
      try (CursorHolder holder = factory.makeCursorHolder(specWith(new AndFilter(children)))) {
        Assertions.assertEquals(List.of(List.of("acme", "us-west-2")), scanTenantRegion(holder));
      }
    }
  }

  @Test
  void testSingleGroupMatch()
  {
    try (OnheapIncrementalIndex index = standardTwoGroup()) {
      final IncrementalIndexCursorFactory factory = new IncrementalIndexCursorFactory(index);
      final Filter filter = new TypedInFilter("tenant", ColumnType.STRING, List.of("globex"), null, null);
      try (CursorHolder holder = factory.makeCursorHolder(specWith(filter))) {
        Assertions.assertEquals(List.of(List.of("globex", "eu-west-1")), scanTenantRegion(holder));
      }
    }
  }

  @Test
  void testAllGroupsPrunedReturnsEmptyResult()
  {
    try (OnheapIncrementalIndex index = standardTwoGroup()) {
      final IncrementalIndexCursorFactory factory = new IncrementalIndexCursorFactory(index);
      final Filter filter = new EqualityFilter("tenant", ColumnType.STRING, "initech", null);
      try (CursorHolder holder = factory.makeCursorHolder(specWith(filter))) {
        final Cursor cursor = holder.asCursor();
        Assertions.assertTrue(cursor.isDone());
        // Selector creation on the done cursor must not throw (engines create selectors before checking isDone()).
        Assertions.assertNotNull(
            cursor.getColumnSelectorFactory().makeDimensionSelector(DefaultDimensionSpec.of("tenant"))
        );
      }
    }
  }
}
