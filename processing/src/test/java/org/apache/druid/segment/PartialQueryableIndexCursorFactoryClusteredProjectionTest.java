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
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.file.CountingRangeReader;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.projections.QueryableProjection;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Partial (on-demand) download coverage for a segment that is <em>both</em> clustered <em>and</em> carries an aggregate
 * projection. The partial cursor factory matches the aggregate projection first and, if nothing matches, dispatches to
 * the clustered groups, so this verifies both paths download only their own bundle(s): a projection-matching query
 * fetches the {@code proj} bundle and none of the {@code __base$<ids>} group bundles, while a non-matching query
 * fetches only the surviving group bundle and not {@code proj}. With no shared columns there is no {@code __base}
 * bundle, so the projection is self-contained.
 */
class PartialQueryableIndexCursorFactoryClusteredProjectionTest extends PartialQueryableIndexCursorFactoryTestBase
{
  private static final long T0 = DateTimes.of("2025-01-01").getMillis();
  private static final String PROJECTION_BUNDLE = "proj";
  // tenants sort to dictionary ids acme=0, globex=1 → group bundles __base$0 (2 rows) and __base$1 (1 row)
  private static final String ACME_BUNDLE = "__base$0";
  private static final String GLOBEX_BUNDLE = "__base$1";

  @TempDir
  static File sharedTempDir;

  private static File segmentDir;

  @BeforeAll
  static void buildSegment()
  {
    final ClusteredValueGroupsBaseTableProjectionSpec clusterSpec =
        ClusteredValueGroupsBaseTableProjectionSpec.builder()
            .columns(
                new StringDimensionSchema("tenant"),
                new StringDimensionSchema("region"),
                new LongDimensionSchema("x"),
                new LongDimensionSchema("__time")
            )
            .clusteringColumns("tenant")
            .build();
    final AggregateProjectionSpec projectionSpec =
        AggregateProjectionSpec.builder(PROJECTION_BUNDLE)
                               .groupingColumns(new StringDimensionSchema("tenant"))
                               .aggregators(
                                   new CountAggregatorFactory("cnt"),
                                   new LongSumAggregatorFactory("sum_x", "x")
                               )
                               .build();
    final IncrementalIndexSchema schema =
        IncrementalIndexSchema.builder()
                              .withMinTimestamp(T0)
                              .withTimestampSpec(new TimestampSpec("ts", "millis", null))
                              .withQueryGranularity(Granularities.NONE)
                              .withDimensionsSpec(clusterSpec.getDimensionsSpec())
                              .withRollup(false)
                              .withClusterSpec(clusterSpec)
                              .withProjections(List.of(projectionSpec))
                              .build();
    final File tmpDir = new File(sharedTempDir, "build_" + ThreadLocalRandom.current().nextInt());
    segmentDir = IndexBuilder.create()
                             .useV10()
                             .tmpDir(tmpDir)
                             .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                             .schema(schema)
                             .indexSpec(IndexSpec.builder().withMetadataCompression(CompressionStrategy.NONE).build())
                             .rows(List.of(
                                 row(T0 + 2, "globex", "eu-west-1", 5),
                                 row(T0, "acme", "us-east-1", 10),
                                 row(T0 + 1, "acme", "us-west-2", 20)
                             ))
                             .buildMMappedIndexFile();
  }

  @Test
  void testProjectionMatchDownloadsOnlyProjectionBundle() throws IOException
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    try (IndexAndMapper opened = openIndex(rangeReader, "combo_projection")) {
      final PartialQueryableIndexCursorFactory factory = factory(opened.index());
      // group-by tenant + sum(x): matches the aggregate projection, so the clustered path is never consulted.
      final CursorBuildSpec aggSpec = CursorBuildSpec.builder()
                                                     .setGroupingColumns(List.of("tenant"))
                                                     .setAggregators(List.of(new LongSumAggregatorFactory("sum_x", "x")))
                                                     .setPhysicalColumns(Set.of("tenant", "x"))
                                                     .build();

      // sanity: the index is both clustered and projection-bearing, and this spec resolves to the projection
      Assertions.assertNotNull(opened.index().getClusteredBaseSummary(), "segment must be clustered");
      final QueryableProjection<QueryableIndex> matched = opened.index().getProjection(aggSpec);
      Assertions.assertNotNull(matched, "spec must match the aggregate projection");
      Assertions.assertEquals(PROJECTION_BUNDLE, matched.getName());

      try (AsyncCursorHolder asyncHolder = factory.makeCursorHolderAsync(aggSpec);
           CursorHolder holder = asyncHolder.release()) {
        Assertions.assertNotNull(holder.asCursor(), "projection-matched cursor must build");

        final Set<String> downloaded = opened.mapper().getDownloadedFiles();
        // The projection bundle's columns were materialized.
        Assertions.assertTrue(
            downloaded.stream().anyMatch(f -> f.startsWith(PROJECTION_BUNDLE + "/")),
            "projection bundle must be downloaded; got: " + downloaded
        );
        // None of the clustered group bundles were touched: the projection match short-circuits clustered dispatch.
        Assertions.assertTrue(
            downloaded.stream().noneMatch(f -> f.startsWith(ACME_BUNDLE + "/") || f.startsWith(GLOBEX_BUNDLE + "/")),
            "no cluster group bundle should be downloaded for a projection-matched query; got: " + downloaded
        );
      }
    }
  }

  @Test
  void testNonMatchingQueryDispatchesToClusteredGroups() throws IOException
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    try (IndexAndMapper opened = openIndex(rangeReader, "combo_clustered")) {
      final PartialQueryableIndexCursorFactory factory = factory(opened.index());
      // a filter on the clustering column with no grouping/aggregators does not match the aggregate projection, so the
      // clustered path runs and prunes to the surviving group only.
      final CursorBuildSpec spec = CursorBuildSpec.builder()
                                                  .setFilter(new EqualityFilter("tenant", ColumnType.STRING, "acme", null))
                                                  .build();
      Assertions.assertNull(opened.index().getProjection(spec), "filter-only spec must not match the projection");

      try (AsyncCursorHolder asyncHolder = factory.makeCursorHolderAsync(spec);
           CursorHolder holder = asyncHolder.release()) {
        Assertions.assertEquals(List.of("us-east-1", "us-west-2"), scanRegion(holder));

        final Set<String> downloaded = opened.mapper().getDownloadedFiles();
        // Only the surviving (acme) group bundle materialized.
        Assertions.assertTrue(downloaded.contains(ACME_BUNDLE + "/region"), "got: " + downloaded);
        Assertions.assertTrue(
            downloaded.stream().noneMatch(f -> f.startsWith(GLOBEX_BUNDLE + "/")),
            "pruned globex group must not download; got: " + downloaded
        );
        // The projection bundle is irrelevant to this query and must not be downloaded.
        Assertions.assertTrue(
            downloaded.stream().noneMatch(f -> f.startsWith(PROJECTION_BUNDLE + "/")),
            "projection bundle must not download for a clustered-dispatch query; got: " + downloaded
        );
      }
    }
  }

  private PartialQueryableIndexCursorFactory factory(PartialQueryableIndex index)
  {
    return new PartialQueryableIndexCursorFactory(
        index,
        QueryableIndexTimeBoundaryInspector.create(index),
        noOpAcquirer(directExec())
    );
  }

  private static List<String> scanRegion(CursorHolder holder)
  {
    final Cursor cursor = holder.asCursor();
    final List<String> out = new ArrayList<>();
    if (cursor == null) {
      return out;
    }
    final DimensionSelector regionSel =
        cursor.getColumnSelectorFactory().makeDimensionSelector(DefaultDimensionSpec.of("region"));
    while (!cursor.isDone()) {
      out.add(regionSel.getRow().size() == 0 ? null : regionSel.lookupName(regionSel.getRow().get(0)));
      cursor.advance();
    }
    return out;
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
}
