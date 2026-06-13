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
import org.apache.druid.data.input.impl.ClusteredValueGroupsBaseTableProjectionSpec;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
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
        .clusteringColumns(new StringDimensionSchema("tenant"))
        .dimensions(new StringDimensionSchema("region"))
        .metrics(new CountAggregatorFactory("count"))
        .build();
  }

  private static IncrementalIndexSchema clusteredSchema(ClusteredValueGroupsBaseTableProjectionSpec spec)
  {
    return IncrementalIndexSchema.builder()
                                 .withMinTimestamp(T0)
                                 .withTimestampSpec(TIMESTAMP_SPEC)
                                 .withQueryGranularity(Granularities.NONE)
                                 .withDimensionsSpec(spec.getDimensionsSpec())
                                 .withMetrics(new CountAggregatorFactory("count"))
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

  private QueryableIndex buildSegmentWithMetrics(String dirName, List<InputRow> rows, AggregatorFactory[] metrics)
  {
    final ClusteredValueGroupsBaseTableProjectionSpec spec = ClusteredValueGroupsBaseTableProjectionSpec.builder()
        .clusteringColumns(new StringDimensionSchema("tenant"))
        .dimensions(new StringDimensionSchema("region"))
        .metrics(metrics)
        .build();
    final IncrementalIndexSchema schema = IncrementalIndexSchema.builder()
                                                                .withMinTimestamp(T0)
                                                                .withTimestampSpec(TIMESTAMP_SPEC)
                                                                .withQueryGranularity(Granularities.NONE)
                                                                .withDimensionsSpec(spec.getDimensionsSpec())
                                                                .withMetrics(metrics)
                                                                .withRollup(false)
                                                                .withClusterSpec(spec)
                                                                .build();
    return IndexBuilder.create()
                       .useV10()
                       .tmpDir(new File(tempDir, dirName))
                       .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                       .schema(schema)
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

    // Per-group QueryableIndex: region + count physically present, clustering column not.
    final QueryableIndex acmeGroup = index.getClusterGroupQueryableIndex(groups.get(0));
    Assertions.assertNotNull(acmeGroup);
    Assertions.assertEquals(2, acmeGroup.getNumRows());
    Assertions.assertNotNull(acmeGroup.getColumnHolder("region"));
    Assertions.assertNotNull(acmeGroup.getColumnHolder("count"));
    Assertions.assertNull(acmeGroup.getColumnHolder("tenant"));

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
        new AggregatorFactory[]{new LongSumAggregatorFactory("count", "count")},
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

    Assertions.assertEquals(
        List.of(
            List.of("acme", "us-east-1"),
            List.of("acme", "us-west-2"),
            List.of("globex", "eu-west-1"),
            List.of("globex", "eu-central-1"),
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

  @Test
  void testMergeClusteredSegmentsWithDifferentAggregatorsRejected()
  {
    // All adapters reaching merge descend from one IncrementalIndexSchema, so cluster groups always share an
    // aggregator set; mergeClusterSchemas asserts that invariant defensively. A hand-built pair with differing
    // aggregators must be rejected loudly rather than NPE'ing later when a group missing a segment-wide metric is
    // written.
    final QueryableIndex segment1 = buildSegmentWithMetrics(
        "agg-mismatch-1",
        List.of(row(T0, "acme", "us-east-1")),
        new AggregatorFactory[]{new CountAggregatorFactory("count")}
    );
    final QueryableIndex segment2 = buildSegmentWithMetrics(
        "agg-mismatch-2",
        List.of(row(T0 + 1, "globex", "eu-west-1")),
        new AggregatorFactory[]{new CountAggregatorFactory("total")}
    );

    final IndexBuilder builderForMerger = IndexBuilder.create().useV10();
    final IndexMergerV10 merger = new IndexMergerV10(
        TestHelper.makeJsonMapper(),
        builderForMerger.getIndexIO(),
        OffHeapMemorySegmentWriteOutMediumFactory.instance()
    );

    // Pass the union of both segments' metrics as the merge aggregators so the base merge's own metric-consistency
    // check (which would otherwise reject the mismatch first with an IAE) is satisfied, and the clustered guard in
    // mergeClusterSchemas is the thing that rejects the differing per-segment summary aggregators.
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> merger.mergeQueryableIndex(
            List.of(segment1, segment2),
            false,
            new AggregatorFactory[]{new CountAggregatorFactory("count"), new CountAggregatorFactory("total")},
            new File(tempDir, "agg-mismatch-merged"),
            IndexSpec.getDefault(),
            OffHeapMemorySegmentWriteOutMediumFactory.instance(),
            -1
        )
    );
    Assertions.assertTrue(
        e.getMessage().contains("different aggregators"),
        "expected aggregator-mismatch message, got: " + e.getMessage()
    );
  }
}
