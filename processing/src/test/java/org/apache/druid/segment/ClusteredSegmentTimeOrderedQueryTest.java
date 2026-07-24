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
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Order;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryConfig;
import org.apache.druid.query.scan.ScanQueryEngine;
import org.apache.druid.query.scan.ScanQueryQueryToolChest;
import org.apache.druid.query.scan.ScanQueryRunnerFactory;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryEngine;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Functional coverage that the time-ordering query engines work over a CLUSTERED base-table segment whose {@code
 * __time} is the first non-clustering column (so the read path serves them via the k-way {@code __time} merge; see
 * {@link MergingClusterGroupCursor}). Runs a granular {@code timeseries} and an ascending/descending native {@code
 * scan} — scan hard-enforces cursor time ordering, so a passing time-ordered scan over a multi-group clustered segment
 * could not happen without the merge. Each result is also compared against an equivalent non-clustered (naturally
 * {@code __time}-sorted) segment to pin correctness.
 */
class ClusteredSegmentTimeOrderedQueryTest extends InitializedNullHandlingTest
{
  private static final String DATA_SOURCE = "clustered-time-ordered";
  private static final TimestampSpec TIMESTAMP_SPEC = new TimestampSpec("ts", "millis", null);
  private static final long T0 = DateTimes.of("2026-01-01T00:00:00.000Z").getMillis();
  private static final long MINUTE = 60_000L;
  private static final Interval INTERVAL = Intervals.utc(T0, T0 + 4 * MINUTE);

  // Two tenants => two cluster groups; timestamps interleave across the groups, ingested out of order. So the
  // clustering-first concatenation is NOT globally time-ordered, and the merge must interleave by __time.
  private static final List<InputRow> ROWS = List.of(
      row(T0, "acme", 1),
      row(T0 + 2 * MINUTE, "acme", 4),
      row(T0 + MINUTE, "globex", 2),
      row(T0 + 3 * MINUTE, "globex", 8)
  );

  // A single tenant => a single cluster group, so queries take the single-group clustered path.
  private static final List<InputRow> SINGLE_GROUP_ROWS = List.of(
      row(T0, "acme", 1),
      row(T0 + MINUTE, "acme", 2),
      row(T0 + 2 * MINUTE, "acme", 4)
  );

  @TempDir
  File tempDir;

  private Segment clusteredSegment;
  private Segment nonClusteredSegment;

  @BeforeEach
  void setUp()
  {
    clusteredSegment = new QueryableIndexSegment(buildClustered("clustered", ROWS), SegmentId.dummy(DATA_SOURCE));
    nonClusteredSegment = new QueryableIndexSegment(buildNonClustered("plain", ROWS), SegmentId.dummy(DATA_SOURCE));
  }

  @Test
  void testGranularTimeseriesOverClusteredSegment()
  {
    // MINUTE-granular sum requires a globally __time-ordered cursor (the granularizer advances buckets as time
    // increases); each bucket here comes from a different group, so the merge is what makes it correct.
    final List<List<Long>> expected = List.of(
        List.of(T0, 1L),
        List.of(T0 + MINUTE, 2L),
        List.of(T0 + 2 * MINUTE, 4L),
        List.of(T0 + 3 * MINUTE, 8L)
    );
    Assertions.assertEquals(expected, runTimeseries(clusteredSegment));
    // ... and identical to the equivalent non-clustered, naturally time-sorted segment.
    Assertions.assertEquals(runTimeseries(nonClusteredSegment), runTimeseries(clusteredSegment));
  }

  @Test
  void testAscendingScanOverClusteredSegment()
  {
    final List<List<Object>> expected = List.of(
        Arrays.asList(T0, "acme", 1L),
        Arrays.asList(T0 + MINUTE, "globex", 2L),
        Arrays.asList(T0 + 2 * MINUTE, "acme", 4L),
        Arrays.asList(T0 + 3 * MINUTE, "globex", 8L)
    );
    Assertions.assertEquals(expected, runScanRows(clusteredSegment, Order.ASCENDING));
    Assertions.assertEquals(
        runScanRows(nonClusteredSegment, Order.ASCENDING),
        runScanRows(clusteredSegment, Order.ASCENDING)
    );
  }

  @Test
  void testDescendingScanOverClusteredSegment()
  {
    final List<List<Object>> expected = List.of(
        Arrays.asList(T0 + 3 * MINUTE, "globex", 8L),
        Arrays.asList(T0 + 2 * MINUTE, "acme", 4L),
        Arrays.asList(T0 + MINUTE, "globex", 2L),
        Arrays.asList(T0, "acme", 1L)
    );
    Assertions.assertEquals(expected, runScanRows(clusteredSegment, Order.DESCENDING));
    Assertions.assertEquals(
        runScanRows(nonClusteredSegment, Order.DESCENDING),
        runScanRows(clusteredSegment, Order.DESCENDING)
    );
  }

  @Test
  void testAscendingScanOverSingleGroupClusteredSegment()
  {
    // A clustered segment that resolves to a SINGLE cluster group must also serve time-ordered scans. Previously the
    // single-group path advertised the clustering-first ordering (getTimeOrder()==NONE) and the scan engine rejected
    // it, so the same query succeeded with >=2 groups but failed with one. Scan throws unless the cursor is
    // time-ordered, so this passing is the proof the single-group path now reports __time ordering.
    final Segment clustered =
        new QueryableIndexSegment(buildClustered("clustered-single", SINGLE_GROUP_ROWS), SegmentId.dummy(DATA_SOURCE));
    final Segment plain =
        new QueryableIndexSegment(buildNonClustered("plain-single", SINGLE_GROUP_ROWS), SegmentId.dummy(DATA_SOURCE));
    final List<List<Object>> expected = List.of(
        Arrays.asList(T0, "acme", 1L),
        Arrays.asList(T0 + MINUTE, "acme", 2L),
        Arrays.asList(T0 + 2 * MINUTE, "acme", 4L)
    );
    Assertions.assertEquals(expected, runScanRows(clustered, Order.ASCENDING));
    Assertions.assertEquals(runScanRows(plain, Order.ASCENDING), runScanRows(clustered, Order.ASCENDING));
  }

  @Test
  void testDescendingScanOverSingleGroupClusteredSegment()
  {
    final Segment clustered = new QueryableIndexSegment(
        buildClustered("clustered-single-desc", SINGLE_GROUP_ROWS),
        SegmentId.dummy(DATA_SOURCE)
    );
    final Segment plain = new QueryableIndexSegment(
        buildNonClustered("plain-single-desc", SINGLE_GROUP_ROWS),
        SegmentId.dummy(DATA_SOURCE)
    );
    final List<List<Object>> expected = List.of(
        Arrays.asList(T0 + 2 * MINUTE, "acme", 4L),
        Arrays.asList(T0 + MINUTE, "acme", 2L),
        Arrays.asList(T0, "acme", 1L)
    );
    Assertions.assertEquals(expected, runScanRows(clustered, Order.DESCENDING));
    Assertions.assertEquals(runScanRows(plain, Order.DESCENDING), runScanRows(clustered, Order.DESCENDING));
  }

  private static List<List<Long>> runTimeseries(Segment segment)
  {
    final TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                        .dataSource(DATA_SOURCE)
                                        .granularity(Granularities.MINUTE)
                                        .intervals(new MultipleIntervalSegmentSpec(List.of(INTERVAL)))
                                        .aggregators(new LongSumAggregatorFactory("sum_m", "m"))
                                        .build();
    final TimeseriesQueryRunnerFactory factory = new TimeseriesQueryRunnerFactory(
        new TimeseriesQueryQueryToolChest(),
        new TimeseriesQueryEngine(),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );
    final List<Result<TimeseriesResultValue>> results =
        factory.createRunner(segment).run(QueryPlus.wrap(query)).toList();
    final List<List<Long>> out = new ArrayList<>();
    for (Result<TimeseriesResultValue> result : results) {
      final long sum = ((Number) result.getValue().getMetric("sum_m")).longValue();
      out.add(List.of(result.getTimestamp().getMillis(), sum));
    }
    return out;
  }

  private static List<List<Object>> runScanRows(Segment segment, Order order)
  {
    final ScanQuery query = Druids.newScanQueryBuilder()
                                  .dataSource(DATA_SOURCE)
                                  .intervals(new MultipleIntervalSegmentSpec(List.of(INTERVAL)))
                                  .columns(ColumnHolder.TIME_COLUMN_NAME, "tenant", "m")
                                  .order(order)
                                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_LIST)
                                  .build();
    final ScanQueryRunnerFactory factory = new ScanQueryRunnerFactory(
        new ScanQueryQueryToolChest(DefaultGenericQueryMetricsFactory.instance()),
        new ScanQueryEngine(),
        new ScanQueryConfig()
    );
    final List<ScanResultValue> results = factory.createRunner(segment).run(QueryPlus.wrap(query)).toList();
    // Extract full [__time, tenant, m] rows so the comparison against the non-clustered baseline verifies per-column
    // dispatch (winning group's tenant/m paired with the right __time), not just the time column.
    final List<List<Object>> rows = new ArrayList<>();
    for (ScanResultValue result : results) {
      for (Object event : (List<?>) result.getEvents()) {
        @SuppressWarnings("unchecked")
        final Map<String, Object> row = (Map<String, Object>) event;
        rows.add(Arrays.asList(
            DimensionHandlerUtils.convertObjectToLong(row.get(ColumnHolder.TIME_COLUMN_NAME)),
            row.get("tenant"),
            DimensionHandlerUtils.convertObjectToLong(row.get("m"))
        ));
      }
    }
    return rows;
  }

  private QueryableIndex buildClustered(String dirName, List<InputRow> rows)
  {
    // columns [tenant, __time, m], clustering [tenant] => ordering [tenant, __time, m], so __time is the first
    // non-clustering column and each cluster group is individually __time-sorted.
    final ClusteredValueGroupsBaseTableProjectionSpec clusterSpec =
        ClusteredValueGroupsBaseTableProjectionSpec.builder()
            .columns(
                new StringDimensionSchema("tenant"),
                new LongDimensionSchema("__time"),
                new LongDimensionSchema("m")
            )
            .clusteringColumns("tenant")
            .build();
    final IncrementalIndexSchema schema =
        IncrementalIndexSchema.builder()
                              .withMinTimestamp(T0)
                              .withTimestampSpec(TIMESTAMP_SPEC)
                              .withQueryGranularity(Granularities.NONE)
                              .withDimensionsSpec(clusterSpec.getDimensionsSpec())
                              .withRollup(false)
                              .withClusterSpec(clusterSpec)
                              .build();
    return IndexBuilder.create()
                       .useV10()
                       .tmpDir(new File(tempDir, dirName))
                       .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                       .schema(schema)
                       .rows(rows)
                       .buildMMappedIndex();
  }

  private QueryableIndex buildNonClustered(String dirName, List<InputRow> rows)
  {
    final IncrementalIndexSchema schema =
        IncrementalIndexSchema.builder()
                              .withMinTimestamp(T0)
                              .withTimestampSpec(TIMESTAMP_SPEC)
                              .withQueryGranularity(Granularities.NONE)
                              .withDimensionsSpec(
                                  DimensionsSpec.builder()
                                                .setDimensions(List.of(
                                                    new StringDimensionSchema("tenant"),
                                                    new LongDimensionSchema("m")
                                                ))
                                                .build()
                              )
                              .withRollup(false)
                              .build();
    return IndexBuilder.create()
                       .tmpDir(new File(tempDir, dirName))
                       .schema(schema)
                       .rows(rows)
                       .buildMMappedIndex();
  }

  private static InputRow row(long ts, String tenant, long m)
  {
    final Map<String, Object> event = new HashMap<>();
    event.put("ts", ts);
    event.put("tenant", tenant);
    event.put("m", m);
    return new MapBasedInputRow(ts, List.of("tenant", "m"), event);
  }
}
