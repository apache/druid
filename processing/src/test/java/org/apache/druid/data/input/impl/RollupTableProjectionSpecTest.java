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

package org.apache.druid.data.input.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.stream.Collectors;

/**
 * The column and granularity-carrier rules shared with {@link TableProjectionSpec} live in
 * {@link BaseTableProjectionSpec} statics and are covered exhaustively in {@link TableProjectionSpecTest}; this covers
 * what the rollup layout adds — the aggregators, and advertising rollup.
 */
class RollupTableProjectionSpecTest extends InitializedNullHandlingTest
{
  private static RollupTableProjectionSpec pagesSpec()
  {
    return RollupTableProjectionSpec.builder()
        .groupingColumns(new StringDimensionSchema("page"), new LongDimensionSchema("__time"))
        .aggregators(new LongSumAggregatorFactory("total", "total"), new LongSumAggregatorFactory("rows", "rows"))
        .build();
  }

  @Test
  void testShapeFollowsDeclaredOrderAndCarriesAggregators()
  {
    final RollupTableProjectionSpec spec = pagesSpec();

    // Declared grouping order is both storage and sort order, with __time at its declared position; the aggregators
    // become the metric columns and advertise the rollup layout.
    Assertions.assertEquals(
        ImmutableList.of(OrderBy.ascending("page"), OrderBy.ascending("__time")),
        spec.getOrdering()
    );
    Assertions.assertEquals(spec.getGroupingColumns(), spec.getDimensionsSpec().getDimensions());
    Assertions.assertFalse(spec.getDimensionsSpec().isForceSegmentSortByTime());
    Assertions.assertArrayEquals(
        new AggregatorFactory[]{new LongSumAggregatorFactory("total", "total"), new LongSumAggregatorFactory("rows", "rows")},
        spec.getMetrics()
    );
    Assertions.assertTrue(spec.isRollup());
  }

  @Test
  void testEmptyAggregatorsAllowed()
  {
    // A rollup table without metrics collapses rows with identical grouping values, mirroring the legacy rollup=true
    // granularity spec without a metricsSpec.
    final RollupTableProjectionSpec spec = RollupTableProjectionSpec.builder()
        .groupingColumns(new StringDimensionSchema("page"), new LongDimensionSchema("__time"))
        .build();
    Assertions.assertEquals(0, spec.getMetrics().length);
    Assertions.assertTrue(spec.isRollup());
  }

  @Test
  void testAggregatorNameCollidingWithColumnRejected()
  {
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> RollupTableProjectionSpec.builder()
            .groupingColumns(new StringDimensionSchema("page"), new LongDimensionSchema("__time"))
            .aggregators(new LongSumAggregatorFactory("page", "cnt"))
            .build()
    );
    Assertions.assertTrue(
        e.getMessage().contains("aggregator [page] duplicates the name of a column or another aggregator"),
        e.getMessage()
    );
  }

  @Test
  void testAggregatorNameCollidingWithAggregatorRejected()
  {
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> RollupTableProjectionSpec.builder()
            .groupingColumns(new StringDimensionSchema("page"), new LongDimensionSchema("__time"))
            .aggregators(new LongSumAggregatorFactory("total", "total"), new CountAggregatorFactory("total"))
            .build()
    );
    Assertions.assertTrue(
        e.getMessage().contains("aggregator [total] duplicates the name of a column or another aggregator"),
        e.getMessage()
    );
  }

  /**
   * A metric named for the query-granularity carrier would be shadowed by the carrier virtual column once a query
   * granularity is attached (virtual columns resolve before stored columns), so it is rejected at construction.
   */
  @Test
  void testAggregatorNamedForGranularityCarrierRejected()
  {
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> RollupTableProjectionSpec.builder()
            .groupingColumns(new StringDimensionSchema("page"), new LongDimensionSchema("__time"))
            .aggregators(new LongSumAggregatorFactory(Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME, "cnt"))
            .build()
    );
    Assertions.assertTrue(
        e.getMessage().contains(
            "aggregator cannot be named [" + Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME + "]"
        ),
        e.getMessage()
    );
  }

  /**
   * The spec's aggregators are applied uniformly: ingestion combines what arrives under the metric column's name, and
   * compaction re-aggregates stored rows. An aggregator that is not its own combining form (a COUNT, a sketch build,
   * an input field differing from the output) would silently change results on re-aggregation, so it is rejected — a
   * count is stored by summing a count column.
   */
  @Test
  void testNonSelfCombiningAggregatorRejected()
  {
    final DruidException count = Assertions.assertThrows(
        DruidException.class,
        () -> RollupTableProjectionSpec.builder()
            .groupingColumns(new StringDimensionSchema("page"), new LongDimensionSchema("__time"))
            .aggregators(new CountAggregatorFactory("rows"))
            .build()
    );
    Assertions.assertTrue(count.getMessage().contains("aggregator [rows] is not its own combining form"), count.getMessage());

    final DruidException differentInput = Assertions.assertThrows(
        DruidException.class,
        () -> RollupTableProjectionSpec.builder()
            .groupingColumns(new StringDimensionSchema("page"), new LongDimensionSchema("__time"))
            .aggregators(new LongSumAggregatorFactory("total", "cnt"))
            .build()
    );
    Assertions.assertTrue(
        differentInput.getMessage().contains("aggregator [total] is not its own combining form"),
        differentInput.getMessage()
    );
  }

  @Test
  void testNullAggregatorEntryRejected()
  {
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> RollupTableProjectionSpec.builder()
            .groupingColumns(new StringDimensionSchema("page"), new LongDimensionSchema("__time"))
            .aggregators(new LongSumAggregatorFactory("total", "total"), null)
            .build()
    );
    Assertions.assertTrue(e.getMessage().contains("aggregators must not contain null entries"), e.getMessage());
  }

  @Test
  void testWithQueryGranularityAddsCarrierAndKeepsAggregators()
  {
    final RollupTableProjectionSpec spec = pagesSpec().withQueryGranularity(Granularities.HOUR);

    final VirtualColumn vc = spec.getVirtualColumns().getVirtualColumn(Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME);
    Assertions.assertNotNull(vc);
    Assertions.assertEquals(Granularities.HOUR, spec.getQueryGranularity());
    Assertions.assertEquals(pagesSpec().getGroupingColumns(), spec.getGroupingColumns());
    Assertions.assertArrayEquals(pagesSpec().getMetrics(), spec.getMetrics());

    // Idempotent once a carrier is present.
    Assertions.assertSame(spec, spec.withQueryGranularity(Granularities.DAY));
  }

  /**
   * The granularity rules are the shared {@link BaseTableProjectionSpec} validators, exercised exhaustively in
   * {@link TableProjectionSpecTest}; this just proves the rollup spec is wired to them.
   */
  @Test
  void testWithQueryGranularityRejectionsAreWired()
  {
    Assertions.assertTrue(
        Assertions.assertThrows(DruidException.class, () -> pagesSpec().withQueryGranularity(Granularities.ALL))
                  .getMessage()
                  .contains("ALL")
    );
    Assertions.assertTrue(
        Assertions.assertThrows(
            DruidException.class,
            () -> pagesSpec().withQueryGranularity(
                new PeriodGranularity(new Period("P1D"), null, DateTimes.inferTzFromString("America/Los_Angeles"))
            )
        ).getMessage().contains("only period granularities in the UTC time zone")
    );
  }

  @Test
  void testHasEqualCompactionStateIgnoresQueryGranularityAndComparesAggregators()
  {
    Assertions.assertTrue(pagesSpec().withQueryGranularity(Granularities.HOUR).hasEqualCompactionState(pagesSpec()));
    Assertions.assertTrue(pagesSpec().hasEqualCompactionState(pagesSpec().withQueryGranularity(Granularities.HOUR)));

    final RollupTableProjectionSpec differentAggregators = RollupTableProjectionSpec.builder()
        .groupingColumns(new StringDimensionSchema("page"), new LongDimensionSchema("__time"))
        .aggregators(new LongSumAggregatorFactory("total", "total"))
        .build();
    Assertions.assertFalse(pagesSpec().hasEqualCompactionState(differentAggregators));
  }

  @Test
  void testHasEqualCompactionStateRejectsDifferentSpecType()
  {
    // A plain table over the same columns is a different layout: it stores every row, a rollup table aggregates them.
    final TableProjectionSpec plain = TableProjectionSpec.builder()
        .columns(new StringDimensionSchema("page"), new LongDimensionSchema("__time"))
        .build();
    Assertions.assertFalse(pagesSpec().hasEqualCompactionState(plain));
    Assertions.assertFalse(plain.hasEqualCompactionState(pagesSpec()));
  }

  @Test
  void testWithAdditionalColumnsAppendsGroupingColumnsAndKeepsAggregators()
  {
    final RollupTableProjectionSpec spec = pagesSpec()
        .withQueryGranularity(Granularities.HOUR)
        .withAdditionalColumns(ImmutableList.of(new StringDimensionSchema("city")));

    Assertions.assertEquals(
        ImmutableList.of("page", "__time", "city"),
        spec.getGroupingColumns().stream().map(DimensionSchema::getName).collect(Collectors.toList())
    );
    Assertions.assertArrayEquals(pagesSpec().getMetrics(), spec.getMetrics());
    Assertions.assertEquals(Granularities.HOUR, spec.getQueryGranularity());

    Assertions.assertSame(spec, spec.withAdditionalColumns(null));
    Assertions.assertSame(spec, spec.withAdditionalColumns(Collections.emptyList()));
  }

  @Test
  void testWithAdditionalColumnsRejectsTimeColumnAndAggregatorNames()
  {
    Assertions.assertTrue(
        Assertions.assertThrows(
            DruidException.class,
            () -> pagesSpec().withAdditionalColumns(ImmutableList.of(new LongDimensionSchema("__time")))
        ).getMessage().contains("[__time]")
    );
    // An appended column colliding with an aggregator would be one column with two definitions; the constructor's
    // aggregator validation rejects it.
    Assertions.assertTrue(
        Assertions.assertThrows(
            DruidException.class,
            () -> pagesSpec().withAdditionalColumns(ImmutableList.of(new LongDimensionSchema("total")))
        ).getMessage().contains("aggregator [total] duplicates the name of a column or another aggregator")
    );
  }

  @Test
  void testSerdeRoundTripsThroughInterface() throws Exception
  {
    final ObjectMapper mapper = TestHelper.makeJsonMapper();

    final BaseTableProjectionSpec bare = pagesSpec();
    final String bareJson = mapper.writeValueAsString(bare);
    Assertions.assertTrue(bareJson.contains("\"type\":\"rollupTable\""), bareJson);
    Assertions.assertEquals(bare, mapper.readValue(bareJson, BaseTableProjectionSpec.class));

    final BaseTableProjectionSpec withGranularity = pagesSpec().withQueryGranularity(Granularities.HOUR);
    Assertions.assertEquals(
        withGranularity,
        mapper.readValue(mapper.writeValueAsString(withGranularity), BaseTableProjectionSpec.class)
    );
  }
}
