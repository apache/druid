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

package org.apache.druid.query.groupby;

import org.apache.druid.query.QueryResourceId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class GroupByStatsProviderTest
{
  private static final double DELTA = 1e-9;

  @Test
  public void testMetricCollection()
  {
    GroupByStatsProvider statsProvider = new GroupByStatsProvider();

    QueryResourceId id1 = new QueryResourceId("q1");
    GroupByStatsProvider.PerQueryStats stats1 = statsProvider.getPerQueryStatsContainer(id1);

    stats1.mergeBufferAcquisitionTime(300);
    stats1.mergeBufferAcquisitionTime(400);
    // Two slices of the same query: usage SUMS to 80, while spill proximity is the per-slice MAX. Both slices report
    // their own peak fill ratio (bucket-count based, in [0,1]); the fullest (0.05) drives proximity.
    stats1.addMergeBufferUsedBytes(50);
    stats1.sliceUsage(0.05);
    stats1.addMergeBufferUsedBytes(30);
    stats1.sliceUsage(0.03);
    stats1.spilledBytes(200);
    stats1.spilledBytes(400);
    stats1.dictionarySize(100);
    stats1.dictionarySize(200);

    QueryResourceId id2 = new QueryResourceId("q2");
    GroupByStatsProvider.PerQueryStats stats2 = statsProvider.getPerQueryStatsContainer(id2);

    stats2.mergeBufferAcquisitionTime(500);
    stats2.mergeBufferAcquisitionTime(600);
    // Single slice at 0.05.
    stats2.addMergeBufferUsedBytes(100);
    stats2.sliceUsage(0.05);
    stats2.spilledBytes(400);
    stats2.spilledBytes(600);
    stats2.dictionarySize(300);
    stats2.dictionarySize(400);

    GroupByStatsProvider.AggregateStats aggregateStats = statsProvider.getStatsSince();
    Assertions.assertEquals(0L, aggregateStats.getMergeBufferQueries());
    Assertions.assertEquals(0L, aggregateStats.getMergeBufferAcquisitionTimeNs());
    Assertions.assertEquals(0L, aggregateStats.getMaxMergeBufferAcquisitionTimeNs());
    Assertions.assertEquals(0L, aggregateStats.getTotalMergeBufferUsedBytes());
    Assertions.assertEquals(0L, aggregateStats.getMaxMergeBufferUsedBytes());
    Assertions.assertEquals(0.0, aggregateStats.getMaxSpillProximity(), DELTA);
    Assertions.assertEquals(0L, aggregateStats.getSpilledQueries());
    Assertions.assertEquals(0L, aggregateStats.getSpilledBytes());
    Assertions.assertEquals(0L, aggregateStats.getMaxSpilledBytes());
    Assertions.assertEquals(0L, aggregateStats.getMergeDictionarySize());
    Assertions.assertEquals(0L, aggregateStats.getMaxMergeDictionarySize());

    statsProvider.closeQuery(id1);
    statsProvider.closeQuery(id2);

    aggregateStats = statsProvider.getStatsSince();
    Assertions.assertEquals(2, aggregateStats.getMergeBufferQueries());
    Assertions.assertEquals(1800L, aggregateStats.getMergeBufferAcquisitionTimeNs());
    Assertions.assertEquals(1100L, aggregateStats.getMaxMergeBufferAcquisitionTimeNs());
    // bytesUsed sums across queries AND across each query's slices: (50 + 30) + 100 = 180.
    Assertions.assertEquals(180L, aggregateStats.getTotalMergeBufferUsedBytes());
    // maxBytesUsed is the max per-query summed usage: max(80, 100) = 100.
    Assertions.assertEquals(100L, aggregateStats.getMaxMergeBufferUsedBytes());
    // maxSpillProximity is the max per-query proximity, where each query's proximity is its fullest slice's
    // fill ratio: q1 -> max(0.05, 0.03) = 0.05, q2 -> 0.05, so max = 0.05.
    Assertions.assertEquals(0.05, aggregateStats.getMaxSpillProximity(), DELTA);
    Assertions.assertEquals(2L, aggregateStats.getSpilledQueries());
    Assertions.assertEquals(1600L, aggregateStats.getSpilledBytes());
    Assertions.assertEquals(1000L, aggregateStats.getMaxSpilledBytes());
    Assertions.assertEquals(1000L, aggregateStats.getMergeDictionarySize());
    Assertions.assertEquals(700L, aggregateStats.getMaxMergeDictionarySize());
  }

  @Test
  public void testMetricsWithMultipleQueries()
  {
    GroupByStatsProvider statsProvider = new GroupByStatsProvider();

    QueryResourceId r1 = new QueryResourceId("r1");
    GroupByStatsProvider.PerQueryStats stats1 = statsProvider.getPerQueryStatsContainer(r1);
    stats1.mergeBufferAcquisitionTime(2000);
    stats1.addMergeBufferUsedBytes(50);
    stats1.sliceUsage(0.05);
    stats1.spilledBytes(100);
    stats1.dictionarySize(200);

    QueryResourceId r2 = new QueryResourceId("r2");
    GroupByStatsProvider.PerQueryStats stats2 = statsProvider.getPerQueryStatsContainer(r2);
    stats2.mergeBufferAcquisitionTime(100);
    stats2.addMergeBufferUsedBytes(500);
    stats2.sliceUsage(0.5);
    stats2.spilledBytes(150);
    stats2.dictionarySize(250);

    QueryResourceId r3 = new QueryResourceId("r3");
    GroupByStatsProvider.PerQueryStats stats3 = statsProvider.getPerQueryStatsContainer(r3);
    stats3.mergeBufferAcquisitionTime(200);
    stats3.addMergeBufferUsedBytes(100);
    stats3.sliceUsage(0.1);
    stats3.spilledBytes(3000);
    stats3.dictionarySize(300);

    QueryResourceId r4 = new QueryResourceId("r4");
    GroupByStatsProvider.PerQueryStats stats4 = statsProvider.getPerQueryStatsContainer(r4);
    stats4.mergeBufferAcquisitionTime(300);
    stats4.addMergeBufferUsedBytes(75);
    stats4.sliceUsage(0.075);
    stats4.spilledBytes(200);
    stats4.dictionarySize(1500);

    statsProvider.closeQuery(r1);
    statsProvider.closeQuery(r2);
    statsProvider.closeQuery(r3);
    statsProvider.closeQuery(r4);

    GroupByStatsProvider.AggregateStats aggregateStats = statsProvider.getStatsSince();

    Assertions.assertEquals(2000L, aggregateStats.getMaxMergeBufferAcquisitionTimeNs());
    Assertions.assertEquals(500L, aggregateStats.getMaxMergeBufferUsedBytes());
    // Max per-query proximity across the four queries: max(0.05, 0.5, 0.1, 0.075) = 0.5.
    Assertions.assertEquals(0.5, aggregateStats.getMaxSpillProximity(), DELTA);
    Assertions.assertEquals(3000L, aggregateStats.getMaxSpilledBytes());
    Assertions.assertEquals(1500L, aggregateStats.getMaxMergeDictionarySize());

    Assertions.assertEquals(4L, aggregateStats.getMergeBufferQueries());
    Assertions.assertEquals(2600L, aggregateStats.getMergeBufferAcquisitionTimeNs());
    Assertions.assertEquals(725L, aggregateStats.getTotalMergeBufferUsedBytes());
    Assertions.assertEquals(4L, aggregateStats.getSpilledQueries());
    Assertions.assertEquals(3450L, aggregateStats.getSpilledBytes());
    Assertions.assertEquals(2250L, aggregateStats.getMergeDictionarySize());
  }

  @Test
  public void testPerQueryUsedBytesSumsWhileSpillProximityTakesSliceMax()
  {
    GroupByStatsProvider.PerQueryStats stats = new GroupByStatsProvider.PerQueryStats();

    // Simulate a ConcurrentGrouper closing four equally-sized slices. Each slice reports its own peak fill ratio (in
    // [0, 1]) and its own peak used bytes. Used bytes accumulate (sum); spill proximity is driven by the fullest slice.
    stats.addMergeBufferUsedBytes(10);
    stats.sliceUsage(0.01);
    stats.addMergeBufferUsedBytes(20);
    stats.sliceUsage(0.02);
    stats.addMergeBufferUsedBytes(30);
    stats.sliceUsage(0.03);
    stats.addMergeBufferUsedBytes(40);
    stats.sliceUsage(0.04);

    // Used bytes are summed across slices.
    Assertions.assertEquals(100L, stats.getMergeBufferUsedBytes());
    // Proximity is the fullest slice's fill ratio (0.04), NOT anything computed from the summed usage.
    Assertions.assertEquals(0.04, stats.getSpillProximity(), DELTA);
  }

  @Test
  public void testSpillProximityClampsToRange()
  {
    // A slice at exactly the spill point.
    GroupByStatsProvider.PerQueryStats atSpill = new GroupByStatsProvider.PerQueryStats();
    atSpill.sliceUsage(1.0);
    Assertions.assertEquals(1.0, atSpill.getSpillProximity(), DELTA);

    // Defensive clamping: a caller that somehow passes >1.0 must not produce >1.0.
    GroupByStatsProvider.PerQueryStats over = new GroupByStatsProvider.PerQueryStats();
    over.sliceUsage(1.5);
    Assertions.assertEquals(1.0, over.getSpillProximity(), DELTA);

    // Defensive clamping on the low end: a negative ratio is treated as 0.0 (never contributes to the max).
    GroupByStatsProvider.PerQueryStats neg = new GroupByStatsProvider.PerQueryStats();
    neg.sliceUsage(-0.25);
    Assertions.assertEquals(0.0, neg.getSpillProximity(), DELTA);

    // NaN is ignored entirely so a never-initialized grouper does not corrupt the accumulator.
    GroupByStatsProvider.PerQueryStats nan = new GroupByStatsProvider.PerQueryStats();
    nan.sliceUsage(Double.NaN);
    Assertions.assertEquals(0.0, nan.getSpillProximity(), DELTA);
  }

  @Test
  public void testSpillProximityZeroWhenNoSliceUsageRecorded()
  {
    GroupByStatsProvider.PerQueryStats stats = new GroupByStatsProvider.PerQueryStats();
    // No sliceUsage() call: proximity stays at its initial 0.0.
    stats.addMergeBufferUsedBytes(500);
    Assertions.assertEquals(0.0, stats.getSpillProximity(), DELTA);
  }

  @Test
  public void testSpillProximityPicksFullestSliceWhenSlicesDiffer()
  {
    GroupByStatsProvider.PerQueryStats stats = new GroupByStatsProvider.PerQueryStats();
    // Three slices with differing fill; proximity is the fullest.
    stats.sliceUsage(0.2);
    stats.sliceUsage(0.9);
    stats.sliceUsage(0.1);
    Assertions.assertEquals(0.9, stats.getSpillProximity(), DELTA);
  }

  @Test
  public void testSpillProximityKeepsPerSliceRatioWhenThresholdsDiffer()
  {
    // A single query can pass one PerQueryStats through both small sliced groupers (from a ConcurrentGrouper) and a
    // full-buffer SpillingGrouper (subtotal/nested processing). A small slice can saturate (proximity 1.0) while a much
    // larger full-buffer grouper stays lightly filled (proximity ~0.005). Because sliceUsage records the ratio directly,
    // the saturated slice's 1.0 is preserved verbatim — there is no shared byte threshold to dilute it.
    GroupByStatsProvider.PerQueryStats stats = new GroupByStatsProvider.PerQueryStats();
    stats.sliceUsage(1.0);        // small sliced grouper at its spill point
    stats.sliceUsage(0.005);      // large full-buffer grouper barely filled
    Assertions.assertEquals(1.0, stats.getSpillProximity(), DELTA);
  }

  @Test
  public void testAggregateStatsResetZeroesSpillProximity()
  {
    GroupByStatsProvider.AggregateStats aggregateStats = new GroupByStatsProvider.AggregateStats(
        1L,
        100L,
        100L,
        200L,
        200L,
        0.75,
        2L,
        200L,
        200L,
        300L,
        300L
    );
    Assertions.assertEquals(0.75, aggregateStats.getMaxSpillProximity(), DELTA);

    aggregateStats.reset();
    Assertions.assertEquals(0.0, aggregateStats.getMaxSpillProximity(), DELTA);
  }

  @Test
  public void testAggregateStatsCopyConstructorRoundTripsSpillProximity()
  {
    GroupByStatsProvider.AggregateStats original = new GroupByStatsProvider.AggregateStats(
        1L,
        100L,
        100L,
        200L,
        200L,
        0.42,
        2L,
        200L,
        200L,
        300L,
        300L
    );
    GroupByStatsProvider.AggregateStats copy = new GroupByStatsProvider.AggregateStats(original);

    Assertions.assertEquals(0.42, copy.getMaxSpillProximity(), DELTA);
    // Spill proximity is the 6th ctor arg, sitting between maxBytesUsed and spilledQueries; verify neighbours
    // did not shift position.
    Assertions.assertEquals(200L, copy.getMaxMergeBufferUsedBytes());
    Assertions.assertEquals(2L, copy.getSpilledQueries());
  }

  @Test
  public void testAggregateStatsTakesMaxSpillProximityAcrossQueries()
  {
    GroupByStatsProvider.AggregateStats agg = new GroupByStatsProvider.AggregateStats();

    GroupByStatsProvider.PerQueryStats low = new GroupByStatsProvider.PerQueryStats();
    low.mergeBufferAcquisitionTime(10);
    low.sliceUsage(0.3);
    agg.addQueryStats(low);

    GroupByStatsProvider.PerQueryStats high = new GroupByStatsProvider.PerQueryStats();
    high.mergeBufferAcquisitionTime(10);
    high.sliceUsage(0.8);
    agg.addQueryStats(high);

    GroupByStatsProvider.PerQueryStats mid = new GroupByStatsProvider.PerQueryStats();
    mid.mergeBufferAcquisitionTime(10);
    mid.sliceUsage(0.5);
    agg.addQueryStats(mid);

    Assertions.assertEquals(0.8, agg.getMaxSpillProximity(), DELTA);
  }

  @Test
  public void testSpillProximityDroppedWhenNoAcquisitionTimeRecorded()
  {
    // A PerQueryStats with slice usage but no acquisition time is not folded into the mergeBuffer block, mirroring
    // the monitor guard. In practice this never happens: acquisition time is recorded in
    // GroupByResourcesReservationPool.reserve() before any grouper initializes.
    GroupByStatsProvider.AggregateStats agg = new GroupByStatsProvider.AggregateStats();
    GroupByStatsProvider.PerQueryStats stats = new GroupByStatsProvider.PerQueryStats();
    stats.sliceUsage(0.9);
    agg.addQueryStats(stats);

    Assertions.assertEquals(0L, agg.getMergeBufferQueries());
    Assertions.assertEquals(0.0, agg.getMaxSpillProximity(), DELTA);
  }

  /**
   * End-to-end through {@link GroupByStatsProvider} reproducing the user's scenario: a 125MiB merge buffer divided
   * into 240 per-thread slices (sliceSize ~= 546KiB). Each slice fills well below the configured buffer size, yet the
   * fullest slice reaches its spill trigger (peak size/regrowthThreshold == 1.0) and the query spills. The summed
   * {@code bytesUsed} is far below {@code sizeBytes}, which is exactly why comparing it to {@code sizeBytes} was
   * misleading; {@code maxSpillProximity} instead reports 1.0, correctly indicating the query was at the spill point.
   */
  @Test
  public void testEndToEndSlicedBufferSpillScenario()
  {
    final long sizeBytes = 125L * 1024 * 1024;            // druid.processing.buffer.sizeBytes (125MiB)
    final int numThreads = 240;                           // concurrencyHint / numThreads
    final long sliceSize = sizeBytes / numThreads;        // per-slice capacity (~546KiB)

    GroupByStatsProvider statsProvider = new GroupByStatsProvider();
    QueryResourceId id = new QueryResourceId("spilly");
    GroupByStatsProvider.PerQueryStats stats = statsProvider.getPerQueryStatsContainer(id);

    stats.mergeBufferAcquisitionTime(42);
    long expectedUsed = 0;
    for (int i = 0; i < numThreads; i++) {
      // Most slices stay light; one slice (i == 0) reaches its spill trigger (proximity 1.0).
      final long sliceUsed = (i == 0) ? sliceSize / 2 : sliceSize / 20;
      final double sliceProximity = (i == 0) ? 1.0 : 0.1;
      stats.addMergeBufferUsedBytes(sliceUsed);
      stats.sliceUsage(sliceProximity);
      expectedUsed += sliceUsed;
    }
    stats.spilledBytes(1_000_000L);

    statsProvider.closeQuery(id);
    GroupByStatsProvider.AggregateStats aggregateStats = statsProvider.getStatsSince();

    // The fullest slice hit its spill trigger, so proximity is exactly 1.0.
    Assertions.assertEquals(1.0, aggregateStats.getMaxSpillProximity(), DELTA);
    // ...even though the summed usage across slices is a tiny fraction of the configured buffer size.
    Assertions.assertEquals(expectedUsed, aggregateStats.getTotalMergeBufferUsedBytes());
    Assertions.assertTrue(
        aggregateStats.getTotalMergeBufferUsedBytes() < sizeBytes / 2,
        "summed bytesUsed should look small next to sizeBytes, despite the spill"
    );
    Assertions.assertEquals(1L, aggregateStats.getSpilledQueries());
  }
}
