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

package org.apache.druid.sql.calcite.schema;

import org.apache.druid.sql.calcite.schema.SystemSchema.DerivedSegmentStatus;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.Predicate;

/**
 * Unit tests for the per-datasource status cube {@link DatasourceSegmentStats}. Verifies that folding
 * the cube over a filter's cell mask reproduces the same distributive/algebraic aggregates a row-level
 * scan would compute, using the exact FILTERs from the console's {@code GROUP BY datasource} query.
 */
public class DatasourceSegmentStatsTest
{
  /**
   * A single datasource's segments, chosen to exercise every status dimension and each replication
   * bucket. Columns: active, published, available, realtime, overshadowed, replicationFactor, size,
   * numRows, numReplicas.
   */
  private DatasourceSegmentStats stats;

  private DatasourceSegmentStats build()
  {
    final DatasourceSegmentStats.Accumulator acc = new DatasourceSegmentStats.Accumulator();
    // A: active, published, available, historical, not overshadowed, repl=2 (POSITIVE)
    acc.add(status(true, true, true, false, false, 2L, 10L, 2L), 100L);
    // B: active, published, available, historical, not overshadowed, repl=0 (ZERO)
    acc.add(status(true, true, true, false, false, 0L, 20L, 1L), 200L);
    // C: inactive, published, unavailable, historical, overshadowed, repl=-1 (UNKNOWN)
    acc.add(status(false, true, false, false, true, -1L, 5L, 0L), 50L);
    // D: active, unpublished, available, realtime, not overshadowed, repl=-1 (UNKNOWN)
    acc.add(status(true, false, true, true, false, -1L, 3L, 1L), 30L);
    return acc.build();
  }

  private static DerivedSegmentStatus status(
      boolean active,
      boolean published,
      boolean available,
      boolean realtime,
      boolean overshadowed,
      long replicationFactor,
      long numRows,
      long numReplicas
  )
  {
    return new DerivedSegmentStatus(
        numReplicas,
        numRows,
        available,
        realtime,
        published,
        active,
        overshadowed,
        replicationFactor
    );
  }

  /**
   * Builds a cell mask from a predicate over a cell's decoded status, mirroring how the rule turns a
   * FILTER into a mask.
   */
  private static boolean[] mask(Predicate<Integer> cellMatches)
  {
    final boolean[] mask = new boolean[DatasourceSegmentStats.NUM_CELLS];
    for (int key = 0; key < DatasourceSegmentStats.NUM_CELLS; key++) {
      mask[key] = cellMatches.test(key);
    }
    return mask;
  }

  private static boolean[] allCells()
  {
    return mask(key -> true);
  }

  @Test
  public void testCountAndSumsMatchTheConsoleFilters()
  {
    stats = build();

    // COUNT(*) FILTER (WHERE is_active = 1)  -> A, B, D
    final boolean[] active = mask(DatasourceSegmentStats::cellIsActive);
    Assert.assertEquals(3, stats.count(active));
    // SUM(size) FILTER (is_active = 1) = 100 + 200 + 30
    Assert.assertEquals(330L, stats.sumSize(active));
    // SUM(num_rows) FILTER (is_active = 1) = 10 + 20 + 3
    Assert.assertEquals(33L, stats.sumNumRows(active));
    // SUM(size * num_replicas) FILTER (is_active = 1) = 100*2 + 200*1 + 30*1
    Assert.assertEquals(430L, stats.sumReplicatedSize(active));
  }

  @Test
  public void testZeroReplicaAndLoadDropCounts()
  {
    stats = build();

    // num_zero_replica_segments: is_published AND !is_overshadowed AND replication_factor = 0 -> B only
    final boolean[] zeroReplica = mask(key ->
        DatasourceSegmentStats.cellIsPublished(key)
        && !DatasourceSegmentStats.cellIsOvershadowed(key)
        && DatasourceSegmentStats.cellReplBucket(key) == DatasourceSegmentStats.REPL_ZERO);
    Assert.assertEquals(1, stats.count(zeroReplica));

    // num_segments_to_load: is_published AND !is_overshadowed AND !is_available AND replication_factor > 0 -> none
    final boolean[] toLoad = mask(key ->
        DatasourceSegmentStats.cellIsPublished(key)
        && !DatasourceSegmentStats.cellIsOvershadowed(key)
        && !DatasourceSegmentStats.cellIsAvailable(key)
        && DatasourceSegmentStats.cellReplBucket(key) == DatasourceSegmentStats.REPL_POSITIVE);
    Assert.assertEquals(0, stats.count(toLoad));

    // num_segments_to_drop: is_available AND !is_active -> none (C is inactive but unavailable)
    final boolean[] toDrop = mask(key ->
        DatasourceSegmentStats.cellIsAvailable(key) && !DatasourceSegmentStats.cellIsActive(key));
    Assert.assertEquals(0, stats.count(toDrop));
  }

  @Test
  public void testMinMaxAvgOverAvailableHistoricalSegments()
  {
    stats = build();

    // MIN/MAX/AVG(num_rows) FILTER (is_available = 1 AND is_realtime = 0) -> A(10), B(20)
    final boolean[] availableHistorical = mask(key ->
        DatasourceSegmentStats.cellIsAvailable(key) && !DatasourceSegmentStats.cellIsRealtime(key));
    Assert.assertEquals(2, stats.count(availableHistorical));
    Assert.assertEquals(Long.valueOf(10L), stats.minNumRows(availableHistorical));
    Assert.assertEquals(Long.valueOf(20L), stats.maxNumRows(availableHistorical));
    // AVG is DOUBLE in SQL: (10 + 20) / 2 = 15.0 (not integer division)
    Assert.assertEquals(
        15.0d,
        (double) stats.sumNumRows(availableHistorical) / stats.count(availableHistorical),
        0.0d
    );
  }

  @Test
  public void testMinMaxAreNullWhenNoCellMatches()
  {
    stats = build();

    // A mask that no segment falls into: overshadowed AND realtime (none of A-D).
    final boolean[] empty = mask(key ->
        DatasourceSegmentStats.cellIsOvershadowed(key) && DatasourceSegmentStats.cellIsRealtime(key));
    Assert.assertEquals(0, stats.count(empty));
    Assert.assertNull(stats.minNumRows(empty));
    Assert.assertNull(stats.maxNumRows(empty));
  }

  @Test
  public void testFullRollupTotals()
  {
    stats = build();
    final boolean[] all = allCells();
    Assert.assertEquals(4, stats.count(all));
    Assert.assertEquals(380L, stats.sumSize(all));       // 100+200+50+30
    Assert.assertEquals(38L, stats.sumNumRows(all));      // 10+20+5+3
    Assert.assertEquals(Long.valueOf(3L), stats.minNumRows(all));
    Assert.assertEquals(Long.valueOf(20L), stats.maxNumRows(all));
  }

  @Test
  public void testCellKeyRoundTrips()
  {
    final int key = DatasourceSegmentStats.cellKey(true, false, true, false, true, DatasourceSegmentStats.REPL_POSITIVE);
    Assert.assertTrue(DatasourceSegmentStats.cellIsActive(key));
    Assert.assertFalse(DatasourceSegmentStats.cellIsPublished(key));
    Assert.assertTrue(DatasourceSegmentStats.cellIsAvailable(key));
    Assert.assertFalse(DatasourceSegmentStats.cellIsRealtime(key));
    Assert.assertTrue(DatasourceSegmentStats.cellIsOvershadowed(key));
    Assert.assertEquals(DatasourceSegmentStats.REPL_POSITIVE, DatasourceSegmentStats.cellReplBucket(key));
  }

  @Test
  public void testReplBucketOf()
  {
    Assert.assertEquals(DatasourceSegmentStats.REPL_ZERO, DatasourceSegmentStats.replBucketOf(0L));
    Assert.assertEquals(DatasourceSegmentStats.REPL_POSITIVE, DatasourceSegmentStats.replBucketOf(3L));
    Assert.assertEquals(DatasourceSegmentStats.REPL_UNKNOWN, DatasourceSegmentStats.replBucketOf(-1L));
  }
}
