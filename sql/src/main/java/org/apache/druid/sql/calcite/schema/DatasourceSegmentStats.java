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

import javax.annotation.Nullable;

/**
 * Per-datasource "status sub-cube": the distributive/algebraic segment measures, bucketed by the
 * low-cardinality status dimensions ({@code is_active, is_published, is_available, is_realtime,
 * is_overshadowed}, plus a 3-way {@code replication_factor} bucket). Any {@code GROUP BY datasource}
 * aggregate whose {@code FILTER} touches only those status columns is answered by folding the cube
 * cells the filter selects (a mask) - see {@code SegmentsRollupRule}. All stored measures are
 * distributive/algebraic, so the fold is exact.
 */
public final class DatasourceSegmentStats
{
  /**
   * The boolean status dimensions of the cube, in bit order (index == bit position). {@link #BIT_ACTIVE}
   * etc. index into this, and {@link #FLAG_CELLS} is derived from its length, so the cube stays
   * consistent if a dimension is added or removed.
   */
  private static final String[] FLAG_DIMENSIONS = {
      "is_active",
      "is_published",
      "is_available",
      "is_realtime",
      "is_overshadowed"
  };

  private static final int BIT_ACTIVE = 0;
  private static final int BIT_PUBLISHED = 1;
  private static final int BIT_AVAILABLE = 2;
  private static final int BIT_REALTIME = 3;
  private static final int BIT_OVERSHADOWED = 4;

  // replication_factor buckets.
  public static final int REPL_ZERO = 0;
  public static final int REPL_POSITIVE = 1;
  public static final int REPL_UNKNOWN = 2;
  private static final int REPL_BUCKETS = 3;

  // (2 ^ #flag-dimensions) flag combos x replication buckets.
  private static final int FLAG_CELLS = 1 << FLAG_DIMENSIONS.length;
  public static final int NUM_CELLS = FLAG_CELLS * REPL_BUCKETS;

  // Per-cell measures (parallel arrays indexed by the packed status key).
  private final long[] count;
  private final long[] sumSize;
  private final long[] sumNumRows;
  private final long[] sumReplicatedSize;
  private final long[] minNumRows;
  private final long[] maxNumRows;

  private DatasourceSegmentStats(
      long[] count,
      long[] sumSize,
      long[] sumNumRows,
      long[] sumReplicatedSize,
      long[] minNumRows,
      long[] maxNumRows
  )
  {
    this.count = count;
    this.sumSize = sumSize;
    this.sumNumRows = sumNumRows;
    this.sumReplicatedSize = sumReplicatedSize;
    this.minNumRows = minNumRows;
    this.maxNumRows = maxNumRows;
  }

  // -- static key/dimension helpers (shared with the rule's predicate evaluator) --

  public static int cellKey(
      boolean isActive,
      boolean isPublished,
      boolean isAvailable,
      boolean isRealtime,
      boolean isOvershadowed,
      int replBucket
  )
  {
    int flags = 0;
    flags |= isActive ? (1 << BIT_ACTIVE) : 0;
    flags |= isPublished ? (1 << BIT_PUBLISHED) : 0;
    flags |= isAvailable ? (1 << BIT_AVAILABLE) : 0;
    flags |= isRealtime ? (1 << BIT_REALTIME) : 0;
    flags |= isOvershadowed ? (1 << BIT_OVERSHADOWED) : 0;
    return replBucket * FLAG_CELLS + flags;
  }

  public static int replBucketOf(long replicationFactor)
  {
    if (replicationFactor == 0) {
      return REPL_ZERO;
    }
    return replicationFactor > 0 ? REPL_POSITIVE : REPL_UNKNOWN;
  }

  public static boolean cellIsActive(int key)
  {
    return flagSet(key, BIT_ACTIVE);
  }

  public static boolean cellIsPublished(int key)
  {
    return flagSet(key, BIT_PUBLISHED);
  }

  public static boolean cellIsAvailable(int key)
  {
    return flagSet(key, BIT_AVAILABLE);
  }

  public static boolean cellIsRealtime(int key)
  {
    return flagSet(key, BIT_REALTIME);
  }

  public static boolean cellIsOvershadowed(int key)
  {
    return flagSet(key, BIT_OVERSHADOWED);
  }

  public static int cellReplBucket(int key)
  {
    return key / FLAG_CELLS;
  }

  private static boolean flagSet(int key, int bit)
  {
    return ((key % FLAG_CELLS) & (1 << bit)) != 0;
  }

  // -- fold measures over a cell mask (the filter's selected cells) --

  public long count(final boolean[] mask)
  {
    long total = 0;
    for (int i = 0; i < NUM_CELLS; i++) {
      if (mask[i]) {
        total += count[i];
      }
    }
    return total;
  }

  public long sumSize(final boolean[] mask)
  {
    return sum(sumSize, mask);
  }

  public long sumNumRows(final boolean[] mask)
  {
    return sum(sumNumRows, mask);
  }

  public long sumReplicatedSize(final boolean[] mask)
  {
    return sum(sumReplicatedSize, mask);
  }

  @Nullable
  public Long minNumRows(final boolean[] mask)
  {
    long result = Long.MAX_VALUE;
    boolean any = false;
    for (int i = 0; i < NUM_CELLS; i++) {
      if (mask[i] && count[i] > 0) {
        any = true;
        result = Math.min(result, minNumRows[i]);
      }
    }
    return any ? result : null;
  }

  @Nullable
  public Long maxNumRows(final boolean[] mask)
  {
    long result = Long.MIN_VALUE;
    boolean any = false;
    for (int i = 0; i < NUM_CELLS; i++) {
      if (mask[i] && count[i] > 0) {
        any = true;
        result = Math.max(result, maxNumRows[i]);
      }
    }
    return any ? result : null;
  }

  private static long sum(final long[] measure, final boolean[] mask)
  {
    long total = 0;
    for (int i = 0; i < NUM_CELLS; i++) {
      if (mask[i]) {
        total += measure[i];
      }
    }
    return total;
  }

  static final class Accumulator
  {
    private final long[] count = new long[NUM_CELLS];
    private final long[] sumSize = new long[NUM_CELLS];
    private final long[] sumNumRows = new long[NUM_CELLS];
    private final long[] sumReplicatedSize = new long[NUM_CELLS];
    private final long[] minNumRows;
    private final long[] maxNumRows;

    Accumulator()
    {
      minNumRows = new long[NUM_CELLS];
      maxNumRows = new long[NUM_CELLS];
      java.util.Arrays.fill(minNumRows, Long.MAX_VALUE);
      java.util.Arrays.fill(maxNumRows, Long.MIN_VALUE);
    }

    void add(final DerivedSegmentStatus status, final long size)
    {
      final int key = cellKey(
          status.isActive,
          status.isPublished,
          status.isAvailable,
          status.isRealtime,
          status.isOvershadowed,
          replBucketOf(status.replicationFactor)
      );
      count[key]++;
      sumSize[key] += size;
      sumNumRows[key] += status.numRows;
      sumReplicatedSize[key] += size * status.numReplicas;
      minNumRows[key] = Math.min(minNumRows[key], status.numRows);
      maxNumRows[key] = Math.max(maxNumRows[key], status.numRows);
    }

    DatasourceSegmentStats build()
    {
      return new DatasourceSegmentStats(count, sumSize, sumNumRows, sumReplicatedSize, minNumRows, maxNumRows);
    }
  }
}
